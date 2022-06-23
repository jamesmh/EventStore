﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Common.Utils;
using EventStore.Core.Exceptions;
using EventStore.Core.TransactionLog;
using EventStore.Core.Util;
using EventStore.Core.Index.Hashes;
using EventStore.Core.TransactionLog.Checkpoint;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.Index {
	public class TableIndex : ITableIndex {
		public const string IndexMapFilename = "indexmap";
		private const int MaxMemoryTables = 1;

		private static readonly ILogger Log = LogManager.GetLoggerFor<TableIndex>();
		internal static readonly IndexEntry InvalidIndexEntry = new IndexEntry(0, -1, -1);

		public long CommitCheckpoint {
			get { return Interlocked.Read(ref _commitCheckpoint); }
		}

		public long PrepareCheckpoint {
			get { return Interlocked.Read(ref _prepareCheckpoint); }
		}

		private readonly int _maxSizeForMemory;
		private readonly int _maxTablesPerLevel;
		private readonly bool _additionalReclaim;
		private readonly bool _inMem;
		private readonly bool _skipIndexVerify;
		private readonly int _indexCacheDepth;
		private readonly int _initializationThreads;
		private readonly byte _ptableVersion;
		private readonly string _directory;
		private readonly Func<IMemTable> _memTableFactory;
		private readonly Func<TFReaderLease> _tfReaderFactory;
		private readonly IIndexFilenameProvider _fileNameProvider;

		private readonly object _awaitingTablesLock = new object();

		private IndexMap _indexMap;
		private List<TableItem> _awaitingMemTables;
		private long _commitCheckpoint = -1;
		private long _prepareCheckpoint = -1;

		// there are two background processes, scavenge and writing/merging ptables.
		private volatile bool _backgroundRunning;
		private readonly ManualResetEventSlim _backgroundRunningEvent = new ManualResetEventSlim(true);

		private IHasher _lowHasher;
		private IHasher _highHasher;

		private bool _initialized;
		public const string ForceIndexVerifyFilename = ".forceverify";
		private readonly int _maxAutoMergeIndexLevel;

		public TableIndex(string directory,
			IHasher lowHasher,
			IHasher highHasher,
			Func<IMemTable> memTableFactory,
			Func<TFReaderLease> tfReaderFactory,
			byte ptableVersion,
			int maxAutoMergeIndexLevel,
			int maxSizeForMemory = 1000000,
			int maxTablesPerLevel = 4,
			bool additionalReclaim = false,
			bool inMem = false,
			bool skipIndexVerify = false,
			int indexCacheDepth = 16,
			int initializationThreads = 1
		) {
			Ensure.NotNullOrEmpty(directory, "directory");
			Ensure.NotNull(memTableFactory, "memTableFactory");
			Ensure.NotNull(lowHasher, "lowHasher");
			Ensure.NotNull(highHasher, "highHasher");
			Ensure.NotNull(tfReaderFactory, "tfReaderFactory");
			Ensure.Positive(initializationThreads, "initializationThreads");

			if (maxTablesPerLevel <= 1)
				throw new ArgumentOutOfRangeException("maxTablesPerLevel");

			if (indexCacheDepth > 28 || indexCacheDepth < 8) throw new ArgumentOutOfRangeException("indexCacheDepth");

			_directory = directory;
			_memTableFactory = memTableFactory;
			_tfReaderFactory = tfReaderFactory;
			_fileNameProvider = new GuidFilenameProvider(directory);
			_maxSizeForMemory = maxSizeForMemory;
			_maxTablesPerLevel = maxTablesPerLevel;
			_additionalReclaim = additionalReclaim;
			_inMem = inMem;
			_skipIndexVerify = ShouldForceIndexVerify() ? false : skipIndexVerify;
			_indexCacheDepth = indexCacheDepth;
			_initializationThreads = initializationThreads;
			_ptableVersion = ptableVersion;
			_awaitingMemTables = new List<TableItem> {new TableItem(_memTableFactory(), -1, -1, 0, false)};

			_lowHasher = lowHasher;
			_highHasher = highHasher;

			_maxAutoMergeIndexLevel = maxAutoMergeIndexLevel;
		}

		public void Initialize(long chaserCheckpoint) {
			Ensure.Nonnegative(chaserCheckpoint, "chaserCheckpoint");

			//NOT THREAD SAFE (assumes one thread)
			if (_initialized)
				throw new IOException("TableIndex is already initialized.");
			_initialized = true;

			if (_inMem) {
				_indexMap = IndexMap.CreateEmpty(_maxTablesPerLevel, int.MaxValue);
				_prepareCheckpoint = _indexMap.PrepareCheckpoint;
				_commitCheckpoint = _indexMap.CommitCheckpoint;
				return;
			}

			if (ShouldForceIndexVerify()) {
				Log.Debug("Forcing verification of index files...");
			}

			CreateIfDoesNotExist(_directory);
			var indexmapFile = Path.Combine(_directory, IndexMapFilename);

			// if TableIndex's CommitCheckpoint is >= amount of written TFChunk data,
			// we'll have to remove some of PTables as they point to non-existent data
			// this can happen (very unlikely, though) on master crash
			try {
				_indexMap = IndexMap.FromFile(indexmapFile, _maxTablesPerLevel, true, _indexCacheDepth,
					_skipIndexVerify, _initializationThreads, _maxAutoMergeIndexLevel);
				if (_indexMap.CommitCheckpoint >= chaserCheckpoint) {
					_indexMap.Dispose(TimeSpan.FromMilliseconds(5000));
					throw new CorruptIndexException(String.Format(
						"IndexMap's CommitCheckpoint ({0}) is greater than ChaserCheckpoint ({1}).",
						_indexMap.CommitCheckpoint, chaserCheckpoint));
				}

				//verification should be completed by now
				DeleteForceIndexVerifyFile();
			} catch (CorruptIndexException exc) {
				Log.ErrorException(exc, "ReadIndex is corrupted...");
				LogIndexMapContent(indexmapFile);
				DumpAndCopyIndex();
				File.SetAttributes(indexmapFile, FileAttributes.Normal);
				File.Delete(indexmapFile);
				DeleteForceIndexVerifyFile();
				_indexMap = IndexMap.FromFile(indexmapFile, _maxTablesPerLevel, true, _indexCacheDepth,
					_skipIndexVerify, _initializationThreads, _maxAutoMergeIndexLevel);
			}

			_prepareCheckpoint = _indexMap.PrepareCheckpoint;
			_commitCheckpoint = _indexMap.CommitCheckpoint;

			// clean up all other remaining files
			var indexFiles = _indexMap.InOrder().Select(x => Path.GetFileName(x.Filename))
				.Union(new[] {IndexMapFilename});
			var toDeleteFiles = Directory.EnumerateFiles(_directory).Select(Path.GetFileName)
				.Except(indexFiles, StringComparer.OrdinalIgnoreCase);
			foreach (var filePath in toDeleteFiles) {
				var file = Path.Combine(_directory, filePath);
				File.SetAttributes(file, FileAttributes.Normal);
				File.Delete(file);
			}
		}

		private static void LogIndexMapContent(string indexmapFile) {
			try {
				Log.Error("IndexMap '{indexMap}' content:\n {content}", indexmapFile,
					Helper.FormatBinaryDump(File.ReadAllBytes(indexmapFile)));
			} catch (Exception exc) {
				Log.ErrorException(exc, "Unexpected error while dumping IndexMap '{indexMap}'.", indexmapFile);
			}
		}

		private void DumpAndCopyIndex() {
			string dumpPath = null;
			try {
				dumpPath = Path.Combine(Path.GetDirectoryName(_directory),
					string.Format("index-backup-{0:yyyy-MM-dd_HH-mm-ss.fff}", DateTime.UtcNow));
				Log.Error("Making backup of index folder for inspection to {dumpPath}...", dumpPath);
				FileUtils.DirectoryCopy(_directory, dumpPath, copySubDirs: true);
			} catch (Exception exc) {
				Log.ErrorException(exc, "Unexpected error while copying index to backup dir '{dumpPath}'", dumpPath);
			}
		}

		private static void CreateIfDoesNotExist(string directory) {
			if (!Directory.Exists(directory))
				Directory.CreateDirectory(directory);
		}

		public void Add(long commitPos, string streamId, long version, long position) {
			Ensure.Nonnegative(commitPos, "commitPos");
			Ensure.Nonnegative(version, "version");
			Ensure.Nonnegative(position, "position");

			AddEntries(commitPos, new[] {CreateIndexKey(streamId, version, position)});
		}

		public void AddEntries(long commitPos, IList<IndexKey> entries) {
			//should only be called on a single thread.
			var table = (IMemTable)_awaitingMemTables[0].Table; // always a memtable

			var collection = entries.Select(x => CreateIndexEntry(x)).ToList();
			table.AddEntries(collection);

			if (table.Count >= _maxSizeForMemory) {
				long prepareCheckpoint = collection[0].Position;
				for (int i = 1, n = collection.Count; i < n; ++i) {
					prepareCheckpoint = Math.Max(prepareCheckpoint, collection[i].Position);
				}

				TryProcessAwaitingTables(commitPos, prepareCheckpoint);
			}
		}

		public Task MergeIndexes() {
			TryManualMerge();
			return Task.CompletedTask;
		}

		public bool IsBackgroundTaskRunning {
			get { return _backgroundRunning; }
		}

		//Automerge only
		private void TryProcessAwaitingTables(long commitPos, long prepareCheckpoint) {
			lock (_awaitingTablesLock) {
				var newTables = new List<TableItem> {new TableItem(_memTableFactory(), -1, -1, 0, false)};
				newTables.AddRange(_awaitingMemTables.Select(
					(x, i) => i == 0 ? new TableItem(x.Table, prepareCheckpoint, commitPos, x.Level, x.IsFromIndexMap) : x));

				Log.Trace("Switching MemTable, currently: {awaitingMemTables} awaiting tables.", newTables.Count);

				_awaitingMemTables = newTables;
				if (_inMem) return;
				TryProcessAwaitingTables();

				if (_additionalReclaim)
					ThreadPool.QueueUserWorkItem(x => ReclaimMemoryIfNeeded(_awaitingMemTables));
			}
		}

		public void TryManualMerge() {
			lock (_awaitingTablesLock) {
				var (maxLevel, highest) = _indexMap.GetTableForManualMerge();
				if (highest == null) return; //no work to do

				//These values are actually ignored later as manual merge will never change the checkpoint as no
				//new entries are added, but they can be helpful to see when the manual merge was called
				//because of the way the "queue" currently works (LIFO) it should always be the same
				var prepare = _indexMap.PrepareCheckpoint;
				var commit = _indexMap.CommitCheckpoint;
				var newTables = new List<TableItem>(_awaitingMemTables) {
					new TableItem(highest, prepare, commit, maxLevel, true)
				};
				_awaitingMemTables = newTables;
				TryProcessAwaitingTables();
			}
		}

		private void TryProcessAwaitingTables() {
			lock (_awaitingTablesLock) {
				if (!_backgroundRunning) {
					_backgroundRunningEvent.Reset();
					_backgroundRunning = true;
					ThreadPool.QueueUserWorkItem(x => ReadOffQueue());
				}
			}
		}

		private void ReadOffQueue() {
			try {
				while (true) {
					TableItem tableItem;
					//ISearchTable table;
					lock (_awaitingTablesLock) {
						Log.Trace("Awaiting tables queue size is: {awaitingMemTables}.", _awaitingMemTables.Count);
						if (_awaitingMemTables.Count == 1) {
							return;
						}

						tableItem = _awaitingMemTables[_awaitingMemTables.Count - 1];
					}

					PTable ptable;
					var memtable = tableItem.Table as IMemTable;
					if (memtable != null) {
						memtable.MarkForConversion();
						ptable = PTable.FromMemtable(memtable, _fileNameProvider.GetFilenameNewTable(),
							_indexCacheDepth, _skipIndexVerify);
					} else
						ptable = (PTable)tableItem.Table;

					var indexmapFile = Path.Combine(_directory, IndexMapFilename);
					MergeResult mergeResult;
					using (var reader = _tfReaderFactory()) {
						mergeResult = _indexMap.AddPTable(ptable, tableItem.PrepareCheckpoint,
							tableItem.CommitCheckpoint,
							(streamId, currentHash) => UpgradeHash(streamId, currentHash),
							entry => reader.ExistsAt(entry.Position),
							entry => ReadEntry(reader, entry.Position),
							_fileNameProvider,
							_ptableVersion,
							tableItem.Level,
							_indexCacheDepth,
							_skipIndexVerify);
					}

					_indexMap = mergeResult.MergedMap;
					_indexMap.SaveToFile(indexmapFile);

					lock (_awaitingTablesLock) {
						var memTables = _awaitingMemTables.ToList();

						var corrTable = memTables.First(x => x.Table.Id == ptable.Id);
						memTables.Remove(corrTable);

						// parallel thread could already switch table,
						// so if we have another PTable instance with same ID,
						// we need to kill that instance as we added ours already
						if (!ReferenceEquals(corrTable.Table, ptable) && corrTable.Table is PTable)
							((PTable)corrTable.Table).MarkForDestruction();

						Log.Trace("There are now {awaitingMemTables} awaiting tables.", memTables.Count);
						_awaitingMemTables = memTables;
					}

					mergeResult.ToDelete.ForEach(x => x.MarkForDestruction());
				}
			} catch (FileBeingDeletedException exc) {
				Log.ErrorException(exc,
					"Could not acquire chunk in TableIndex.ReadOffQueue. It is OK if node is shutting down.");
			} catch (Exception exc) {
				Log.ErrorException(exc, "Error in TableIndex.ReadOffQueue");
				throw;
			} finally {
				lock (_awaitingTablesLock) {
					_backgroundRunning = false;
					_backgroundRunningEvent.Set();
				}
			}
		}

		public void WaitForBackgroundTasks() {
			if (!_backgroundRunningEvent.Wait(7000)) {
				throw new TimeoutException("Waiting for background tasks took too long.");
			}
		}

		public void Scavenge(IIndexScavengerLog log, CancellationToken ct) =>
			Scavenge(checkSuitability: table => { }, shouldKeep: null, log: log, ct: ct);

		public void Scavenge(
			Action<PTable> checkSuitability,
			Func<IndexEntry, bool> shouldKeep,
			IIndexScavengerLog log,
			CancellationToken ct) {

			GetExclusiveBackgroundTask(ct);
			var sw = Stopwatch.StartNew();

			try {
				Log.Info("Starting scavenge of TableIndex.");
				ScavengeInternal(checkSuitability, shouldKeep, log, ct);
			} finally {
				// Since scavenging indexes is the only place the ExistsAt optimization makes sense (and takes up a lot of memory), we can clear it after an index scavenge has completed. 
				TFChunkReaderExistsAtOptimizer.Instance.DeOptimizeAll();

				lock (_awaitingTablesLock) {
					_backgroundRunning = false;
					_backgroundRunningEvent.Set();

					TryProcessAwaitingTables();
				}

				Log.Info("Completed scavenge of TableIndex.  Elapsed: {elapsed}", sw.Elapsed);
			}
		}

		private void ScavengeInternal(
			Action<PTable> checkSuitability,
			Func<IndexEntry, bool> shouldKeep,
			IIndexScavengerLog log,
			CancellationToken ct) {

			var toScavenge = _indexMap.InOrder().ToList();

			foreach (var pTable in toScavenge) {
				var startNew = Stopwatch.StartNew();

				try {
					ct.ThrowIfCancellationRequested();

					using (var reader = _tfReaderFactory()) {
						var indexmapFile = Path.Combine(_directory, IndexMapFilename);

						Func<IndexEntry, bool> existsAt = entry => reader.ExistsAt(entry.Position);
						var scavengeResult = _indexMap.Scavenge(pTable.Id, ct,
							checkSuitability,
							shouldKeep ?? existsAt,
							(streamId, currentHash) => UpgradeHash(streamId, currentHash),
							existsAt,
							entry => ReadEntry(reader, entry.Position), _fileNameProvider, _ptableVersion,
							_indexCacheDepth, _skipIndexVerify);

						if (scavengeResult.IsSuccess) {
							_indexMap = scavengeResult.ScavengedMap;
							_indexMap.SaveToFile(indexmapFile);

							scavengeResult.OldTable.MarkForDestruction();

							var entriesDeleted = scavengeResult.OldTable.Count - scavengeResult.NewTable.Count;
							log.IndexTableScavenged(scavengeResult.Level, scavengeResult.Index, startNew.Elapsed,
								entriesDeleted, scavengeResult.NewTable.Count, scavengeResult.SpaceSaved);
						} else {
							log.IndexTableNotScavenged(scavengeResult.Level, scavengeResult.Index, startNew.Elapsed,
								pTable.Count, "");
						}
					}
				} catch (OperationCanceledException) {
					log.IndexTableNotScavenged(-1, -1, startNew.Elapsed, pTable.Count, "Scavenge cancelled");
					throw;
				} catch (Exception ex) {
					log.IndexTableNotScavenged(-1, -1, startNew.Elapsed, pTable.Count, ex.Message);
					throw;
				}
			}
		}

		private void GetExclusiveBackgroundTask(CancellationToken ct) {
			while (true) {
				lock (_awaitingTablesLock) {
					if (!_backgroundRunning) {
						_backgroundRunningEvent.Reset();
						_backgroundRunning = true;
						return;
					}
				}

				Log.Info("Waiting for TableIndex background task to complete before starting scavenge.");
				_backgroundRunningEvent.Wait(ct);
			}
		}

		private static Tuple<string, bool> ReadEntry(TFReaderLease reader, long position) {
			RecordReadResult result = reader.TryReadAt(position);
			if (!result.Success)
				return new Tuple<string, bool>(String.Empty, false);
			if (result.LogRecord.RecordType != TransactionLog.LogRecords.LogRecordType.Prepare)
				throw new Exception(string.Format("Incorrect type of log record {0}, expected Prepare record.",
					result.LogRecord.RecordType));
			return new Tuple<string, bool>(((TransactionLog.LogRecords.PrepareLogRecord)result.LogRecord).EventStreamId,
				true);
		}

		private void ReclaimMemoryIfNeeded(List<TableItem> awaitingMemTables) {
			var toPutOnDisk = awaitingMemTables.OfType<IMemTable>().Count() - MaxMemoryTables;
			for (var i = awaitingMemTables.Count - 1; i >= 1 && toPutOnDisk > 0; i--) {
				var memtable = awaitingMemTables[i].Table as IMemTable;
				if (memtable == null || !memtable.MarkForConversion())
					continue;

				Log.Trace("Putting awaiting file as PTable instead of MemTable [{id}].", memtable.Id);

				var ptable = PTable.FromMemtable(memtable, _fileNameProvider.GetFilenameNewTable(), _indexCacheDepth,
					_skipIndexVerify);
				var swapped = false;
				lock (_awaitingTablesLock) {
					for (var j = _awaitingMemTables.Count - 1; j >= 1; j--) {
						var tableItem = _awaitingMemTables[j];
						if (!(tableItem.Table is IMemTable) || tableItem.Table.Id != ptable.Id) continue;
						swapped = true;
						_awaitingMemTables[j] = new TableItem(ptable,
							tableItem.PrepareCheckpoint,
							tableItem.CommitCheckpoint,
							tableItem.Level, false);
						break;
					}
				}

				if (!swapped)
					ptable.MarkForDestruction();
				toPutOnDisk--;
			}
		}

		public bool TryGetOneValue(string streamId, long version, out long position) {
			ulong stream = CreateHash(streamId);
			int counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetOneValueInternal(stream, version, out position);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		private bool TryGetOneValueInternal(ulong stream, long version, out long position) {
			if (version < 0)
				throw new ArgumentOutOfRangeException("version");

			var awaiting = _awaitingMemTables;
			foreach (var tableItem in awaiting) {
				if(tableItem.IsFromIndexMap) continue;
				if (tableItem.Table.TryGetOneValue(stream, version, out position))
					return true;
			}

			var map = _indexMap;
			foreach (var table in map.InOrder()) {
				if (table.TryGetOneValue(stream, version, out position))
					return true;
			}

			position = 0;
			return false;
		}

		public bool TryGetLatestEntry(string streamId, out IndexEntry entry) {
			ulong stream = CreateHash(streamId);
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetLatestEntryInternal(stream, out entry);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		private bool TryGetLatestEntryInternal(ulong stream, out IndexEntry entry) {
			var awaiting = _awaitingMemTables;

			foreach (var t in awaiting) {
				if(t.IsFromIndexMap) continue;
				if (t.Table.TryGetLatestEntry(stream, out entry))
					return true;
			}

			var map = _indexMap;
			foreach (var table in map.InOrder()) {
				if (table.TryGetLatestEntry(stream, out entry))
					return true;
			}

			entry = InvalidIndexEntry;
			return false;
		}

		public bool TryGetLatestEntry(ulong stream, long beforePosition, Func<IndexEntry, bool> isForThisStream, out IndexEntry entry) {
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetLatestEntryInternal(stream, beforePosition, isForThisStream, out entry);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		public bool TryGetLatestEntry(string streamId, long beforePosition, Func<IndexEntry, bool> isForThisStream, out IndexEntry entry) {
			ulong stream = CreateHash(streamId);
			return TryGetLatestEntry(stream, beforePosition, isForThisStream, out entry);
		}

		private bool TryGetLatestEntryInternal(ulong stream, long beforePosition, Func<IndexEntry,bool> isForThisStream, out IndexEntry entry) {
			var awaiting = _awaitingMemTables;

			foreach (var t in awaiting) {
				if(t.IsFromIndexMap) continue;
				if (t.Table.TryGetLatestEntry(stream, beforePosition, isForThisStream, out entry))
					return true;
			}

			var map = _indexMap;
			foreach (var table in map.InOrder()) {
				if (table.TryGetLatestEntry(stream, beforePosition, isForThisStream, out entry))
					return true;
			}

			entry = InvalidIndexEntry;
			return false;
		}

		public bool TryGetOldestEntry(string streamId, out IndexEntry entry) {
			ulong stream = CreateHash(streamId);
			return TryGetOldestEntry(stream, out entry);
		}

		public bool TryGetOldestEntry(ulong stream, out IndexEntry entry) {
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetOldestEntryInternal(stream, out entry);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		private bool TryGetOldestEntryInternal(ulong stream, out IndexEntry entry) {
			var map = _indexMap;
			foreach (var table in map.InReverseOrder()) {
				if (table.TryGetOldestEntry(stream, out entry))
					return true;
			}

			var awaiting = _awaitingMemTables;
			for (var index = awaiting.Count - 1; index >= 0; index--) {
				if(awaiting[index].IsFromIndexMap) continue;
				if (awaiting[index].Table.TryGetOldestEntry(stream, out entry))
					return true;
			}

			entry = InvalidIndexEntry;
			return false;
		}

		public bool TryGetNextEntry(string streamId, long afterVersion, out IndexEntry entry) {
			ulong stream = CreateHash(streamId);
			return TryGetNextEntry(stream, afterVersion, out entry);
		}

		public bool TryGetNextEntry(ulong stream, long afterVersion, out IndexEntry entry) {
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetNextEntryInternal(stream, afterVersion, out entry);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		private bool TryGetNextEntryInternal(ulong stream, long afterVersion, out IndexEntry entry) {
			var map = _indexMap;
			foreach (var table in map.InReverseOrder()) {
				if (table.TryGetNextEntry(stream, afterVersion, out entry))
					return true;
			}

			var awaiting = _awaitingMemTables;
			for (var index = awaiting.Count - 1; index >= 0; index--) {
				if(awaiting[index].IsFromIndexMap) continue;
				if (awaiting[index].Table.TryGetNextEntry(stream, afterVersion, out entry))
					return true;
			}

			entry = InvalidIndexEntry;
			return false;
		}

		public bool TryGetPreviousEntry(string streamId, long beforeVersion, out IndexEntry entry) {
			ulong stream = CreateHash(streamId);
			return TryGetPreviousEntry(stream, beforeVersion, out entry);
		}

		public bool TryGetPreviousEntry(ulong stream, long beforeVersion, out IndexEntry entry) {
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return TryGetPreviousEntryInternal(stream, beforeVersion, out entry);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		private bool TryGetPreviousEntryInternal(ulong stream, long beforeVersion, out IndexEntry entry) {
			var awaiting = _awaitingMemTables;

			foreach (var t in awaiting) {
				if(t.IsFromIndexMap) continue;
				if (t.Table.TryGetPreviousEntry(stream, beforeVersion, out entry))
					return true;
			}

			var map = _indexMap;
			foreach (var table in map.InOrder()) {
				if (table.TryGetPreviousEntry(stream, beforeVersion, out entry))
					return true;
			}

			entry = InvalidIndexEntry;
			return false;
		}

		//qq old: limit is quite dangerous because it prevents a full search of the tables.
		// whatever we get back from a table will be  for the right stream hash and within the version ranges
		// and it will definitely be in order. but if limit is set higher we might get extra duplicates
		// which might override records that we would have returned with limit set lower.
		//qq old: which probably means we need to look at the calls with limit really carefully - dont do something like pass maxcount from the client in as the limit.
		public IEnumerable<IndexEntry> GetRange(string streamId, long startVersion, long endVersion,
			int? limit = null) => GetRange(CreateHash(streamId), startVersion, endVersion, limit);

		public IEnumerable<IndexEntry> GetRange(ulong stream, long startVersion, long endVersion, int? limit = null) {
			var counter = 0;
			while (counter < 5) {
				counter++;
				try {
					return GetRangeInternal(stream, startVersion, endVersion, limit);
				} catch (FileBeingDeletedException) {
					Log.Trace("File being deleted.");
				} catch (MaybeCorruptIndexException e) {
					ForceIndexVerifyOnNextStartup();
					throw e;
				}
			}

			throw new InvalidOperationException("Files are locked.");
		}

		//qq old: what does this actually return? a readonly list of IndexEntries in descending order.
		// this usually means the version is descending in the ouput, but the details are a bit complicated:
		// it used to be always in IndexEntry default order (streamhash, version, position) descending
		//  which means newest first. if we have a mix of 32bit and 64bit indexes then the same stream will have two different hashes
		//  BUT the 64bit tables will be newer than the 32bit ones, and the 64bit hashes will be greater than the 32bit ones,
		//  so the sort order isn't affected. EXCEPT when a later table contains an earlier version number as in the problem that
		//  required indexscanonread. when that happens the versions will be returned out of order here, but we are deailing with that in indexreader.
		//
		// so it is the job of the caller to
		//   1. resolve hash collisions
		//   2. deal with one stream that has multiple events with the same version bug (for which the results here could be out of version order and will need sorting by version and then deduplicating.
		// 
		//qq old: what is limit here, in teh end it is the limit that gets passed to ptable.readforward.
		// it is the maximum number of records that _each ptable_ will return, it's the HIGHER versions that they will return and leave behind the lower.
		private IEnumerable<IndexEntry> GetRangeInternal(ulong hash, long startVersion, long endVersion,
			int? limit = null) {
			if (startVersion < 0)
				throw new ArgumentOutOfRangeException("startVersion");
			if (endVersion < 0)
				throw new ArgumentOutOfRangeException("endVersion");

			var candidates = new List<IEnumerator<IndexEntry>>();

			var awaiting = _awaitingMemTables;
			for (int index = 0; index < awaiting.Count; index++) {
				if(awaiting[index].IsFromIndexMap) continue;
				var range = awaiting[index].Table.GetRange(hash, startVersion, endVersion, limit).GetEnumerator();
				if (range.MoveNext())
					candidates.Add(range);
			}

			var map = _indexMap;
			foreach (var table in map.InOrder()) {
				var range = table.GetRange(hash, startVersion, endVersion, limit).GetEnumerator();
				if (range.MoveNext())
					candidates.Add(range);
			}

			var last = new IndexEntry(0, 0, 0);
			var first = true;

			var sortedCandidates = new List<IndexEntry>();
			while (candidates.Count > 0) {
				var maxIdx = GetMaxOf(candidates);
				var winner = candidates[maxIdx];

				var best = winner.Current;
				// if it is the first then definitely not a duplicate
				// if different position then definitely not a duplicate
				// but just being a different stream doesn't stop it being a duplicate. it has to be a different stream and version to not be a duplicate.
				// so if it is the same stream OR same version it can be a duplicate if it has the same position.
				//qq old: so we are saying we if have two entries that point to the same position (but why would this ever happen??)
				//   if they are for the same stream hash but different version, then remove it as a duplicate - afaik this isn't possible??
				//   if they are for the same version but different stream hash, then remove it as a duplicate - 32bit vs 64bit could explain why perhaps
				//   different version and different strema hash: keep it even though it is for same position.
				if (first ||
					((last.Stream != best.Stream) && (last.Version != best.Version)) ||
					last.Position != best.Position) {
					last = best;
					sortedCandidates.Add(best);
					first = false;
				}

				if (!winner.MoveNext())
					candidates.RemoveAt(maxIdx);
			}

			return sortedCandidates;
		}

		private static int GetMaxOf(List<IEnumerator<IndexEntry>> enumerators) {
			var max = new IndexEntry(
				stream: ulong.MinValue,
				version: 0,
				position: long.MinValue);

			int idx = 0;
			for (int i = 0; i < enumerators.Count; i++) {
				var cur = enumerators[i].Current;
				if (cur.CompareTo(max) > 0) {
					max = cur;
					idx = i;
				}
			}

			return idx;
		}

		public void Close(bool removeFiles = true) {
			if (!_backgroundRunningEvent.Wait(7000))
				throw new TimeoutException("Could not finish background thread in reasonable time.");
			if (_inMem)
				return;
			if (_indexMap == null) return;
			if (removeFiles) {
				_indexMap.InOrder().ToList().ForEach(x => x.MarkForDestruction());
				var fileName = Path.Combine(_directory, IndexMapFilename);
				if (File.Exists(fileName)) {
					File.SetAttributes(fileName, FileAttributes.Normal);
					File.Delete(fileName);
				}
			} else {
				_indexMap.InOrder().ToList().ForEach(x => x.Dispose());
			}

			_indexMap.InOrder().ToList().ForEach(x => x.WaitForDisposal(TimeSpan.FromMilliseconds(5000)));
		}

		private IndexEntry CreateIndexEntry(IndexKey key) {
			key = CreateIndexKey(key.StreamId, key.Version, key.Position);
			return new IndexEntry(key.Hash, key.Version, key.Position);
		}

		private ulong UpgradeHash(string streamId, ulong lowHash) {
			return lowHash << 32 | _highHasher.Hash(streamId);
		}

		private ulong CreateHash(string streamId) {
			return (ulong)_lowHasher.Hash(streamId) << 32 | _highHasher.Hash(streamId);
		}

		private IndexKey CreateIndexKey(string streamId, long version, long position) {
			return new IndexKey(streamId, version, position, CreateHash(streamId));
		}

		private class TableItem {
			public readonly ISearchTable Table;
			public readonly long PrepareCheckpoint;
			public readonly long CommitCheckpoint;
			public readonly int Level;
			public readonly bool IsFromIndexMap;
			public TableItem(ISearchTable table, long prepareCheckpoint, long commitCheckpoint, int level, bool isFromIndexMap) {
				Table = table;
				PrepareCheckpoint = prepareCheckpoint;
				CommitCheckpoint = commitCheckpoint;
				Level = level;
				IsFromIndexMap = isFromIndexMap;
			}
		}

		private void ForceIndexVerifyOnNextStartup() {
			Log.Debug("Forcing index verification on next startup");
			string path = Path.Combine(_directory, ForceIndexVerifyFilename);
			try {
				using (FileStream fs = new FileStream(path, FileMode.OpenOrCreate)) {
				}

				;
			} catch {
				Log.Error("Could not create force index verification file at: {path}", path);
			}

			return;
		}

		private bool ShouldForceIndexVerify() {
			string path = Path.Combine(_directory, ForceIndexVerifyFilename);
			return File.Exists(path);
		}

		private void DeleteForceIndexVerifyFile() {
			string path = Path.Combine(_directory, ForceIndexVerifyFilename);
			try {
				if (File.Exists(path)) {
					File.SetAttributes(path, FileAttributes.Normal);
					File.Delete(path);
				}
			} catch {
				Log.Error("Could not delete force index verification file at: {path}", path);
			}
		}
	}
}

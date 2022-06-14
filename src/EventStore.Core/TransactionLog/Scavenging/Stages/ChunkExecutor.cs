using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Core.Exceptions;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class ChunkExecutor {
		protected static readonly ILogger Log = LogManager.GetLoggerFor<ChunkExecutor>();
	}

	public class ChunkExecutor<TStreamId, TRecord> : ChunkExecutor, IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TRecord> _chunkManager;
		private readonly long _chunkSize;
		private readonly bool _unsafeIgnoreHardDeletes;
		private readonly int _cancellationCheckPeriod;
		private readonly Throttle _throttle;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TRecord> chunkManager,
			long chunkSize,
			bool unsafeIgnoreHardDeletes,
			int cancellationCheckPeriod,
			Throttle throttle) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_chunkSize = chunkSize;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
			_cancellationCheckPeriod = cancellationCheckPeriod;
			_throttle = throttle;
		}

		public void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			Log.Trace("SCAVENGING: Starting new scavenge chunk execution phase for {scavengePoint}",
				scavengePoint.GetName());

			var checkpoint = new ScavengeCheckpoint.ExecutingChunks(
				scavengePoint: scavengePoint,
				doneLogicalChunkNumber: default);
			state.SetCheckpoint(checkpoint);
			Execute(checkpoint, state, scavengerLogger, cancellationToken);
		}

		public void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			Log.Trace("SCAVENGING: Executing chunks from checkpoint: {checkpoint}", checkpoint);

			var startFromChunk = checkpoint?.DoneLogicalChunkNumber + 1 ?? 0;
			var scavengePoint = checkpoint.ScavengePoint;
			var sw = new Stopwatch();

			foreach (var physicalChunk in GetAllPhysicalChunks(startFromChunk, scavengePoint)) {
				var transaction = state.BeginTransaction();
				try {
					var physicalWeight = state.SumChunkWeights(
						physicalChunk.ChunkStartNumber,
						physicalChunk.ChunkEndNumber);

					if (physicalWeight > scavengePoint.Threshold || _unsafeIgnoreHardDeletes) {
						ExecutePhysicalChunk(
							physicalWeight,
							scavengePoint,
							state,
							scavengerLogger,
							physicalChunk,
							sw,
							cancellationToken);

						state.ResetChunkWeights(
							physicalChunk.ChunkStartNumber,
							physicalChunk.ChunkEndNumber);
					}

					cancellationToken.ThrowIfCancellationRequested();

					transaction.Commit(
						new ScavengeCheckpoint.ExecutingChunks(
							scavengePoint,
							physicalChunk.ChunkEndNumber));
				} catch {
					// invariant: there is always an open transaction whenever an exception can be thrown
					transaction.Rollback();
					throw;
				}
				_throttle.Rest(cancellationToken);
			}
		}

		private IEnumerable<IChunkReaderForExecutor<TStreamId, TRecord>> GetAllPhysicalChunks(
			int startFromChunk,
			ScavengePoint scavengePoint) {

			var scavengePos = _chunkSize * startFromChunk;
			var upTo = scavengePoint.Position;
			while (scavengePos < upTo) {
				// in bounds because we stop before the scavenge point
				var physicalChunk = _chunkManager.GetChunkReaderFor(scavengePos);

				if (!physicalChunk.IsReadOnly)
					yield break;

				yield return physicalChunk;

				scavengePos = physicalChunk.ChunkEndPosition;
			}
		}

		private void ExecutePhysicalChunk(
			float physicalWeight,
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			IChunkReaderForExecutor<TStreamId, TRecord> sourceChunk,
			Stopwatch sw,
			CancellationToken cancellationToken) {

			sw.Restart();

			int chunkStartNumber = sourceChunk.ChunkStartNumber;
			long chunkStartPos = sourceChunk.ChunkStartPosition;
			int chunkEndNumber = sourceChunk.ChunkEndNumber;
			long chunkEndPos = sourceChunk.ChunkEndPosition;
			var oldChunkName = sourceChunk.Name;

			Log.Trace(
				"SCAVENGING: started to scavenge physical chunk: {oldChunkName} " +
				"with weight {physicalWeight:N0}. " +
				"{chunkStartNumber} => {chunkEndNumber} ({chunkStartPosition} => {chunkEndPosition})",
				oldChunkName,
				physicalWeight,
				chunkStartNumber, chunkEndNumber, chunkStartPos, chunkEndPos);

			IChunkWriterForExecutor<TStreamId, TRecord> outputChunk;
			try {
				outputChunk = _chunkManager.CreateChunkWriter(sourceChunk);
				Log.Trace(
					"SCAVENGING: Resulting temp chunk file: {tmpChunkPath}.", 
					Path.GetFileName(outputChunk.FileName));

			} catch (IOException ex) {
				Log.ErrorException(ex,
					"IOException during creating new chunk for scavenging purposes. " +
					"Stopping scavenging process...");
				throw;
			}

			try {
				var cancellationCheckCounter = 0;
				var discardedCount = 0;
				var keptCount = 0;

				// nonPrepareRecord and prepareRecord ae reused through the iteration
				var nonPrepareRecord = new RecordForExecutor<TStreamId, TRecord>.NonPrepare();
				var prepareRecord = new RecordForExecutor<TStreamId, TRecord>.Prepare();

				foreach (var isPrepare in sourceChunk.ReadInto(nonPrepareRecord, prepareRecord)) {
					//qq add a test to make sure we keep the system records
					if (isPrepare) {
						if (ShouldDiscard(state, scavengePoint, prepareRecord)) {
							discardedCount++;
						} else {
							keptCount++;
							outputChunk.WriteRecord(prepareRecord);
						}
					} else {
						keptCount++;
						outputChunk.WriteRecord(nonPrepareRecord);
					}

					if (++cancellationCheckCounter == _cancellationCheckPeriod) {
						cancellationCheckCounter = 0;
						cancellationToken.ThrowIfCancellationRequested();
					}
				}

				Log.Trace(
					"SCAVENGING: Scavenging {oldChunkName} traversed {recordsCount} including {filteredCount}.",
					oldChunkName, discardedCount + keptCount, keptCount);

				outputChunk.Complete(out var newFileName, out var newFileSize);

				var elapsed = sw.Elapsed;
				Log.Trace(
					"SCAVENGING: Scavenging of chunks:"
					+ "\n{oldChunkName}"
					+ "\ncompleted in {elapsed}."
					+ "\nNew chunk: {tmpChunkPath} --> #{chunkStartNumber}-{chunkEndNumber} ({newChunk})."
					+ "\nOld chunk total size: {oldSize}, scavenged chunk size: {newSize}.",
					oldChunkName,
					elapsed,
					Path.GetFileName(outputChunk.FileName), chunkStartNumber, chunkEndNumber,
					Path.GetFileName(newFileName),
					sourceChunk.FileSize, newFileSize);

				var spaceSaved = sourceChunk.FileSize - newFileSize;
				scavengerLogger.ChunksScavenged(chunkStartNumber, chunkEndNumber, elapsed, spaceSaved);

			} catch (FileBeingDeletedException exc) {
				Log.Info(
					"SCAVENGING: Got FileBeingDeletedException exception during scavenging, that probably means some chunks were re-replicated."
					+ "\nStopping scavenging and removing temp chunk '{tmpChunkPath}'..."
					+ "\nException message: {e}.",
					outputChunk.FileName,
					exc.Message);

				outputChunk.Abort(deleteImmediately: true);
				throw;

			} catch (OperationCanceledException) {
				Log.Info("SCAVENGING: Cancelled at: {oldChunkName}", oldChunkName);
				outputChunk.Abort(deleteImmediately: false);
				throw;

			} catch (Exception ex) {
				Log.InfoException(
					ex,
					"SCAVENGING: Got exception while scavenging chunk: #{chunkStartNumber}-{chunkEndNumber}.",
					chunkStartNumber, chunkEndNumber);

				outputChunk.Abort(deleteImmediately: true);
				throw;
			}
		}

		private bool ShouldDiscard(
			IScavengeStateForChunkExecutor<TStreamId> state,
			ScavengePoint scavengePoint,
			RecordForExecutor<TStreamId, TRecord>.Prepare record) {

			// the discard points ought to be sufficient, but sometimes this will be quicker
			// and it is a nice safty net
			if (record.LogPosition >= scavengePoint.Position)
				return false;

			if (!record.IsSelfCommitted) {
				// we could discard from transactions sometimes, either by accumulating a state for them
				// or doing a similar trick as old scavenge and limiting it to transactions that were
				// stated and commited in the same chunk. however for now this isn't considered so
				// important because someone with transactions to scavenge has probably scavenged them
				// already with old scavenge. could be added later
				return false;
			}

			//qq consider how/where to cache the this stuff per stream for quick lookups
			var details = GetStreamExecutionDetails(
				state,
				record.StreamId);

			if (details.IsTombstoned) {
				if (_unsafeIgnoreHardDeletes) {
					// remove _everything_ for metadata and original streams
					Log.Info(
						"SCAVENGING: Removing hard deleted stream tombstone for stream {stream} at position {transactionPosition}",
						record.StreamId, record.LogPosition);
					return true;
				}

				if (_metastreamLookup.IsMetaStream(record.StreamId)) {
					// when the original stream is tombstoned we can discard the _whole_ metadata stream
					return true;
				}

				// otherwise obey the discard points below.
			}

			// if definitePoint says discard then discard.
			if (details.DiscardPoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!details.MaybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			// discard said no, but maybe discard said yes
			if (!details.MaxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - details.MaxAge;
		}

		private ChunkExecutionInfo GetStreamExecutionDetails(
			IScavengeStateForChunkExecutor<TStreamId> state,
			TStreamId streamId) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!state.TryGetMetastreamData(streamId, out var metastreamData)) {
					metastreamData = MetastreamData.Empty;
				}

				return new ChunkExecutionInfo(
					isTombstoned: metastreamData.IsTombstoned,
					discardPoint: metastreamData.DiscardPoint,
					maybeDiscardPoint: DiscardPoint.KeepAll,
					maxAge: null);
			} else {
				// original stream
				if (state.TryGetChunkExecutionInfo(streamId, out var details)) {
					return details;
				} else {
					return new ChunkExecutionInfo(
						isTombstoned: false,
						discardPoint: DiscardPoint.KeepAll,
						maybeDiscardPoint: DiscardPoint.KeepAll,
						maxAge: null);
				}
			}
		}
	}
}

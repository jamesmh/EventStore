using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Utils;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Index;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.Services.Storage {
	public abstract class StorageScavenger {
	}

	public class StorageScavenger<TStreamId> :
		StorageScavenger,
		IHandle<ClientMessage.ScavengeDatabase>,
		IHandle<ClientMessage.StopDatabaseScavenge>,
		IHandle<SystemMessage.StateChangeMessage> {
		private readonly TFChunkDb _db;
		private readonly ITableIndex<TStreamId> _tableIndex;
		private readonly IReadIndex<TStreamId> _readIndex;
		private readonly IMetastreamLookup<TStreamId> _metastreams;
		private readonly bool _alwaysKeepScavenged;
		private readonly bool _mergeChunks;
		private readonly bool _unsafeIgnoreHardDeletes;
		private readonly ITFChunkScavengerLogManager _logManager;
		private readonly object _lock = new object();

		private TFChunkScavenger<TStreamId> _currentScavenge;
		private CancellationTokenSource _cancellationTokenSource;

		public StorageScavenger(TFChunkDb db, ITableIndex<TStreamId> tableIndex, IReadIndex<TStreamId> readIndex,
			IMetastreamLookup<TStreamId> metastreams,
			ITFChunkScavengerLogManager logManager, bool alwaysKeepScavenged, bool mergeChunks,
			bool unsafeIgnoreHardDeletes) {
			Ensure.NotNull(db, "db");
			Ensure.NotNull(logManager, "logManager");
			Ensure.NotNull(tableIndex, "tableIndex");
			Ensure.NotNull(readIndex, "readIndex");
			Ensure.NotNull(metastreams, nameof(metastreams));

			_db = db;
			_tableIndex = tableIndex;
			_readIndex = readIndex;
			_metastreams = metastreams;
			_alwaysKeepScavenged = alwaysKeepScavenged;
			_mergeChunks = mergeChunks;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
			_logManager = logManager;
		}

		public void Handle(SystemMessage.StateChangeMessage message) {
			if (message.State == VNodeState.Leader || message.State == VNodeState.Follower) {
				_logManager.Initialise();
			}
		}

		public void Handle(ClientMessage.ScavengeDatabase message) {
			if (IsAllowed(message.User, message.CorrelationId, message.Envelope)) {
				lock (_lock) {
					if (_currentScavenge != null) {
						message.Envelope.ReplyWith(new ClientMessage.ScavengeDatabaseResponse(message.CorrelationId,
							ClientMessage.ScavengeDatabaseResponse.ScavengeResult.InProgress,
							_currentScavenge.ScavengeId));
					} else {
						var tfChunkScavengerLog = _logManager.CreateLog();

						_cancellationTokenSource = new CancellationTokenSource();
						var newScavenge = _currentScavenge = new TFChunkScavenger<TStreamId>(_db, tfChunkScavengerLog, _tableIndex,
							_readIndex, _metastreams, unsafeIgnoreHardDeletes: _unsafeIgnoreHardDeletes, threads: message.Threads);
						var newScavengeTask = _currentScavenge.Scavenge(_alwaysKeepScavenged, _mergeChunks,
							message.StartFromChunk, _cancellationTokenSource.Token);

						HandleCleanupWhenFinished(newScavengeTask, newScavenge);

						message.Envelope.ReplyWith(new ClientMessage.ScavengeDatabaseResponse(message.CorrelationId,
							ClientMessage.ScavengeDatabaseResponse.ScavengeResult.Started,
							tfChunkScavengerLog.ScavengeId));
					}
				}
			}
		}

		public void Handle(ClientMessage.StopDatabaseScavenge message) {
			if (IsAllowed(message.User, message.CorrelationId, message.Envelope)) {
				lock (_lock) {
					if (_currentScavenge != null && _currentScavenge.ScavengeId == message.ScavengeId) {
						_cancellationTokenSource.Cancel();

						message.Envelope.ReplyWith(new ClientMessage.ScavengeDatabaseResponse(message.CorrelationId,
							ClientMessage.ScavengeDatabaseResponse.ScavengeResult.Stopped,
							_currentScavenge.ScavengeId));
					} else {
						message.Envelope.ReplyWith(new ClientMessage.ScavengeDatabaseResponse(message.CorrelationId,
							ClientMessage.ScavengeDatabaseResponse.ScavengeResult.InvalidScavengeId,
							_currentScavenge?.ScavengeId));
					}
				}
			}
		}

		private async void HandleCleanupWhenFinished(Task newScavengeTask, TFChunkScavenger<TStreamId> newScavenge) {
			// Clean up the reference to the TfChunkScavenger once it's finished.
			await newScavengeTask.ConfigureAwait(false);

			lock (_lock) {
				if (newScavenge == _currentScavenge) {
					_currentScavenge = null;
				}
			}
		}

		private bool IsAllowed(ClaimsPrincipal user, Guid correlationId, IEnvelope envelope) {
			if (user == null || (!user.LegacyRoleCheck(SystemRoles.Admins) && !user.LegacyRoleCheck(SystemRoles.Operations))) {
				envelope.ReplyWith(new ClientMessage.ScavengeDatabaseResponse(correlationId,
					ClientMessage.ScavengeDatabaseResponse.ScavengeResult.Unauthorized, null));
				return false;
			}

			return true;
		}
	}
}

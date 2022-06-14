using System;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Core.Messages;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.Services.Storage {
	// This abstracts the scavenge implementation away from the StorageScavenger
	// The resulting scavenger is used for one continuous run. If it is cancelled or
	// completed then starting scavenge again will instantiate another scavenger
	// with a different id.
	interface IScavengerFactory {
		IScavenger Create(
			ClientMessage.ScavengeDatabase message,
			ITFChunkScavengerLog logger);
	}

	//qqqq rename, this factory covers new and old
	public class NewScavengerFactory : IScavengerFactory {
		private readonly Func<ClientMessage.ScavengeDatabase, ITFChunkScavengerLog, IScavenger> _create;

		public NewScavengerFactory(
			Func<ClientMessage.ScavengeDatabase, ITFChunkScavengerLog, IScavenger> create) {

			_create = create;
		}

		public IScavenger Create(
			ClientMessage.ScavengeDatabase message,
			ITFChunkScavengerLog logger) {

			return _create(message, logger);
		}
	}

	public class OldScavenger : IScavenger {
		private readonly bool _alwaysKeepScavenged;
		private readonly bool _mergeChunks;
		private readonly int _startFromChunk;
		private readonly TFChunkScavenger _tfChunkScavenger;

		public string ScavengeId => _tfChunkScavenger.ScavengeId;

		public OldScavenger(
			bool alwaysKeepScaveged,
			bool mergeChunks,
			int startFromChunk,
			TFChunkScavenger tfChunkScavenger) {

			_alwaysKeepScavenged = alwaysKeepScaveged;
			_mergeChunks = mergeChunks;
			_startFromChunk = startFromChunk;
			_tfChunkScavenger = tfChunkScavenger;
		}

		public Task ScavengeAsync(CancellationToken cancellationToken) {
			return _tfChunkScavenger.Scavenge(
				alwaysKeepScavenged: _alwaysKeepScavenged,
				mergeChunks: _mergeChunks,
				startFromChunk: _startFromChunk,
				ct: cancellationToken);
		}
	}
}

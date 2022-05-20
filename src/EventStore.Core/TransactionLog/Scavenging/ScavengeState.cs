﻿using System.Collections.Generic;
using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Data;

namespace EventStore.Core.TransactionLog.Scavenging {
	// This datastructure is read and written to by the Accumulator/Calculator/Executors.
	// They contain the scavenge logic, this is just the holder of the data.
	//
	// we store data for metadata streams and for original streams, but we need to store
	// different data for each so we have two maps. we have one collision detector since
	// we need to detect collisions between all of the streams.
	// we don't need to store data for every original stream, only ones that need scavenging.
	public class ScavengeState<TStreamId> : IScavengeState<TStreamId> {

		private readonly CollisionDetector<TStreamId> _collisionDetector;

		// data stored keyed against metadata streams
		private readonly MetastreamCollisionMap<TStreamId> _metastreamDatas;

		// data stored keyed against original (non-metadata) streams
		private readonly OriginalStreamCollisionMap<TStreamId> _originalStreamDatas;

		private readonly IScavengeMap<int, ChunkTimeStampRange> _chunkTimeStampRanges;
		private readonly IChunkWeightScavengeMap _chunkWeights;
		private readonly IScavengeMap<Unit, ScavengeCheckpoint> _checkpointStorage;
		private readonly ITransactionManager _transactionManager;

		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;

		public ScavengeState(
			ILongHasher<TStreamId> hasher,
			IMetastreamLookup<TStreamId> metastreamLookup,
			IScavengeMap<TStreamId, Unit> collisionStorage,
			IScavengeMap<ulong, TStreamId> hashes,
			IMetastreamScavengeMap<ulong> metaStorage,
			IMetastreamScavengeMap<TStreamId> metaCollisionStorage,
			IOriginalStreamScavengeMap<ulong> originalStorage,
			IOriginalStreamScavengeMap<TStreamId> originalCollisionStorage,
			IScavengeMap<Unit, ScavengeCheckpoint> checkpointStorage,
			IScavengeMap<int, ChunkTimeStampRange> chunkTimeStampRanges,
			IChunkWeightScavengeMap chunkWeights,
			ITransactionManager transactionManager) {

			//qq inject this so that in log v3 we can have a trivial implementation
			//qq to save us having to look up the stream names repeatedly
			_collisionDetector = new CollisionDetector<TStreamId>(
				//qq configurable cacheMaxCount
				new LruCachingScavengeMap<ulong, TStreamId>(hashes, cacheMaxCount: 10_000),
				collisionStorage,
				hasher);

			_hasher = hasher;
			_metastreamLookup = metastreamLookup;
			_checkpointStorage = checkpointStorage;

			_metastreamDatas = new MetastreamCollisionMap<TStreamId>(
				_hasher,
				_collisionDetector.IsCollision,
				metaStorage,
				metaCollisionStorage);

			_originalStreamDatas = new OriginalStreamCollisionMap<TStreamId>(
				_hasher,
				_collisionDetector.IsCollision,
				originalStorage,
				originalCollisionStorage);

			_chunkTimeStampRanges = chunkTimeStampRanges;
			_chunkWeights = chunkWeights;

			_transactionManager = transactionManager;
		}

		// reuses the same transaction object for multiple transactions.
		// caller is reponsible for committing, rolling back, or disposing
		// the transaction before calling BeginTransaction again
		public ITransactionCompleter BeginTransaction() {
			_transactionManager.Begin();
			return _transactionManager;
		}

		public bool TryGetCheckpoint(out ScavengeCheckpoint checkpoint) =>
			_checkpointStorage.TryGetValue(Unit.Instance, out checkpoint);

		public IEnumerable<TStreamId> AllCollisions() {
			return _collisionDetector.AllCollisions();
		}

		public bool TryGetOriginalStreamData(
			TStreamId streamId,
			out OriginalStreamData originalStreamData) =>

			_originalStreamDatas.TryGetValue(streamId, out originalStreamData);

		//
		// FOR ACCUMULATOR
		//

		public void DetectCollisions(TStreamId streamId) {
			var collisionResult = _collisionDetector.DetectCollisions(
				streamId,
				out var collision);

			if (collisionResult == CollisionResult.NewCollision) {
				_metastreamDatas.NotifyCollision(collision);
				_originalStreamDatas.NotifyCollision(collision);
			}
		}

		public void SetMetastreamDiscardPoint(TStreamId metastreamId, DiscardPoint discardPoint) {
			_metastreamDatas.SetDiscardPoint(metastreamId, discardPoint);
		}

		public void SetMetastreamTombstone(TStreamId metastreamId) {
			_metastreamDatas.SetTombstone(metastreamId);
		}

		public void SetOriginalStreamMetadata(TStreamId originalStreamId, StreamMetadata metadata) {
			_originalStreamDatas.SetMetadata(originalStreamId, metadata);
		}

		public void SetOriginalStreamTombstone(TStreamId originalStreamId) {
			_originalStreamDatas.SetTombstone(originalStreamId);
		}

		public void SetChunkTimeStampRange(int logicalChunkNumber, ChunkTimeStampRange range) {
			_chunkTimeStampRanges[logicalChunkNumber] = range;
		}

		//
		// FOR CALCULATOR
		//

		//qq name.. something to do with active?
		public IEnumerable<(StreamHandle<TStreamId>, OriginalStreamData)> OriginalStreamsToScavenge(
			StreamHandle<TStreamId> checkpoint) {

			return _originalStreamDatas.Enumerate(checkpoint);
		}

		public void SetOriginalStreamDiscardPoints(
			StreamHandle<TStreamId> handle,
			CalculationStatus status,
			DiscardPoint discardPoint,
			DiscardPoint maybeDiscardPoint) {

			_originalStreamDatas.SetDiscardPoints(handle, status, discardPoint, maybeDiscardPoint);
		}

		public void IncreaseChunkWeight(int logicalChunkNumber, float extraWeight) {
			_chunkWeights.IncreaseWeight(logicalChunkNumber, extraWeight);
		}

		public bool TryGetChunkTimeStampRange(int logicalChunkNumber, out ChunkTimeStampRange range) =>
			_chunkTimeStampRanges.TryGetValue(logicalChunkNumber, out range);

		//
		// FOR CHUNK EXECUTOR
		//

		public float SumChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber) =>
			_chunkWeights.SumChunkWeights(startLogicalChunkNumber, endLogicalChunkNumber);

		public void ResetChunkWeights(int startLogicalChunkNumber, int endLogicalChunkNumber) {
			_chunkWeights.ResetChunkWeights(startLogicalChunkNumber, endLogicalChunkNumber);
		}

		public bool TryGetChunkExecutionInfo(
			TStreamId streamId,
			out ChunkExecutionInfo info) =>

			_originalStreamDatas.TryGetChunkExecutionInfo(streamId, out info);


		public bool TryGetMetastreamData(TStreamId streamId, out MetastreamData data) =>
			_metastreamDatas.TryGetValue(streamId, out data);

		//
		// FOR INDEX EXECUTOR
		//

		public bool TryGetIndexExecutionInfo(
			StreamHandle<TStreamId> handle,
			out IndexExecutionInfo info) {

			// here we know that the handle is of the correct kind
			// but we do not know whether it is for a metastream or an originalstream.
			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					// not a collision, but we do not know whether it is a metastream or not.
					// check both maps (better if we didnt have to though..)
					return TryGetDiscardPointForOriginalStream(handle, out info)
						|| TryGetDiscardPointForMetadataStream(handle, out info);
				case StreamHandle.Kind.Id:
					// collision, but at least we can tell whether it is a metastream or not.
					// so just check one map.
					return _metastreamLookup.IsMetaStream(handle.StreamId)
						? TryGetDiscardPointForMetadataStream(handle, out info)
						: TryGetDiscardPointForOriginalStream(handle, out info);
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		private bool TryGetDiscardPointForMetadataStream(
			StreamHandle<TStreamId> handle,
			out IndexExecutionInfo info) {

			if (!_metastreamDatas.TryGetValue(handle, out var data)) {
				info = default;
				return false;
			}

			info = new IndexExecutionInfo(
				isMetastream: true,
				isTombstoned: data.IsTombstoned,
				discardPoint: data.DiscardPoint);
			return true;
		}

		private bool TryGetDiscardPointForOriginalStream(
			StreamHandle<TStreamId> handle,
			out IndexExecutionInfo info) {

			if (!_originalStreamDatas.TryGetValue(handle, out var data)) {
				info = default;
				return false;
			}

			info = new IndexExecutionInfo(
				isMetastream: false,
				isTombstoned: data.IsTombstoned,
				discardPoint: data.DiscardPoint);
			return true;
		}

		public bool IsCollision(ulong streamHash) {
			//qq track these as we go rather than calculating each time on demand.
			var collidingHashes = new HashSet<ulong>();
			
			foreach (var collidingKey in _collisionDetector.AllCollisions()) {
				collidingHashes.Add(_hasher.Hash(collidingKey));
			}

			return collidingHashes.Contains(streamHash);
		}

		//
		// For cleaner
		//

		public bool AllChunksExecuted() =>
			_chunkWeights.AllWeightsAreZero();

		public void DeleteOriginalStreamData(bool deleteArchived) {
			_originalStreamDatas.DeleteMany(deleteArchived: deleteArchived);
		}

		public void DeleteMetastreamData() {
			_metastreamDatas.DeleteAll();
		}
	}
}

﻿using System;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Chunks;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Calculator<TStreamId> : ICalculator<TStreamId> {
		private readonly ILongHasher<TStreamId> _hasher;
		private readonly IIndexReaderForCalculator<TStreamId> _index;
		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;

		public Calculator(
			ILongHasher<TStreamId> hasher,
			IIndexReaderForCalculator<TStreamId> index,
			IMetastreamLookup<TStreamId> metastreamLookup) {

			_hasher = hasher;
			_index = index;
			_metastreamLookup = metastreamLookup;
		}

		public void Calculate(
			ScavengePoint scavengePoint,
			IScavengeStateForCalculator<TStreamId> state) {

			// iterate through the metadata streams, for each one use the metadata to modify the
			// discard point of the stream and store it. along the way note down which chunks
			// the records to be discarded.
			foreach (var (metastreamHandle, metastreamData) in state.MetastreamDatas) {
				var originalStreamHandle = GetOriginalStreamHandle(
					state,
					metastreamHandle,
					metastreamData.OriginalStreamHash);

				//qq it would be neat if this interface gave us some hint about the location of
				// the DP so that we could set it in a moment cheaply without having to search.
				// although, if its a wal that'll be cheap anyway.
				//
				//qq i dont think we can save this lookup by storing it on the metastreamData
				// because when we find, say, the tombstone of the original stream and want to set its
				// DP, the metadata stream does not necessarily exist.
				// unless we don't care here about the tombstone, and only care about things that could
				// be conveniently set on the metastreamdata by the accumulator
				//qq if the scavengemap supports RMW that might have a bearing too, but for now maybe
				// this is just overcomplicating things.
				//qq how bad is this, how much could we save
				if (!state.TryGetOriginalStreamData(
						originalStreamHandle,
						out var originalStreamData)) {
					originalStreamData = new EnrichedDiscardPoint(
						isTombstoned: false,
						discardPoint: DiscardPoint.KeepAll);
				}

				var adjustedDiscardPoint = CalculateDiscardPointForStream(
					state,
					originalStreamHandle,
					originalStreamData.IsTombstoned,
					originalStreamData.DiscardPoint,
					metastreamData,
					scavengePoint);

				state.SetOriginalStreamData(
					originalStreamHandle,
					new EnrichedDiscardPoint(
						isTombstoned: originalStreamData.IsTombstoned,
						adjustedDiscardPoint));
			}
		}

		//qq make sure all the cases are covered by the tests
		// This gets the handle to the original stream, given the handle to the metadata stream and the
		// hash of the original stream.
		//
		// The resulting handle needs to contain the original stream name if it is a collision,
		// and just the hash if it is not a collision.
		private StreamHandle<TStreamId> GetOriginalStreamHandle(
			IScavengeStateForCalculator<TStreamId> state,
			StreamHandle<TStreamId> metastreamHandle,
			ulong originalStreamHash) {

			if (!state.IsCollision(originalStreamHash)) {
				return StreamHandle.ForHash<TStreamId>(originalStreamHash);
			}

			if (metastreamHandle.Kind == StreamHandle.Kind.Id) {
				var originalStreamId = _metastreamLookup.OriginalStreamOf(metastreamHandle.StreamId);
				return StreamHandle.ForStreamId(originalStreamId);
			}

			if (metastreamHandle.Kind != StreamHandle.Kind.Hash) {
				throw new ArgumentOutOfRangeException(nameof(metastreamHandle), metastreamHandle, null);
			}

			// metastreamHandle is a hash, so the metastream does not collide with anything.
			foreach (var collision in state.Collisions()) {
				// we are calculating the originalStreamHandle. we know that the originalStream
				// collides, so the handle will be a streamId (not a hash) and that streamId is
				// in the list of collisions, which is short. We just need to pick the right one.
				// it is the collision that:
				//   1. is an originalstream
				//   2. has a metadata stream name that hashes to the right hash
				//      (metastreamHandle.StreamHash)
				if (_metastreamLookup.IsMetaStream(collision))
					continue;

				var metastreamOfCollision = _metastreamLookup.MetaStreamOf(collision);
				if (_hasher.Hash(metastreamOfCollision) != metastreamHandle.StreamHash)
					continue;

				return StreamHandle.ForStreamId(collision);
			}

			throw new InvalidOperationException(
				$"Could not get the original stream handle for " +
				$"metaStream: {metastreamHandle}. " +
				$"originalStreamHash: {originalStreamHash}. " +
				"corrupt scavenge state?"); //qq add detail
		}

		// streamHandle: handle to the original stream
		// discardPoint: the discard point previously set by the accumulator
		// metadata: metadata of the original stream (stored in the metadata stream)
		private DiscardPoint CalculateDiscardPointForStream(
			IScavengeStateForCalculator<TStreamId> scavengeState,
			StreamHandle<TStreamId> streamHandle,
			bool isTombstoned,
			DiscardPoint discardPoint,
			MetastreamData metadata,
			ScavengePoint scavengePoint) {

			// Events in original streams can be discarded because of:
			//   Tombstones, TruncateBefore, MaxCount, MaxAge.
			// SO:
			// 1. determine an overall discard point from
			//       - a) discard point (this covers Tombstones)
			//       - b) truncate before
			//       - c) maxcount
			// 2. and a logposition cutoff for
			//       - d) maxage   <- this one does require looking at the location of the events
			// 3. iterate through the eventinfos calling discard for each one that is discarded
			//    by the discard point and logpositioncutoff. this discovers the final discard point.
			// 4. return the final discard point.
			//
			// there are, therefore, three discard points to keep clear,
			//qq and which all need better names
			// - the `originalDiscardPoint` determined by the Accumulator without respect to the
			//   scavengepoint (but includes tombstone)
			// - the `modifiedDiscardPoint` which takes into account the maxcount and tb by applying the
			//   scavenge point
			// - the finalDiscardPoint which takes into account the maxage and
			//   ensures not discarding the last event
			//qq        ^ hum.. this last might involve moving the discard point backwards
			//            so there are some points when we do need to move it backwards
			// this method calculates the FinalDiscardPoint

			//qq consider what will happen here if the strea, doesn't exist
			//  if it doesn't exist at all then presumably there is nothing to scavenge
			//    we can set the disard point to anything
			//  if it doesn't exist before the scavenge point but does later then
			//    there is nothing to remove as part of this scavenge, but we need to be careful
			//    not to remove the later events.
			var lastEventNumber = _index.GetLastEventNumber(
				streamHandle,
				scavengePoint);

			var logPositionCutoff = 0L;

			// if the stream is tombstoned, the accumulator already set the discard point to discard
			// everything except the tombstone. just do that, no need for further adjustment of the
			// discard point.
			if (!isTombstoned) {
				//qq check these all carefuly

				// Discard more if required by TruncateBefore
				if (metadata.TruncateBefore != null) {
					var dpTruncateBefore = DiscardPoint.DiscardBefore(metadata.TruncateBefore.Value);
					discardPoint = DiscardPoint.AnyOf(discardPoint, dpTruncateBefore);
				}

				// Discard more if required by MaxCount
				if (metadata.MaxCount != null) {
					//qq turn these into tests. although remember overall we will never discard the last
					//event
					// say the lastEventNumber in the stream is number 5
					// and the maxCount is 2
					// then we want to keep events numbered 4 and 5
					// we want to discard events including event 3
					//
					// say the lastEventNumber is 5
					// and the maxCount is 0
					// we want to discard events including event 5
					//
					// say the lastEventNumber is 5
					// and the maxCount is 6
					// we want to discard events including event -1 (keep all events)
					//
					// say the lastEventNumber is 5
					// and the maxCount is 5
					// we want to discard events including 0
					var lastEventToDiscard = lastEventNumber - metadata.MaxCount.Value;
					//qq be careful not to call this with long.max
					var dpMaxCount = DiscardPoint.DiscardIncluding(lastEventToDiscard);
					discardPoint = DiscardPoint.AnyOf(discardPoint, dpMaxCount);
				}

				// Discard more if required by MaxAge
				// here we determine the logPositionCutoff, we won't know which event number
				// (DiscardPoint) that will translate into until we start looking at the EventInfos.
				if (metadata.MaxAge != null) {
					logPositionCutoff = CalculateLogCutoffForMaxAge(
						scavengePoint,
						metadata.MaxAge.Value);
				}
			}

			// Now discardPoint and logPositionCutoff are set. iterate through the EventInfos calling
			// discard for each one that is discarded (so each chunk knows how many records are discarded)
			// This determines the final DiscardPoint, accounting for logPositionCutoff (MaxAge)
			// Note: when the handle is a hash the ReadEventInfoForward call is index-only

			// read in slices because the stream might be huge.
			const int maxCount = 100; //qq what would be sensible? probably pretty large
			var fromEventNumber = 0L; //qq maybe from the previous scavenge point
			while (true) {
				//qq limit the read to the scavengepoint too?
				var slice = _index.ReadEventInfoForward(
					streamHandle,
					fromEventNumber,
					maxCount,
					scavengePoint);

				//qq naive, we dont need to check every event, we could check the last one
				// and if that is to be discarded then we can discard everything in this slice.
				foreach (var eventInfo in slice) {
					//qq consider this inequality
					var isLastEventInStream = eventInfo.EventNumber == lastEventNumber;
					var beforeScavengePoint = eventInfo.LogPosition < scavengePoint.Position;
					//qq correct inequality?
					var discardForLogPosition = eventInfo.LogPosition < logPositionCutoff;
					var discardForEventNumber = discardPoint.ShouldDiscard(eventInfo.EventNumber);

					var discard =
						!isLastEventInStream &&
						beforeScavengePoint &&
						(discardForLogPosition || discardForEventNumber);

					// always keep the last event in the stream.
					if (discard) {
						Discard(scavengeState, eventInfo.LogPosition);
					} else {
						// found the first one to keep. we are done discarding.
						return DiscardPoint.DiscardBefore(eventInfo.EventNumber);
					}
				}

				if (slice.Length < maxCount) {
					//qq we discarded everything in the stream, this should never happen
					// since we always keep the last event (..unless ignore hard deletes
					// is enabled)
					// which ones are otherwise in danger of removing all the events?
					//  hard deleted?
					//
					//qq although, the old scavenge might be capable of removing all the events
					// after this scavenge point... which would produce this condition.
					//
					// in these situatiosn what discard point should we return, or do we need to abort
					throw new Exception("panic"); //qq dont panic really shouldn't
				}

				fromEventNumber += slice.Length;
			}
		}

		//qq rename.
		//qq fill this in using IsExpiredByMaxAge below
		// it wants to return the log position of a record (probably at the start of a chunk)
		// such that the event at that position is older than the maxage, allowing for some skew
		// so that we are pretty sure that any events before that position would also be excluded
		// by that maxage.
		static long CalculateLogCutoffForMaxAge(
			ScavengePoint scavengePoint,
			TimeSpan maxAge) {

			// binary chop or maybe just scan backwards the chunk starting at times.
			throw new NotImplementedException();
		}

		//qq nb: index-only shortcut for maxage works for transactions too because it is the
		// prepare timestamp that we use not the commit timestamp.
		//qq replace this with CalculateLogCutoffForMaxAge above
		static bool IsExpiredByMaxAge(
			ScavengePoint scavengePoint,
			MetastreamData streamData,
			long logPosition) {

			if (streamData.MaxAge == null)
				return false;

			//qq a couple of these methods calculate the chunk number, consider passing
			// the chunk number in directly
			var chunkNumber = (int)(logPosition / TFConsts.ChunkSize);

			// We can discard the event when it is as old or older than the cutoff
			var cutoff = scavengePoint.EffectiveNow - streamData.MaxAge.Value;

			// but we weaken the condition to say we only discard when the whole chunk is older than
			// the cutoff (which implies that the event certainly is)
			// the whole chunk is older than the cutoff only when the next chunk started before the
			// cutoff
			var nextChunkCreatedAt = GetChunkCreatedAt(chunkNumber + 1);
			//qq ^ consider if there might not be a next chunk
			// say we closed the last chunk (this one) exactly on a boundary and haven't created the next
			// one yet.

			// however, consider clock skew. we want to avoid the case where we accidentally discard a
			// record that we should have kept, because the chunk stamp said discard but the real record
			// stamp would have said keep.
			// for this to happen the records stamp would have to be newer than the chunk stamp.
			// add a maxSkew to the nextChunkCreatedAt to make it discard less.
			//qq make configurable
			var nextChunkCreatedAtIncludingSkew = nextChunkCreatedAt + TimeSpan.FromMinutes(1);
			var discard = nextChunkCreatedAtIncludingSkew <= cutoff;
			return discard;

			DateTime GetChunkCreatedAt(int chunkNum) {
				throw new NotImplementedException(); //qq
			}
		}

		// figure out which chunk it is for and note it down
		//qq chunk instructions are per logical chunk (for now)
		private void Discard(
			IScavengeStateForCalculator<TStreamId> state,
			long logPosition) {

			var chunkNumber = (int)(logPosition / TFConsts.ChunkSize);

			//qq dont go lookin it up every time, hold on to one set of chunkinstructions until we
			// have made it to the next chunk.
			if (!state.TryGetChunkWeight(chunkNumber, out var weight))
				weight = 0;
			state.SetChunkWeight(chunkNumber, weight++);
		}
	}
}

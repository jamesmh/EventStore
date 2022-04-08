﻿using System;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class TombstoneTests : ScavengerTestsBase {
		//qqq we will probably want a set of these to test out collisions when there are tombstones
		// and a set for collisions when there is metadata (maybe combination metadata and tombstone)
		[Fact]
		public void simple_tombstone() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Delete(1, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(1)
				});
		}

		[Fact]
		public void single_tombstone() {
			CreateScenario(x => x
				.Chunk(
					Rec.Delete(1, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(0)
				});
		}

		[Fact]
		public void tombstone_in_metadata_stream_not_supported() {
			// eventstore refuses denies access to write such a tombstone in the first place,
			// including in ESv5
			var e = Assert.Throws<InvalidOperationException>(() => {
				CreateScenario(x => x
					.Chunk(
						Rec.Delete(0, "$$ab-1"))
					.CompleteLastChunk())
					.Run();
			});

			Assert.Equal("Found Tombstone in metadata stream $$ab-1", e.Message);
		}
	}
}

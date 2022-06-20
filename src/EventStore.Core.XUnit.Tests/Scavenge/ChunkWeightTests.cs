﻿using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.XUnit.Tests.Scavenge.Sqlite;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ChunkWeightTests : SqliteDbPerTest<ChunkWeightTests> {
		[Fact]
		public async Task simple() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "ab-1"), // weight: 2
						Rec.Write(t++, "ab-1"), // weight: 2
						Rec.Write(t++, "ab-1"))
					.Chunk(
						ScavengePointRec(t++, threshold: 1000)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.AssertState(state => {
					Assert.Equal(4, state.SumChunkWeights(0, 0));
					Assert.Equal(0, state.SumChunkWeights(1, 1));
				})
				.RunAsync();
		}

		[Fact]
		public async Task max_age_maybe_discard() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1", timestamp: Expired), // weight: 1
						Rec.Write(t++, "ab-1", timestamp: Expired), // weight: 1
						Rec.Write(t++, "ab-1", timestamp: Active), // weight: 1
						Rec.Write(t++, "ab-1", timestamp: Active), // weight: 0 - last event
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxAgeMetadata))
					.Chunk(
						ScavengePointRec(t++, threshold: 1000)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.AssertState(state => {
					Assert.Equal(3, state.SumChunkWeights(0, 0));
					Assert.Equal(0, state.SumChunkWeights(1, 1));
					Assert.True(state.TryGetOriginalStreamData("ab-1", out var data));
					Assert.Equal(DiscardPoint.DiscardBefore(0), data.DiscardPoint);
					Assert.Equal(DiscardPoint.DiscardBefore(3), data.MaybeDiscardPoint);
				})
				.RunAsync();
		}

		[Fact]
		public async Task metadata_replaced_by_metadata() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // weight: 2
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						ScavengePointRec(t++, threshold: 1000)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.AssertState(state => {
					Assert.Equal(2, state.SumChunkWeights(0, 0));
					Assert.Equal(0, state.SumChunkWeights(1, 1));
				})
				.RunAsync();
		}

		[Fact]
		public async Task metadata_replaced_by_tombstone() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // weight: 2
					.Chunk(
						Rec.CommittedDelete(t++, "ab-1"),
						ScavengePointRec(t++, threshold: 1000)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.AssertState(state => {
					Assert.Equal(2, state.SumChunkWeights(0, 0));
					Assert.Equal(0, state.SumChunkWeights(1, 1));
				})
				.RunAsync();
		}

		[Fact]
		public async Task metadata_replaced_multiple_times() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // weight: 2
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1), // weight: 2
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // weight: 2
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1), // weight: 2
						Rec.CommittedDelete(t++, "ab-1"),
						ScavengePointRec(t++, threshold: 1000)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.AssertState(state => {
					Assert.Equal(2, state.SumChunkWeights(0, 0));
					Assert.Equal(4, state.SumChunkWeights(1, 1));
					Assert.Equal(2, state.SumChunkWeights(2, 2));
				})
				.RunAsync();
		}
	}
}

using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.XUnit.Tests.Scavenge.Sqlite;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	//qq split into suitable test classes
	public class MiscScavengerTests : SqliteDbPerTest<MiscScavengerTests> {
		//qq there is Rec.TransSt and TransEnd.. what do Writes and commits mean here without those?
		// probably applies to every test in here
		[Fact]
		public async Task Trivial() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						// the first letter of the stream name determines its hash value
						// a-1:       a stream called "a-1" which hashes to "a"
						Rec.Write(t++, "ab-1"),

						// setting metadata for a-1, which does not collide with a-1
						//qq um this isn't in a metadata stream so it probably wont be recognised as metadata
						// instead the stream should be called "ma1" which hashes to #a "$$ma1" which hashes
						// to #m and the hasher chooses which character depending on whether it is a metadta
						// stream.
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task seen_stream_before() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "ab-1"))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task collision() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "ab-2"))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadata_non_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadata_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "aa-1"),
						Rec.Write(t++, "$$aa-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadata_applies_to_correct_stream_in_hidden_collision() {
			// metastream sets metadata for stream ab-1 (which hashes to b)
			// but that stream doesn't exist.
			// make sure that cb-2 (which also hashes to b) does not pick up that metadata.
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "cb-2"),
						Rec.Write(t++, "cb-2"))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(0, 1, 2),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_non_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$cd-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$cd-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_all_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$aa-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$aa-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$aa-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$aa-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_original_streams_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$cb-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$cb-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_meta_streams_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$ac-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$ac-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_original_and_meta_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$ab-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$ab-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_different_streams_cross_colliding() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$ba-2", "$metadata", metadata: MaxCount2),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount3),
						Rec.Write(t++, "$$ba-2", "$metadata", metadata: MaxCount4))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(2, 3),
					x.Recs[1],
				});
		}

		[Fact]
		public async Task metadatas_for_same_stream() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount2))
					.Chunk(ScavengePointRec(t++)))
				.WithState(x => x.WithConnectionPool(Fixture.DbConnectionPool))
				.RunAsync(x => new[] {
					x.Recs[0].KeepIndexes(1),
					x.Recs[1],
				});
		}
	}
}

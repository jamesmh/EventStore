﻿using System;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class CalculationStatusTests {
		async Task RunAsync(
			CalculationStatus expected,
			StreamMetadata metadata,
			int numEvents,
			bool isTombstoned) {

			var t = 0;
			var (state, db) = await new Scenario()
				.WithDb(x => x
					.Chunk(Rec.Prepare(t++, "$$ab-1", "$metadata", metadata: metadata))
					.Chunk(Enumerable
						.Range(0, numEvents)
						.Select(_ => Rec.Prepare(t++, "ab-1"))
						.ToArray())
					.Chunk(isTombstoned
						? Rec.Delete(t++, "ab-1")
						: Rec.Prepare(t++, "cd-2"))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync();

			if (expected == CalculationStatus.Spent) {
				// it should be deleted
				Assert.False(state.TryGetOriginalStreamData("ab-1", out _));
			} else {
				// it should be present with the expected status
				Assert.True(state.TryGetOriginalStreamData("ab-1", out var data));
				Assert.Equal(expected, data.Status);
			}
		}

		[Fact]
		public async Task max_age() {
			await RunAsync(
				expected: CalculationStatus.Active,
				metadata: new StreamMetadata(maxAge: TimeSpan.FromMinutes(1)),
				numEvents: 1,
				isTombstoned: false);
		}

		[Fact]
		public async Task max_count() {
			await RunAsync(
				expected: CalculationStatus.Active,
				metadata: new StreamMetadata(maxCount: 1),
				numEvents: 1,
				isTombstoned: false);
		}

		[Fact]
		public async Task truncate_before_active() {
			await RunAsync(
				expected: CalculationStatus.Active,
				metadata: new StreamMetadata(truncateBefore: 5),
				numEvents: 5,
				isTombstoned: false);
		}

		[Fact]
		public async Task truncate_before_spent() {
			await RunAsync(
				expected: CalculationStatus.Spent,
				metadata: new StreamMetadata(truncateBefore: 5),
				numEvents: 6,
				isTombstoned: false);
		}

		[Fact]
		public async Task truncate_before_max() {
			await RunAsync(
				expected: CalculationStatus.Spent,
				metadata: SoftDelete,
				numEvents: 6,
				isTombstoned: false);
		}

		[Fact]
		public async Task tombstoned() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: new StreamMetadata(),
				numEvents: 1,
				isTombstoned: true);
		}

		[Fact]
		public async Task tombstoned_and_max_age() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: new StreamMetadata(maxAge: TimeSpan.FromMinutes(1)),
				numEvents: 1,
				isTombstoned: true);
		}

		[Fact]
		public async Task tombstoned_and_max_count() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: new StreamMetadata(maxCount: 1),
				numEvents: 1,
				isTombstoned: true);
		}

		[Fact]
		public async Task tombstoned_and_truncate_before_active() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: new StreamMetadata(truncateBefore: 5),
				numEvents: 5,
				isTombstoned: true);
		}

		[Fact]
		public async Task tombstoned_and_truncate_before_spent() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: new StreamMetadata(truncateBefore: 5),
				numEvents: 6,
				isTombstoned: true);
		}

		[Fact]
		public async Task tombstoned_and_truncate_before_max() {
			await RunAsync(
				expected: CalculationStatus.Archived,
				metadata: SoftDelete,
				numEvents: 6,
				isTombstoned: true);
		}

		[Fact]
		public async Task blank() {
			await RunAsync(
				expected: CalculationStatus.Spent,
				metadata: new StreamMetadata(),
				numEvents: 1,
				isTombstoned: false);
		}
	}
}

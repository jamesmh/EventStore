﻿namespace EventStore.Core.DataStructures.ProbabilisticFilter {
	public static class RoundingExtensions {
		public static long RoundDownToMultipleOf(this long x, int multiple) {
			return (x / multiple) * multiple;
		}

		public static long RoundUpToMultipleOf(this long x, int multiple) {
			var ret = x.RoundDownToMultipleOf(multiple);
			if (ret != x) {
				ret += multiple;
			}
			return ret;
		}
	}
}

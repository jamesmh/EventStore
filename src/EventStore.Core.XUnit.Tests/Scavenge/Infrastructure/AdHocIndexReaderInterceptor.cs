using System;
using EventStore.Core.Data;
using EventStore.Core.Services.Storage.ReaderIndex;
using EventStore.Core.TransactionLog.Scavenging;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class AdHocIndexReaderInterceptor<TStreamId> : IIndexReaderForCalculator<TStreamId> {
		private readonly IIndexReaderForCalculator<TStreamId> _wrapped;
		private readonly Func<
			Func<StreamHandle<TStreamId>, ScavengePoint, long>,
			StreamHandle<TStreamId>, ScavengePoint, long> _f;


		public AdHocIndexReaderInterceptor(
			IIndexReaderForCalculator<TStreamId> wrapped,
			Func<
				Func<StreamHandle<TStreamId>, ScavengePoint, long>,
				StreamHandle<TStreamId>, ScavengePoint, long> f) {

			_wrapped = wrapped;
			_f = f;
		}

		public long GetLastEventNumber(
			StreamHandle<TStreamId> streamHandle,
			ScavengePoint scavengePoint) {

			return _f(_wrapped.GetLastEventNumber, streamHandle, scavengePoint);
		}

		public IndexReadEventInfoResult ReadEventInfoForward(
			StreamHandle<TStreamId> stream,
			long fromEventNumber,
			int maxCount,
			ScavengePoint scavengePoint) {

			return _wrapped.ReadEventInfoForward(stream, fromEventNumber, maxCount, scavengePoint);
		}
	}
}

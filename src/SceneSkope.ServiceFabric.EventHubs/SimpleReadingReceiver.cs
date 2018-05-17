using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class SimpleReadingReceiver : IReadingReceiver
    {
        public ILogger Log { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;

        public int MaxBatchSize { get; set; } = 100;

        public PartitionReceiver Receiver { get; }

        public CancellationTokenSource TokenSource { get; }

        protected SimpleReadingReceiver(ILogger log, PartitionReceiver receiver, IReliableDictionary<string, string> offsets, string partition, CancellationToken ct)
        {
            Log = log.ForContext("partition", partition);
            Receiver = receiver;
            _offsets = offsets;
            _partition = partition;
            TokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading: {Exception}", error.Message);
            switch (error)
            {
                case ReceiverDisconnectedException _:
                case OperationCanceledException _:
                case EventHubsException _:
                    Log.Information("Cancelling the receiver");
                    TokenSource.Cancel();
                    break;
            }
            return Task.CompletedTask;
        }

        protected abstract Task ProcessEventAsync(EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            string latestOffset = null;
            foreach (var @event in events)
            {
                await ProcessEventAsync(@event).ConfigureAwait(false);
                latestOffset = @event.SystemProperties.Offset;
            }
            await OnAllEventsProcessedAsync(latestOffset).ConfigureAwait(false);
        }

        protected virtual Task OnAllEventsProcessedAsync(string latestOffset) => Task.CompletedTask;

        protected Task SaveOffsetAsync(ITransaction tx, string latestOffset) => _offsets.SetAsync(tx, _partition, latestOffset);

        public Task WaitForFinishedAsync(CancellationToken ct) => Task.Delay(Timeout.Infinite, TokenSource.Token);
    }
}

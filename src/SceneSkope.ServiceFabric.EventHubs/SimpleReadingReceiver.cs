using System;
using System.Collections.Generic;
using System.Linq;
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

        public virtual int MaxBatchSize => 100;

        public CancellationToken CancellationToken { get; }

        protected SimpleReadingReceiver(ILogger log, IReliableDictionary<string, string> offsets, string partition, CancellationToken ct)
        {
            Log = log.ForContext("partition", partition);
            _offsets = offsets;
            _partition = partition;
            CancellationToken = ct;
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading: {Exception}", error.Message);
            return Task.CompletedTask;
        }

        protected abstract Task ProcessEventAsync(EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            var count = Log.IsEnabled(Serilog.Events.LogEventLevel.Verbose) ? events.Count() : 0;
            Log.Verbose("Got {Count} events to process", count);
            string latestOffset = null;
            foreach (var @event in events)
            {
                await ProcessEventAsync(@event).ConfigureAwait(false);
                latestOffset = @event.SystemProperties.Offset;
            }
            await OnAllEventsProcessedAsync(latestOffset).ConfigureAwait(false);
            Log.Verbose("Processed {Count} events", count);
        }

        protected virtual Task OnAllEventsProcessedAsync(string latestOffset) => Task.CompletedTask;

        protected Task SaveOffsetAsync(ITransaction tx, string latestOffset) => _offsets.SetAsync(tx, _partition, latestOffset);
    }
}

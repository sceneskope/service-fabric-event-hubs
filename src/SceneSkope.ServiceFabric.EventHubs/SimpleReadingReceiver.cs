using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class SimpleReadingReceiver : IReadingReceiver
    {
        public ILogger Log { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;

        public virtual int MaxBatchSize => 100;

        protected SimpleReadingReceiver(ILogger log, IReliableDictionary<string, string> offsets, string partition)
        {
            Log = log.ForContext("partition", partition);
            _offsets = offsets;
            _partition = partition;
        }

        public virtual Task InitialiseAsync(CancellationToken ct) => Task.FromResult(true);

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading: {exception}", error.Message);
            return Task.FromResult(true);
        }

        protected abstract Task ProcessEventAsync(EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            var count = Log.IsEnabled(Serilog.Events.LogEventLevel.Verbose) ? events.Count() : 0;
            Log.Verbose("Got {count} events to process", count);
            foreach (var @event in events)
            {
                await ProcessEventAsync(@event).ConfigureAwait(false);
            }
            await OnAllEventsProcessedAsync().ConfigureAwait(false);
            Log.Verbose("Processed {count} events", count);
        }

        protected virtual Task OnAllEventsProcessedAsync() => Task.FromResult(true);

        protected Task SaveOffsetAsync(ITransaction tx, string latestOffset) => _offsets.SetAsync(tx, _partition, latestOffset);
    }
}

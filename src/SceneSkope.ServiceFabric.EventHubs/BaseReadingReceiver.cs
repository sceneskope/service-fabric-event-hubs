using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Polly;
using Serilog;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseReadingReceiver : IReadingReceiver
    {
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;
        protected readonly CancellationToken _ct;

        public virtual int MaxBatchSize => 100;
        protected Policy TimeoutPolicy { get; }

        protected BaseReadingReceiver(ILogger log, IReliableStateManager stateManager,
        IReliableDictionary<string, string> offsets, string partition,
        CancellationToken ct)
        {
            Log = log.ForContext("partition", partition);
            StateManager = stateManager;
            _offsets = offsets;
            _partition = partition;
            _ct = ct;
            TimeoutPolicy =
                Policy
                .Handle<TimeoutException>(_ => !_ct.IsCancellationRequested)
                .WaitAndRetryForeverAsync(n => TimeSpan.FromMilliseconds((n < 10) ? n * 100 : 1000),
                    (ex, ts) => Log.Warning(ex, "Delaying {Ts} due to {Exception}", ts, ex.Message));
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading: {Exception}", error.Message);
            return Task.CompletedTask;
        }

        protected abstract Task ProcessEventAsync(ITransaction tx, EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            string lastOffset = null;
            await TimeoutPolicy.ExecuteAsync(async _ =>
            {
                var count = events.Count();
                Log.Verbose("Got {Count} events to process", count);
                using (var tx = StateManager.CreateTransaction())
                {
                    foreach (var @event in events)
                    {
                        await ProcessEventAsync(tx, @event).ConfigureAwait(false);
                        lastOffset = @event.SystemProperties.Offset;
                    }
                    await _offsets.SetAsync(tx, _partition, lastOffset).ConfigureAwait(false);
                    await tx.CommitAsync().ConfigureAwait(false);
                }
                Log.Verbose("Processed {Count} events", count);
            }, _ct, false).ConfigureAwait(false);
        }
    }
}

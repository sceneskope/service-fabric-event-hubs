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
        protected CancellationToken CancellationToken { get; }

        public int MaxBatchSize { get; set; } = 100;
        protected Policy TimeoutPolicy { get; }

        protected BaseReadingReceiver(ILogger log, IReliableStateManager stateManager,
        IReliableDictionary<string, string> offsets, string partition,
        CancellationToken ct)
        {
            Log = log.ForContext("partition", partition);
            StateManager = stateManager;
            _offsets = offsets;
            _partition = partition;
            CancellationToken = ct;
            TimeoutPolicy =
                Policy
#pragma warning disable RCS1163 // Unused parameter.
                .Handle<TimeoutException>(_ => !CancellationToken.IsCancellationRequested)
#pragma warning restore RCS1163 // Unused parameter.
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
#pragma warning disable RCS1163 // Unused parameter.
            await TimeoutPolicy.ExecuteAsync(async _ =>
#pragma warning restore RCS1163 // Unused parameter.
            {
                var count = events.Count();
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
            }, CancellationToken, false).ConfigureAwait(false);
        }
    }
}

using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Polly;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseReadingReceiver : IPartitionReceiveHandler
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
                    (ex, ts) => Log.Warning("Delaying {ts} due to {exception}", ts, ex.Message));
        }

        public virtual Task InitialiseAsync() => Task.FromResult(true);

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading {partition}: {exception}", _partition, error.Message);
            return Task.FromResult(true);
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
                using (var tx = StateManager.CreateTransaction())
                {
                    foreach (var @event in events)
                    {
                        try
                        {
                            await ProcessEventAsync(tx, @event).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (!(ex is FabricException))
                        {
                            _ct.ThrowIfCancellationRequested();
                            Log.Warning(ex, "Error processing: {exception}", ex.Message);
                        }
                        lastOffset = @event.SystemProperties.Offset;
                    }
                    await _offsets.SetAsync(tx, _partition, lastOffset).ConfigureAwait(false);
                    await tx.CommitAsync().ConfigureAwait(false);
                }
            }, _ct, false).ConfigureAwait(false);
        }
    }
}

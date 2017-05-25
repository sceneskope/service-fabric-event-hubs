using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseReadingReceiver : IPartitionReceiveHandler
    {
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }
        public bool AutomaticallySave { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;
        protected readonly CancellationTokenSource _cts;

        public virtual int MaxBatchSize => 100;

        protected BaseReadingReceiver(ILogger log, IReliableStateManager stateManager,
        IReliableDictionary<string, string> offsets, string partition,
        bool automaticallySave,
        CancellationTokenSource cts)
        {
            Log = log.ForContext("partition", partition);
            AutomaticallySave = automaticallySave;
            StateManager = stateManager;
            _offsets = offsets;
            _partition = partition;
            _cts = cts;
        }

        public virtual Task InitialiseAsync() => Task.FromResult(true);

        public virtual Task ProcessErrorAsync(Exception error)
        {
            if (!DontLogException(error))
            {
                Log.Error(error, "Error reading {partition}: {exception}", _partition, error.Message);
            }
            _cts.Cancel();
            return Task.FromResult(true);
        }

        private static bool DontLogException(Exception ex) => (ex is FabricException) || (ex is OperationCanceledException);

        protected abstract Task ProcessEventAsync(EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            string lastOffset = null;

            foreach (var @event in events)
            {
                try
                {
                    await ProcessEventAsync(@event).ConfigureAwait(false);
                }
                catch (Exception ex) when (!DontLogException(ex))
                {
                    Log.Warning(ex, "Error processing: {exception}", ex.Message);
                }
                lastOffset = @event.SystemProperties.Offset;
            }

            if (lastOffset != null)
            {
                if (AutomaticallySave)
                {
                    using (var tx = StateManager.CreateTransaction())
                    {
                        await SaveOffsetAsync(tx, lastOffset).ConfigureAwait(false);
                        await tx.CommitAsync().ConfigureAwait(false);
                    }
                }
            }
        }

        protected Task SaveOffsetAsync(ITransaction tx, string offset) => _offsets.SetAsync(tx, _partition, offset);
    }
}

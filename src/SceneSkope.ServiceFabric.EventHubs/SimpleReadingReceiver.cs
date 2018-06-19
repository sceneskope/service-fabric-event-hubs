using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using ServiceFabric.Utilities;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class SimpleReadingReceiver : IReadingReceiver
    {
        public Func<Exception, bool> TransientExceptionChecker { get; }
        public ServiceFabricRetryHandler RetryHandler { get; }
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;

        public PartitionReceiver Receiver { get; }

        protected SimpleReadingReceiver(ILogger log,
            IReliableStateManager stateManager,
            PartitionReceiver receiver,
            IReliableDictionary<string, string> offsets, string partition,
            ServiceFabricRetryHandler retryHandler,
            Func<Exception, bool> transientExceptionChecker = null)
        {
            RetryHandler = retryHandler;
            Log = log;
            StateManager = stateManager;
            Receiver = receiver;
            _offsets = offsets;
            _partition = partition;
            TransientExceptionChecker = transientExceptionChecker;
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        protected abstract Task ProcessEventAsync(EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
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
    }
}

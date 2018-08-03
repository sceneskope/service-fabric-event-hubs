using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using ServiceFabric.Utilities;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseReadingReceiver : IReadingReceiver
    {
        public Func<Exception, bool> TransientExceptionChecker { get; }
        public ServiceFabricRetryHandler RetryHandler { get; }
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }

        protected readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;

        public PartitionReceiver Receiver { get; }

        protected BaseReadingReceiver(ILogger log,
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

        protected Task SaveOffsetAsync(ITransaction tx, string latestOffset) => _offsets.SetAsync(tx, _partition, latestOffset);

        public virtual void Dispose()
        {
        }

        public Task ProcessEventsAsync(IEnumerable<EventData> events) => ProcessEventsAsync((IReadOnlyList<EventData>)events);

        protected abstract Task ProcessEventsAsync(IReadOnlyList<EventData> events);
    }
}

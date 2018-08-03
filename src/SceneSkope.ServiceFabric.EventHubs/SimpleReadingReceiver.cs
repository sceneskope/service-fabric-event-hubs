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
    public abstract class SimpleReadingReceiver : BaseReadingReceiver
    {

        protected SimpleReadingReceiver(ILogger log,
            IReliableStateManager stateManager,
            PartitionReceiver receiver,
            IReliableDictionary<string, string> offsets, 
            string partition,
            ServiceFabricRetryHandler retryHandler,
            Func<Exception, bool> transientExceptionChecker = null) :
                base(log, stateManager, receiver, offsets, partition, retryHandler, transientExceptionChecker)
        {
        }
        protected abstract Task ProcessEventAsync(EventData @event);

        protected override async Task ProcessEventsAsync(IReadOnlyList<EventData> events)
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
    }
}

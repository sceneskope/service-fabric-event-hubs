using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public interface IReadingReceiver
    {
        ServiceFabricRetryHandler RetryHandler { get; }
        Func<Exception, bool> TransientExceptionChecker { get; }

        Task InitialiseAsync();
        Task ProcessEventsAsync(IEnumerable<EventData> events);
    }
}

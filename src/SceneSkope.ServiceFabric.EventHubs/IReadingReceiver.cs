using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public interface IReadingReceiver : IDisposable
    {
        Task ProcessEventsAsync(IEnumerable<EventData> events);
    }
}

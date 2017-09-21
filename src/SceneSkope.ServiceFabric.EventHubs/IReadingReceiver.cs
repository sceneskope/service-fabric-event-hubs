using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public interface IReadingReceiver : IPartitionReceiveHandler
    {
        Task InitialiseAsync(CancellationToken ct);
        CancellationToken ErrorToken { get; }
    }
}

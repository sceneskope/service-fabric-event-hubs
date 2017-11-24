using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public interface IReadingReceiver : IPartitionReceiveHandler
    {
        Task InitialiseAsync();
    }
}

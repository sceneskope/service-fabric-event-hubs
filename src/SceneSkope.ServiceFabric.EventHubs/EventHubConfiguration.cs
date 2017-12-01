using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using ServiceFabric.Utilities;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public static class EventHubConfiguration
    {
        public static async Task<EventHubClient> GetEventHubClientAsync(string sectionName, Action<string> onFailure, CancellationToken ct)
        {
            var configuration = new FabricConfigurationProvider(sectionName);
            if (!configuration.HasConfiguration)
            {
                await configuration.RejectConfigurationAsync($"No {sectionName} section", onFailure, ct).ConfigureAwait(false);
            }
            var inputConnectionString = new EventHubsConnectionStringBuilder(
                new Uri(await configuration.TryReadConfigurationAsync("EndpointAddress", onFailure, ct).ConfigureAwait(false)),
                await configuration.TryReadConfigurationAsync("EntityPath", onFailure, ct).ConfigureAwait(false),
                await configuration.TryReadConfigurationAsync("SharedAccessKeyName", onFailure, ct).ConfigureAwait(false),
                await configuration.TryReadConfigurationAsync("SharedAccessKey", onFailure, ct).ConfigureAwait(false)
            );
            return EventHubClient.CreateFromConnectionString(inputConnectionString.ToString());
        }
    }
}

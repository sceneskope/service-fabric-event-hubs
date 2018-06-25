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
            var inputConnectionString = await GetEventHubConnectionString(sectionName, onFailure, ct).ConfigureAwait(false);
            return EventHubClient.CreateFromConnectionString(inputConnectionString);
        }

        public static async Task<string> GetEventHubConnectionString(string sectionName, Action<string> onFailure, CancellationToken ct)
        {
            return (await GetEventHubConnectionStringBuilder(sectionName, onFailure, ct).ConfigureAwait(false)).ToString();
        }

        public static async Task<EventHubsConnectionStringBuilder> GetEventHubConnectionStringBuilder(string sectionName, Action<string> onFailure, CancellationToken ct)
        {
            var configuration = new FabricConfigurationProvider(sectionName);
            if (!configuration.HasConfiguration)
            {
                await configuration.RejectConfigurationAsync($"No {sectionName} section", onFailure, ct).ConfigureAwait(false);
                return null;
            }
            try
            {
                var endpointAddress = await configuration.TryReadConfigurationAsync("EndpointAddress", onFailure, ct).ConfigureAwait(false);
                var builder = new EventHubsConnectionStringBuilder(
                    new Uri($"amqps://{endpointAddress}"),
                    await configuration.TryReadConfigurationAsync("EntityPath", onFailure, ct).ConfigureAwait(false),
                    await configuration.TryReadConfigurationAsync("SharedAccessKeyName", onFailure, ct).ConfigureAwait(false),
                    await configuration.TryReadConfigurationAsync("SharedAccessKey", onFailure, ct).ConfigureAwait(false)
                );
                return builder;
            }
            catch (Exception ex)
            {
                ct.ThrowIfCancellationRequested();
                await configuration.RejectConfigurationAsync($"Exception creating connection string builder: {ex.Message}", onFailure, ct).ConfigureAwait(false);
                return null;
            }
        }
    }
}

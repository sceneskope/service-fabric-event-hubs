using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using ServiceFabric.Serilog;
using ServiceFabric.Utilities;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseEventHubService : SerilogStatefulService
    {
        private const string OffsetsName = "offsets";

        public class Configuration
        {
            public EventHubClient Client { get; }
            public string ConsumerGroup { get; }
            public string[] Partitions { get; }
            public IReliableDictionary<string, string> Offsets { get; }

            public Configuration(EventHubClient client, string consumerGroup, string[] partitions, IReliableDictionary<string, string> offsets)
            {
                Client = client;
                ConsumerGroup = consumerGroup;
                Partitions = partitions;
                Offsets = offsets;
            }
        }

        public string ConfigurationSectionName { get; set; } = "EventHubSource";

        public int MaxEntries { get; set; } = 100;

        protected EventPosition DefaultPosition { get; set; } = EventPosition.FromEnd();
        protected TimeSpan? DefaultAge { get; set; }
        protected bool UseEpochReceiver { get; set; } = false;

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger)
            : base(context, logger)
        {
        }

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger, IReliableStateManagerReplica2 stateManager)
            : base(context, stateManager, logger)
        {
        }

        private async Task<string[]> GetPartitionsAsync(EventHubClient client)
        {
            var info = await client.GetRuntimeInformationAsync().ConfigureAwait(false);
            var partitions = await PartitionUtilities.GetOrderedPartitionListAsync(Context.ServiceName).ConfigureAwait(false);
            var thisPartitionIndex = partitions.FindIndex(spi => spi.Id == Partition.PartitionInfo.Id);
            var partitionsPerIndex = info.PartitionCount / partitions.Count;
            var firstPartition = thisPartitionIndex * partitionsPerIndex;
            var possibleLastPartition = (firstPartition + partitionsPerIndex) - 1;
            var lastPartition = possibleLastPartition > info.PartitionCount ? info.PartitionCount - 1 : possibleLastPartition;
            var ourPartitions = new string[(lastPartition - firstPartition) + 1];
            Array.Copy(info.PartitionIds, firstPartition, ourPartitions, 0, ourPartitions.Length);
            return ourPartitions;
        }

        protected virtual async Task<Configuration> TryConfigureAsync(CancellationToken ct)
        {
            try
            {
                void onFailure(string msg) => Log.Error("Error configuring: {Msg}", msg);
                var client = await EventHubConfiguration.GetEventHubClientAsync(ConfigurationSectionName, onFailure, ct).ConfigureAwait(false);

                var configuration = new FabricConfigurationProvider(ConfigurationSectionName);
                var consumerGroup = await configuration.TryReadConfigurationAsync("ConsumerGroup", onFailure, ct).ConfigureAwait(false);

                var partitions = await GetPartitionsAsync(client).ConfigureAwait(false);
                var offsets = await StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetsName).ConfigureAwait(false);
                return new Configuration(client, consumerGroup, partitions, offsets);
            }
            catch (Exception ex)
            {
                ct.ThrowIfCancellationRequested();
                Log.Fatal(ex, "Error configuring event hubs: {Exception}", ex.Message);
                throw;
            }
        }

        protected sealed override async Task RunAsync(CancellationToken cancellationToken)
        {
            var configuration = await TryConfigureAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                var retryHandler = new ServiceFabricRetryHandler(cancellationToken);
                await InternalRunAsync(configuration, retryHandler).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Log.Warning(ex, "Service failed but is not cancelled: {Exception}", ex.Message);
                await Task.Delay(5000, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task InternalRunAsync(Configuration configuration, ServiceFabricRetryHandler retryHandler)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (retryHandler.ServiceCancellationToken.Register(() =>
            {
                Log.Information("Service cancellation requested");
                tcs.SetResult(true);
            }))
            {
                var tasks = new Task[configuration.Partitions.Length + 1];
                for (var i = 0; i < configuration.Partitions.Length; i++)
                {
                    tasks[i] = ProcessPartitionAsync(configuration.Partitions[i], configuration, retryHandler);
                }
                tasks[tasks.Length - 1] = WaitForCancellationAndClose(tcs.Task, configuration.Client);
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
        }

        private async Task WaitForCancellationAndClose(Task waitingTask, EventHubClient client)
        {
            await waitingTask;
            Log.Information("About to close client");
            await client.CloseAsync().ConfigureAwait(false);
            Log.Information("Closed client");
        }

        private async Task<string> ReadOffsetAsync(string partition, IReliableDictionary<string, string> offsets)
        {
            using (var tx = StateManager.CreateTransaction())
            {
                var result = await offsets.TryGetValueAsync(tx, partition).ConfigureAwait(false);
                if (result.HasValue)
                {
                    return result.Value;
                }
                else
                {
                    return null;
                }
            }
        }

        private PartitionReceiver CreateReceiver(ILogger log, Configuration configuration, string partition, string offset)
        {
            var epoch = DateTime.UtcNow.Ticks;
            if (string.IsNullOrWhiteSpace(offset))
            {
                if (DefaultAge.HasValue)
                {
                    var timestamp = DateTime.UtcNow.Subtract(DefaultAge.Value);
                    var position = EventPosition.FromEnqueuedTime(timestamp);
                    if (UseEpochReceiver)
                    {
                        log.Debug("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from time {Timestamp}",
                            epoch, configuration.ConsumerGroup, partition, timestamp);
                        return configuration.Client.CreateEpochReceiver(configuration.ConsumerGroup, partition, position, epoch);
                    }
                    else
                    {
                        log.Debug("Creating receiver for {Consumer}#{Partition} from time {Timestamp}",
                            configuration.ConsumerGroup, partition, timestamp);
                        return configuration.Client.CreateReceiver(configuration.ConsumerGroup, partition, position);
                    }
                }
                else
                {
                    var position = DefaultPosition;
                    if (UseEpochReceiver)
                    {
                        log.Debug("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from position {Position}",
                            epoch, configuration.ConsumerGroup, partition, position);
                        return configuration.Client.CreateEpochReceiver(configuration.ConsumerGroup, partition, position, epoch);
                    }
                    else
                    {
                        log.Debug("Creating receiver for {Consumer}#{Partition} from offset {Position}",
                            configuration.ConsumerGroup, partition, position);
                        return configuration.Client.CreateReceiver(configuration.ConsumerGroup, partition, position);
                    }
                }
            }
            else
            {
                var position = EventPosition.FromOffset(offset);
                if (UseEpochReceiver)
                {
                    log.Debug("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from saved offset {Offset}",
                        epoch, configuration.ConsumerGroup, partition, offset);
                    return configuration.Client.CreateEpochReceiver(configuration.ConsumerGroup, partition, position, epoch);
                }
                else
                {
                    log.Debug("Creating receiver for {Consumer}#{Partition} from saved offset {Offset}",
                        configuration.ConsumerGroup, partition, offset);
                    return configuration.Client.CreateReceiver(configuration.ConsumerGroup, partition, position);
                }
            }
        }

        private async Task ProcessPartitionAsync(string partition, Configuration configuration, ServiceFabricRetryHandler retryHandler)
        {
            var log = Log.ForContext("partition", partition);
            while (!retryHandler.IsCancellationRequested)
            {
                var offset = await retryHandler.CallAsync(_ => ReadOffsetAsync(partition, configuration.Offsets)).ConfigureAwait(false);
                Log.Information("Processing partition {Partition} from offset {Offset}", partition, offset);
                var receiver = CreateReceiver(log, configuration, partition, offset);
                try
                {
                    using (var handler = await CreateReadingReceiverAsync(log, StateManager, receiver, configuration.Offsets, partition, retryHandler).ConfigureAwait(false))
                    {
                        while (!retryHandler.IsCancellationRequested)
                        {
                            var messages = await receiver.ReceiveAsync(MaxEntries).ConfigureAwait(false);
                            if (messages != null)
                            {
                                await handler.ProcessEventsAsync(messages).ConfigureAwait(false);
                            }
                        }
                    }
                    retryHandler.ThrowIfCancellationRequested();
                }
                catch (Exception ex)
                {
                    retryHandler.ThrowIfCancellationRequested();
                    log.Error(ex, "Error processing partition {Partition}: {Exception}", partition, ex.Message);
                }
                finally
                {
                    log.Debug("Finished processing partition for {Partition}", partition);
                    await receiver.CloseAsync().ConfigureAwait(false);
                }
                await retryHandler.RandomDelay().ConfigureAwait(false);
            }
        }

        protected abstract Task<IReadingReceiver> CreateReadingReceiverAsync(ILogger log, IReliableStateManager stateManager,
            PartitionReceiver receiver, IReliableDictionary<string, string> offsets, string partition, ServiceFabricRetryHandler retryHandler);
    }
}

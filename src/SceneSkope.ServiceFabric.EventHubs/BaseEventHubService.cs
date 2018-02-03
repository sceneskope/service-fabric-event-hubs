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
        private const string PartitionsName = "partitions";
        private const string OffsetsName = "offsets";
        private EventHubClient _client;
        private string _consumerGroup;

        public string ConfigurationSectionName { get; set; } = "EventHubSource";

        protected string DefaultPosition { get; set; } = PartitionReceiver.EndOfStream;
        protected TimeSpan? DefaultAge { get; set; }
        protected bool UseEpochReceiver { get; set; } = true;

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger)
            : base(context, logger)
        {
        }

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger, IReliableStateManagerReplica2 stateManager)
            : base(context, stateManager, logger)
        {
        }

        private async Task<string[]> GetOrCreatePartitionListAsync(CancellationToken ct)
        {
            var partitionsDictionary = await StateManager.GetOrAddAsync<IReliableDictionary<string, string[]>>(PartitionsName).ConfigureAwait(false);
            string[] partitions;
            using (var tx = StateManager.CreateTransaction())
            {
                var result = await partitionsDictionary.TryGetValueAsync(tx, PartitionsName).ConfigureAwait(false);
                if (!result.HasValue)
                {
                    partitions = await GetPartitionsAsync(ct).ConfigureAwait(false);
                    await partitionsDictionary.SetAsync(tx, PartitionsName, partitions).ConfigureAwait(false);
                }
                else
                {
                    partitions = result.Value;
                }
                await tx.CommitAsync().ConfigureAwait(false);
            }
            return partitions;
        }

#pragma warning disable RCS1163 // Unused parameter.
        private async Task<string[]> GetPartitionsAsync(CancellationToken ct)
#pragma warning restore RCS1163 // Unused parameter.
        {
            var info = await _client.GetRuntimeInformationAsync().ConfigureAwait(false);
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

        protected virtual async Task TryConfigureAsync(CancellationToken ct)
        {
            void onFailure(string msg) => Log.Error("Error configuring: {Msg}", msg);
            _client = await EventHubConfiguration.GetEventHubClientAsync(ConfigurationSectionName, onFailure, ct).ConfigureAwait(false);

            var configuration = new FabricConfigurationProvider(ConfigurationSectionName);
            _consumerGroup = await configuration.TryReadConfigurationAsync("ConsumerGroup", onFailure, ct).ConfigureAwait(false);
        }

        protected sealed override async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                await TryConfigureAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Error configuring event hubs: {Exception}", ex.Message);
                throw;
            }

            await ControlledRunner.RunAsync(Log, InternalRunAsync, cancellationToken).ConfigureAwait(false);
        }

        private async Task InternalRunAsync(CancellationToken cancellationToken)
        {
            var partitions = await GetOrCreatePartitionListAsync(cancellationToken).ConfigureAwait(false);
            using (cancellationToken.Register(() => Log.Debug("Service cancellation requested")))
            {
                try
                {
                    var offsets = await StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetsName).ConfigureAwait(false);
                    var tasks = new Task[partitions.Length];
                    for (var i = 0; i < partitions.Length; i++)
                    {
                        tasks[i] = ProcessPartitionAsync(partitions[i], offsets, cancellationToken);
                    }
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    Log.Error(ex, "Event hub service exiting due to {Exception}", ex.Message);
                    throw;
                }
            }
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

        private PartitionReceiver CreateReceiver(string partition, string offset)
        {
            var epoch = DateTime.UtcNow.Ticks;
            if (string.IsNullOrWhiteSpace(offset))
            {
                if (DefaultAge.HasValue)
                {
                    var timestamp = DateTime.UtcNow.Subtract(DefaultAge.Value);
                    if (UseEpochReceiver)
                    {
                        Log.Information("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from time {Timestamp}",
                            epoch, _consumerGroup, partition, timestamp);
                        return _client.CreateEpochReceiver(_consumerGroup, partition, timestamp, epoch);
                    }
                    else
                    {
                        Log.Information("Creating receiver for {Consumer}#{Partition} from time {Timestamp}",
                            _consumerGroup, partition, timestamp);
                        return _client.CreateReceiver(_consumerGroup, partition, timestamp);
                    }
                }
                else
                {
                    var offsetToUse = string.IsNullOrWhiteSpace(offset) ? DefaultPosition : offset;
                    var offsetInclusive = offset == PartitionReceiver.StartOfStream;
                    if (UseEpochReceiver)
                    {
                        Log.Information("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from offset {Offset} {Inclusive}",
                            epoch, _consumerGroup, partition, offsetToUse, offsetInclusive);
                        return _client.CreateEpochReceiver(_consumerGroup, partition, offsetToUse, offsetInclusive, DateTime.UtcNow.Ticks);
                    }
                    else
                    {
                        Log.Information("Creating receiver for {Consumer}#{Partition} from offset {Offset} {Inclusive}",
                            _consumerGroup, partition, offsetToUse, offsetInclusive);
                        return _client.CreateReceiver(_consumerGroup, partition, offsetToUse, offsetInclusive);
                    }
                }
            }
            else
            {
                if (UseEpochReceiver)
                {
                    Log.Information("Creating epoch {Epoch} receiver for {Consumer}#{Partition} from saved offset {Offset}",
                        epoch, _consumerGroup, partition, offset);
                    return _client.CreateEpochReceiver(_consumerGroup, partition, offset, false, DateTime.UtcNow.Ticks);
                }
                else
                {
                    Log.Information("Creating receiver for {Consumer}#{Partition} from saved offset {Offset}",
                        _consumerGroup, partition, offset);
                    return _client.CreateReceiver(_consumerGroup, partition, offset, false);
                }
            }
        }

        private async Task ProcessPartitionAsync(string partition, IReliableDictionary<string, string> offsets, CancellationToken ct)
        {
            var offset = await ReadOffsetAsync(partition, offsets).ConfigureAwait(false);
            var receiver = CreateReceiver(partition, offset);
            try
            {
                var handler = CreateReadingReceiver(Log, StateManager, offsets, partition, ct);
                await handler.InitialiseAsync().ConfigureAwait(false);
                receiver.SetReceiveHandler(handler);
                receiver.RetryPolicy = RetryPolicy.NoRetry;
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ct.ThrowIfCancellationRequested();
                Log.Error(ex, "Error processing partition: {Exception}", ex.Message);
            }
            finally
            {
                Log.Information("Finished processing partition iteration {Partition}", partition);
                await receiver.CloseAsync().ConfigureAwait(false);
            }
        }

        protected abstract IReadingReceiver CreateReadingReceiver(ILogger log, IReliableStateManager stateManager,
            IReliableDictionary<string, string> offsets, string partition, CancellationToken ct);
    }
}

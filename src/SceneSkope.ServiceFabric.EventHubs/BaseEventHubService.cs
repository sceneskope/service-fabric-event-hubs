using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using ServiceFabric.Serilog;
using ServiceFabric.Utilities;
using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseEventHubService : SerilogStatefulService
    {
        private const string PartitionsName = "partitions";
        private const string OffsetsName = "offsets";

        protected string DefaultPosition { get; set; } = PartitionReceiver.EndOfStream;

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger)
            : base(context, logger)
        {
        }

        protected BaseEventHubService(StatefulServiceContext context, ILogger logger, IReliableStateManagerReplica stateManager)
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

        protected async Task<string> RejectConfigurationAsync(string reason, CancellationToken ct)
        {
            Log.Error("Invalid configuration: {reason}", reason);
            await Task.Delay(5000, ct).ConfigureAwait(false);
            throw new InvalidOperationException(reason);
        }

        protected Task<string> TryReadConfigurationAsync(FabricConfigurationProvider config, string name, CancellationToken ct)
        {
            var value = config.TryGetValue(name);
            if (string.IsNullOrWhiteSpace(value))
            {
                return RejectConfigurationAsync($"No configuration for {name}", ct);
            }
            else
            {
                return Task.FromResult(value);
            }
        }

        protected virtual async Task TryConfigureAsync(CancellationToken ct)
        {
            var config = new FabricConfigurationProvider("EventHubSource");
            if (!config.HasConfiguration)
            {
                await RejectConfigurationAsync("No event hub source configuration", ct).ConfigureAwait(false);
            }
            var host = await TryReadConfigurationAsync(config, "Host", ct).ConfigureAwait(false);
            var policy = await TryReadConfigurationAsync(config, "Policy", ct).ConfigureAwait(false);
            var key = await TryReadConfigurationAsync(config, "Key", ct).ConfigureAwait(false);
            var eventHub = await TryReadConfigurationAsync(config, "EventHub", ct).ConfigureAwait(false);
            _consumerGroup = await TryReadConfigurationAsync(config, "ConsumerGroup", ct).ConfigureAwait(false);
            var builder = new EventHubsConnectionStringBuilder(new Uri($"amqps://{host}"), eventHub, policy, key);
            _client = EventHubClient.CreateFromConnectionString(builder.ToString());
        }

        private EventHubClient _client;
        private string _consumerGroup;

        protected sealed override async Task RunAsync(CancellationToken cancellationToken)
        {
            await TryConfigureAsync(cancellationToken).ConfigureAwait(false);
            var partitions = await GetOrCreatePartitionListAsync(cancellationToken).ConfigureAwait(false);
            using (var cts = new CancellationTokenSource())
            using (cancellationToken.Register(() => cts.Cancel()))
            {
                try
                {
                    var offsets = await StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetsName).ConfigureAwait(false);
                    var tasks = new Task[partitions.Length];
                    for (var i = 0; i < partitions.Length; i++)
                    {
                        tasks[i] = ProcessPartitionAsync(partitions[i], offsets, cts.Token);
                    }
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (OperationCanceledException ex)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        Log.Information("Host service cancelled");
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    else
                    {
                        Log.Warning(ex, "Not sure why, but this is exiting: {exception}", ex.Message);
                    }
                    throw;
                }
                catch (Exception ex) when (!(ex is FabricException))
                {
                    Log.Error(ex, "Hub service cancelling: {exception}", ex.Message);
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
                    return DefaultPosition;
                }
            }
        }

        private async Task ProcessPartitionAsync(string partition, IReliableDictionary<string, string> offsets, CancellationToken ct)
        {
            var offset = await ReadOffsetAsync(partition, offsets).ConfigureAwait(false);
            Log.Information("Process partition {partition} with offset {offset}", partition, offset);
            var offsetInclusive = offset == PartitionReceiver.StartOfStream;
            var receiver = _client.CreateEpochReceiver(_consumerGroup, partition, offset, offsetInclusive, DateTime.UtcNow.Ticks);
            try
            {
                var handler = CreateReadingReceiver(Log, StateManager, offsets, partition, ct);
                await handler.InitialiseAsync().ConfigureAwait(false);
                receiver.SetReceiveHandler(handler);
                await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!((ex is FabricException) || (ex is OperationCanceledException)))
            {
                Log.Error(ex, "Error processing partition: {exception}", ex.Message);
                throw;
            }
            finally
            {
                Log.Information("Finished processing partition {partition}", partition);
                await receiver.CloseAsync().ConfigureAwait(false);
            }
        }

        protected abstract BaseReadingReceiver CreateReadingReceiver(ILogger log, IReliableStateManager stateManager, IReliableDictionary<string, string> offsets, string partition, CancellationToken ct);
    }
}

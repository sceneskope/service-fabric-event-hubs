using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Serilog;
using ServiceFabric.Utilities;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BatchedReadingReceiver : IReadingReceiver
    {
        public Func<Exception, bool> TransientExceptionChecker { get; }
        public ServiceFabricRetryHandler RetryHandler { get; }
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;

        public PartitionReceiver Receiver { get; }

        protected BatchedReadingReceiver(ILogger log,
            IReliableStateManager stateManager,
            PartitionReceiver receiver,
            IReliableDictionary<string, string> offsets, string partition,
            ServiceFabricRetryHandler retryHandler,
            Func<Exception, bool> transientExceptionChecker = null)
        {
            RetryHandler = retryHandler;
            Log = log;
            StateManager = stateManager;
            Receiver = receiver;
            _offsets = offsets;
            _partition = partition;
            TransientExceptionChecker = transientExceptionChecker;
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        protected abstract Task ProcessEventAsync(ITransaction tx, EventData @event, CancellationToken serviceCancellationToken);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            await BeforeProcessEventsAsync(RetryHandler.ServiceCancellationToken).ConfigureAwait(false);
            await RetryHandler.HandleAsync(async cancel =>
            {
                string lastOffset = null;
                using (var tx = StateManager.CreateTransaction())
                {
                    await AfterTransactionOpenAsync(tx, cancel).ConfigureAwait(false);
                    foreach (var @event in events)
                    {
                        await ProcessEventAsync(tx, @event, cancel).ConfigureAwait(false);
                        lastOffset = @event.SystemProperties.Offset;
                    }
                    await _offsets.SetAsync(tx, _partition, lastOffset).ConfigureAwait(false);
                    await BeforeTransactionCommitAsync(tx, cancel).ConfigureAwait(false);
                    await tx.CommitAsync().ConfigureAwait(false);
                }
            }, false, TransientExceptionChecker).ConfigureAwait(false);
            await AfterProcessEventsAsync(RetryHandler.ServiceCancellationToken).ConfigureAwait(false);
        }

        public virtual Task AfterTransactionOpenAsync(ITransaction tx, CancellationToken serviceCancellationToken) => Task.CompletedTask;
        public virtual Task BeforeTransactionCommitAsync(ITransaction tx, CancellationToken serviceCancellationToken) => Task.CompletedTask;
        public virtual Task BeforeProcessEventsAsync(CancellationToken serviceCancellationToken) => Task.CompletedTask;
        public virtual Task AfterProcessEventsAsync(CancellationToken serviceCancellationToken) => Task.CompletedTask;

        public virtual void Dispose()
        {
        }
    }
}

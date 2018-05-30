using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Polly;
using Serilog;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public abstract class BaseReadingReceiver : IReadingReceiver
    {
        protected static Random RandomJitter { get; } = new Random();
        public ILogger Log { get; }
        public IReliableStateManager StateManager { get; }

        private readonly IReliableDictionary<string, string> _offsets;
        protected readonly string _partition;
        public CancellationTokenSource TokenSource { get; }

        public int MaxBatchSize { get; set; } = 100;
        protected Policy TimeoutPolicy { get; }

        public PartitionReceiver Receiver { get; }

        protected BaseReadingReceiver(ILogger log, IReliableStateManager stateManager,
            PartitionReceiver receiver,
        IReliableDictionary<string, string> offsets, string partition,
        CancellationToken ct)
        {
            Log = log;
            StateManager = stateManager;
            Receiver = receiver;
            _offsets = offsets;
            _partition = partition;
            TokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
            TimeoutPolicy =
                Policy
                .Handle<TimeoutException>()
                .WaitAndRetryForeverAsync(n => TimeSpan.FromMilliseconds((n < 10) ? RandomJitter.Next(n*100, (n+1)*100) : RandomJitter.Next(500, 1500)),
                    (ex, ts) => Log.Warning(ex, "Delaying {Ts} due to {Exception}", ts, ex.Message));
        }

        public virtual Task InitialiseAsync() => Task.CompletedTask;

        public virtual Task ProcessErrorAsync(Exception error)
        {
            Log.Error(error, "Error reading: {Exception}", error.Message);
            switch (error)
            {
                case ReceiverDisconnectedException _:
                case OperationCanceledException _:
                case EventHubsException _:
                    Log.Information("Cancelling the receiver");
                    TokenSource.Cancel();
                    break;
            }
            return Task.CompletedTask;
        }

        protected abstract Task ProcessEventAsync(ITransaction tx, EventData @event);

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null)
            {
                return;
            }

            await BeforeProcessEventsAsync(TokenSource.Token).ConfigureAwait(false);
            try
            {
                string lastOffset = null;
#pragma warning disable RCS1163 // Unused parameter.
                await TimeoutPolicy.ExecuteAsync(async _ =>
#pragma warning restore RCS1163 // Unused parameter.
                {
                    var count = events.Count();
                    using (var tx = StateManager.CreateTransaction())
                    {
                        foreach (var @event in events)
                        {
                            await ProcessEventAsync(tx, @event).ConfigureAwait(false);
                            lastOffset = @event.SystemProperties.Offset;
                        }
                        await _offsets.SetAsync(tx, _partition, lastOffset).ConfigureAwait(false);
                        await tx.CommitAsync().ConfigureAwait(false);
                    }
                }, TokenSource.Token, false).ConfigureAwait(false);
            }
            finally
            {
                await AfterProcessEventsAsync(TokenSource.Token).ConfigureAwait(false);
            }
        }

        public virtual Task WaitForFinishedAsync(CancellationToken ct) => Task.Delay(Timeout.Infinite, TokenSource.Token);

        public virtual Task BeforeProcessEventsAsync(CancellationToken ct) => Task.CompletedTask;
        public virtual Task AfterProcessEventsAsync(CancellationToken ct) => Task.CompletedTask;
    }
}

﻿using System;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Serilog;

namespace SceneSkope.ServiceFabric.EventHubs
{
    public class ServiceFabricRetryHandler
    {
        public CancellationToken ServiceCancellationToken { get; }
        public ILogger Logger { get; }
        private Random JitterProvider { get; } = new Random();
        private int JitterFromMs { get; }
        private int JitterToMs { get; }
        private readonly object _lock = new object();

        public ServiceFabricRetryHandler(ILogger logger, CancellationToken serviceCancellationToken, int jitterFromMs = 10, int jitterToMs = 1000)
        {
            Logger = logger;
            ServiceCancellationToken = serviceCancellationToken;
            JitterFromMs = jitterFromMs;
            JitterToMs = jitterToMs;
        }

        public void ThrowIfCancellationRequested() => ServiceCancellationToken.ThrowIfCancellationRequested();
        public bool IsCancellationRequested => ServiceCancellationToken.IsCancellationRequested;

        public Task RandomDelay() => RandomDelay(JitterFromMs, JitterToMs);

        public Task RandomDelay(int fromMs, int toMs)
        {
            int delayMs;
            lock (_lock)
            {
                delayMs = JitterProvider.Next(fromMs, toMs);
            }
            return Task.Delay(delayMs, ServiceCancellationToken);
        }

        private bool IsTransientException(Exception ex, Func<Exception, bool> transientExceptionChecker)
        {
            switch (ex)
            {
                case TimeoutException te:
                    Logger.Warning("Timeout, retrying after delay");
                    return true;

                case FabricException fe:
                    Logger.Warning(fe, "Fabric exception, retrying after delay: {Exception}", fe.Message);
                    return true;

                default:
                    if (transientExceptionChecker?.Invoke(ex) ?? false)
                    {
                        Logger.Warning(ex, "Transient exception, retrying after delay: {Exception}", ex.Message);
                        return true;
                    }
                    else
                    {
                        Logger.Error(ex, "Not a transient exception: {Exception}", ex.Message);
                        return false;
                    }
            }
        }

        public async Task HandleAsync(Func<CancellationToken, Task> executor, bool continueOnCapturedContext = false, Func<Exception, bool> transientExceptionChecker = null)
        {
            while (true)
            {
                ServiceCancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await executor(ServiceCancellationToken).ConfigureAwait(continueOnCapturedContext);
                    return;
                }
                catch (Exception ex)
                {
                    ServiceCancellationToken.ThrowIfCancellationRequested();
                    if (!IsTransientException(ex, transientExceptionChecker))
                    {
                        return;
                    }
                }
                await RandomDelay().ConfigureAwait(continueOnCapturedContext);
            }
        }

        public Task<TResult> CallAsync<TResult>(Func<CancellationToken, Task<TResult>> executor, bool continueOnCapturedContext = false) =>
            TryCallAsync(executor, continueOnCapturedContext, null)
            .ContinueWith(cv => cv.Result.Value,
                TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously);

        public async Task<ConditionalValue<TResult>> TryCallAsync<TResult>(Func<CancellationToken, Task<TResult>> executor, bool continueOnCapturedContext = false, Func<Exception, bool> transientExceptionChecker = null)
        {
            while (true)
            {
                ServiceCancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var result = await executor(ServiceCancellationToken).ConfigureAwait(continueOnCapturedContext);
                    return new ConditionalValue<TResult>(true, result);
                }
                catch (Exception ex)
                {
                    ServiceCancellationToken.ThrowIfCancellationRequested();
                    if (!IsTransientException(ex, transientExceptionChecker))
                    {
                        return new ConditionalValue<TResult>();
                    }
                }
                await RandomDelay().ConfigureAwait(continueOnCapturedContext);
            }
        }
    }
}
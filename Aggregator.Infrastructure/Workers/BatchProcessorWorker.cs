using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Aggregator.Infrastructure.Workers;

public class BatchProcessorWorker(TickChannelBus bus, ITickRepository repository, ILogger<BatchProcessorWorker> logger) : BackgroundService
{
    private readonly TickChannelBus _bus = bus;
    private readonly ITickRepository _repository = repository;
    private readonly ILogger<BatchProcessorWorker> _logger = logger;

    private const int BatchSize = 1000;
    private readonly TimeSpan _batchTimeout = TimeSpan.FromMilliseconds(500);

    private static readonly Counter TicksWrittenCounter = Metrics.CreateCounter(
        "aggregator_ticks_written_total",
        "Total ticks successfully written to DB via COPY BINARY.");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("BatchProcessorWorker started.");
        var buffer = new List<Tick>(BatchSize);

        var timeoutCts = new CancellationTokenSource();
        var reg = stoppingToken.Register(() => timeoutCts.Cancel());

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (buffer.Count == 0)
                {
                    try
                    {
                        if (!await _bus.WaitToReadAsync(stoppingToken)) break;
                    }
                    catch (OperationCanceledException) { break; }
                }

                if (!timeoutCts.TryReset())
                {
                    timeoutCts.Dispose();
                    reg.Dispose();
                    timeoutCts = new CancellationTokenSource();
                    reg = stoppingToken.Register(() => timeoutCts.Cancel());
                }

                timeoutCts.CancelAfter(_batchTimeout);

                try
                {
                    while (buffer.Count < BatchSize)
                    {
                        if (!await _bus.WaitToReadAsync(timeoutCts.Token)) break;

                        while (buffer.Count < BatchSize && _bus.TryRead(out var tick))
                        {
                            buffer.Add(tick);
                        }
                    }
                }
                catch (OperationCanceledException) { /* Таймаут сбора батча, идем сбрасывать что есть */ }

                if (buffer.Count > 0)
                {
                    await FlushAsync(buffer, stoppingToken);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Fatal error in BatchProcessorWorker loop.");
        }

        _logger.LogInformation("Shutdown triggered. Initiating graceful drain...");
        _bus.Complete();

        using var gracefulCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            while (await _bus.WaitToReadAsync(gracefulCts.Token))
            {
                while (_bus.TryRead(out var tick))
                {
                    buffer.Add(tick);
                    if (buffer.Count >= BatchSize)
                    {
                        await FlushAsync(buffer, gracefulCts.Token);
                    }
                }
            }

            if (buffer.Count > 0)
            {
                _logger.LogInformation("Flushing final {Count} ticks.", buffer.Count);
                await FlushAsync(buffer, gracefulCts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Graceful drain timed out. Unflushed memory ticks are lost.");
        }
        finally
        {
            timeoutCts.Dispose();
            reg.Dispose();
        }

        _logger.LogInformation("BatchProcessorWorker stopped.");
    }

    private async Task FlushAsync(List<Tick> buffer, CancellationToken cancellationToken)
    {
        if (buffer.Count == 0) return;

        try
        {
            await _repository.SaveBatchAsync(buffer, cancellationToken);
            TicksWrittenCounter.Inc(buffer.Count);
            buffer.Clear();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error saving batch. Retrying {Count} ticks in 5 seconds to prevent data loss...", buffer.Count);

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (OperationCanceledException) { }
        }
    }
}

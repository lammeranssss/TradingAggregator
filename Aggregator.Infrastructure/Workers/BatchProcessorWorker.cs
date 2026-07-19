using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Aggregator.Infrastructure.Workers;

public class BatchProcessorWorker(TickChannelBus bus, ITickRepository repository, ILogger<BatchProcessorWorker> logger) : BackgroundService
{
    private readonly TickChannelBus _bus = bus;
    private readonly ITickRepository _repository = repository;
    private readonly ILogger<BatchProcessorWorker> _logger = logger;

    private const int BatchSize = 1000;
    private readonly TimeSpan _batchTimeout = TimeSpan.FromMilliseconds(500);

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
                    catch (OperationCanceledException)
                    {
                        break;
                    }
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
                catch (OperationCanceledException) { /* Таймаут */ }

                if (buffer.Count > 0)
                {
                    await FlushAsync(buffer, CancellationToken.None);
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
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Graceful drain timed out.");
        }
        finally
        {
            if (buffer.Count > 0)
            {
                _logger.LogInformation("Flushing final {Count} ticks.", buffer.Count);
                await FlushAsync(buffer, gracefulCts.Token);
            }

            timeoutCts.Dispose();
            reg.Dispose();
        }

        _logger.LogInformation("BatchProcessorWorker stopped.");
    }

    private async Task FlushAsync(List<Tick> buffer, CancellationToken cancellationToken)
    {
        try
        {
            await _repository.SaveBatchAsync(buffer, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error saving batch. {Count} ticks lost.", buffer.Count);
        }
        finally
        {
            buffer.Clear();
        }
    }
}

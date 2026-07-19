using Aggregator.Core.Services;
using Microsoft.Extensions.Hosting;
using Prometheus;

namespace Aggregator.Infrastructure.Workers;

public class ChannelMetricsReporter(TickChannelBus bus) : BackgroundService
{
    private static readonly Gauge ChannelFullnessGauge = Metrics.CreateGauge(
        "aggregator_channel_fullness_ratio",
        "In-memory channel fullness ratio.");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            ChannelFullnessGauge.Set((double)bus.CurrentCount / bus.Capacity);
        }
    }
}

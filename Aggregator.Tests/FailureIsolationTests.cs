using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Aggregator.Infrastructure.Exchanges;
using Microsoft.Extensions.Logging.Abstractions;
using Polly;
using Polly.Registry;
using Xunit;

namespace Aggregator.Tests;

public class FailureIsolationTests
{
    private class FailingBinanceAdapter : BaseWebSocketAdapter
    {
        protected override Uri Endpoint => new("ws://test");
        protected override ExchangeSource Source => ExchangeSource.Binance;

        public FailingBinanceAdapter(TickChannelBus bus, ResiliencePipelineProvider<string> provider)
            : base(bus, NullLogger<FailingBinanceAdapter>.Instance, provider) { }

        protected override Tick? ParseTick(ref System.Text.Json.Utf8JsonReader reader) => null;

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            throw new Exception("Simulated fatal network error on Binance");
        }
    }

    private class StableCoinbaseAdapter : BaseWebSocketAdapter
    {
        protected override Uri Endpoint => new("ws://test");
        protected override ExchangeSource Source => ExchangeSource.Coinbase;

        public StableCoinbaseAdapter(TickChannelBus bus, ResiliencePipelineProvider<string> provider)
            : base(bus, NullLogger<StableCoinbaseAdapter>.Instance, provider) { }

        protected override Tick? ParseTick(ref System.Text.Json.Utf8JsonReader reader) => null;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var tick = new Tick("ETH-USD", 3500.00m, 10.0m, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), ExchangeSource.Coinbase);
                await Bus.PublishAsync(tick, stoppingToken);
                await Task.Delay(50, stoppingToken);
            }
        }
    }

    [Fact]
    public async Task Infrastructure_WhenBinanceCrashes_CoinbaseContinuesPublishing()
    {
        var bus = new TickChannelBus();
        var registry = new ResiliencePipelineRegistry<string>();

        registry.TryAddBuilder("ws-retry", (builder, context) => builder.AddRetry(new()
        {
            MaxRetryAttempts = 1,
            Delay = TimeSpan.Zero
        }));

        var failingAdapter = new FailingBinanceAdapter(bus, registry);
        var stableAdapter = new StableCoinbaseAdapter(bus, registry);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var failingTask = failingAdapter.StartAsync(cts.Token);
        var stableTask = stableAdapter.StartAsync(cts.Token);

        using var readTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var receivedTick = await bus.Reader.ReadAsync(readTimeoutCts.Token);

        Assert.Equal(ExchangeSource.Coinbase, receivedTick.Source);

        Assert.True(failingTask.IsFaulted || failingTask.IsCompleted);
        Assert.False(stableTask.IsFaulted);
    }
}

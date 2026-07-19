using System.Collections.Concurrent;
using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Aggregator.Tests;

public class LockFreeDeduplicatorTests
{
    [Fact]
    public void Deduplicator_UnderHeavyConcurrency_PreventsDuplicates()
    {
        var fakeTimeProvider = new FakeTimeProvider();
        var logger = NullLogger<LockFreeDeduplicator>.Instance;
        IDeduplicator deduplicator = new LockFreeDeduplicator(fakeTimeProvider, logger, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

        int threadCount = 10;
        int iterations = 5000;
        var duplicateTick = new Tick("BTCUSDT", 65000.00m, 1.5m, 1672531199000, ExchangeSource.Binance);
        var uniqueResults = new ConcurrentBag<bool>();

        Parallel.For(0, threadCount, _ =>
        {
            for (int j = 0; j < iterations; j++)
            {
                if (deduplicator.IsUnique(in duplicateTick, 1))
                {
                    uniqueResults.Add(true);
                }
            }
        });

        Assert.Single(uniqueResults);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(6));
        Assert.True(deduplicator.IsUnique(in duplicateTick, 1));
    }
}

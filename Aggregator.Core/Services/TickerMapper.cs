using System.Collections.Concurrent;

namespace Aggregator.Core.Services;

/// <summary>
/// Решает проблему коллизий GetHashCode и строк.
/// Выдает строгий уникальный int ID для каждого нового тикера.
/// </summary>
public class TickerMapper
{
    private readonly ConcurrentDictionary<string, int> _tickerToId = new(StringComparer.OrdinalIgnoreCase);
    private int _counter = 0;

    private const int MaxTickers = 10_000;

    public int GetOrAddId(string ticker)
    {
        if (_tickerToId.TryGetValue(ticker, out var existingId))
        {
            return existingId;
        }

        if (Volatile.Read(ref _counter) >= MaxTickers)
        {
            throw new InvalidOperationException($"Ticker dictionary limit ({MaxTickers}) reached. Possible invalid data flood.");
        }

        return _tickerToId.GetOrAdd(ticker, _ => Interlocked.Increment(ref _counter));
    }
}
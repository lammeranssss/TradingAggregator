using System.Collections.Concurrent;
using System.Text;
using CommunityToolkit.HighPerformance.Buffers;

namespace Aggregator.Core.Services;

public class TickerMapper
{
    private readonly ConcurrentDictionary<string, int> _tickerToId = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<int, TickerCacheNode> _spanCache = new();
    private int _counter = 0;
    private const int MaxTickers = 10_000;

    public int GetOrAddId(string ticker)
    {
        if (_tickerToId.TryGetValue(ticker, out var existingId)) return existingId;

        if (Volatile.Read(ref _counter) >= MaxTickers)
            throw new InvalidOperationException($"Ticker dictionary limit ({MaxTickers}) reached.");

        var id = _tickerToId.GetOrAdd(ticker, _ => Interlocked.Increment(ref _counter));

        var bytes = Encoding.UTF8.GetBytes(ticker);
        var hash = ComputeFnv1aHash(bytes);
        _spanCache.TryAdd(hash, new TickerCacheNode(bytes, ticker, id));

        return id;
    }

    public bool TryGetIdFromSpan(ReadOnlySpan<byte> span, out int tickerId, out string tickerString)
    {
        var hash = ComputeFnv1aHash(span);

        if (_spanCache.TryGetValue(hash, out var node) && span.SequenceEqual(node.Utf8Bytes))
        {
            tickerId = node.Id;
            tickerString = node.TickerString;
            return true;
        }

        int maxCharCount = Encoding.UTF8.GetMaxCharCount(span.Length);

        Span<char> chars = maxCharCount <= 128 ? stackalloc char[128] : new char[maxCharCount];
        int charCount = Encoding.UTF8.GetChars(span, chars);

        var pooledString = StringPool.Shared.GetOrAdd(chars.Slice(0, charCount));

        tickerString = pooledString;
        tickerId = GetOrAddId(pooledString);
        return true;
    }

    private static int ComputeFnv1aHash(ReadOnlySpan<byte> data)
    {
        unchecked
        {
            const int fnvPrime = 16777619;
            int hash = (int)2166136261;
            foreach (var b in data) hash = (hash ^ b) * fnvPrime;
            return hash;
        }
    }

    private class TickerCacheNode(byte[] utf8Bytes, string tickerString, int id)
    {
        public byte[] Utf8Bytes { get; } = utf8Bytes;
        public string TickerString { get; } = tickerString;
        public int Id { get; } = id;
    }
}

using System.Collections.Concurrent;
using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Microsoft.Extensions.Logging;

namespace Aggregator.Core.Services;

/// <summary>
/// Потокобезопасный дедупликатор с O(1) очисткой и защитой от гонок.
/// </summary>
public class LockFreeDeduplicator : IDeduplicator, IDisposable
{
    private readonly ConcurrentDictionary<TickKey, long> _cache = new();
    private readonly ConcurrentQueue<(TickKey Key, long ArrivalTime)> _evictionQueue = new();

    private readonly ITimer _evictionTimer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<LockFreeDeduplicator> _logger;
    private readonly long _windowMs;

    private int _isEvicting;

    public LockFreeDeduplicator(
        TimeProvider timeProvider,
        ILogger<LockFreeDeduplicator> logger,
        TimeSpan window,
        TimeSpan evictionInterval)
    {
        _timeProvider = timeProvider;
        _logger = logger;
        _windowMs = (long)window.TotalMilliseconds;

        _evictionTimer = _timeProvider.CreateTimer(EvictOldEntries, null, evictionInterval, evictionInterval);
    }

    public bool IsUnique(in Tick tick, int tickerId)
    {
        var key = new TickKey(tick.Source, tickerId, tick.TimestampMs, tick.Price, tick.Volume);
        var arrivalTime = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();

        if (_cache.TryAdd(key, arrivalTime))
        {
            _evictionQueue.Enqueue((key, arrivalTime));
            return true;
        }

        return false;
    }

    private void EvictOldEntries(object? state)
    {
        if (Interlocked.CompareExchange(ref _isEvicting, 1, 0) == 1)
        {
            return;
        }

        try
        {
            var threshold = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds() - _windowMs;

            while (_evictionQueue.TryPeek(out var item) && item.ArrivalTime < threshold)
            {
                if (_evictionQueue.TryDequeue(out var dequeuedItem))
                {
                    var kvp = new KeyValuePair<TickKey, long>(dequeuedItem.Key, dequeuedItem.ArrivalTime);
                    ((ICollection<KeyValuePair<TickKey, long>>)_cache).Remove(kvp);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during deduplication eviction.");
        }
        finally
        {
            Volatile.Write(ref _isEvicting, 0);
        }
    }

    public void Dispose()
    {
        _evictionTimer.Dispose();
        _cache.Clear();
        _evictionQueue.Clear();
    }
}

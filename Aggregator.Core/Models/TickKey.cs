namespace Aggregator.Core.Models;

public readonly record struct TickKey(
    ExchangeSource Source,
    int TickerId,
    long TimestampMs,
    decimal Price,
    decimal Volume 
);

namespace Aggregator.Core.Models;

public readonly record struct Tick(
    string Ticker,
    decimal Price,
    decimal Volume,
    long TimestampMs,
    ExchangeSource Source
);

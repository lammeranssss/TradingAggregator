using System.Buffers;
using System.Buffers.Text;
using System.Text.Json;
using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace Aggregator.Infrastructure.Exchanges;

public class BinanceWebSocketAdapter(
    TickChannelBus bus,
    ILogger<BinanceWebSocketAdapter> logger,
    ResiliencePipelineProvider<string> pipelineProvider,
    TickerMapper tickerMapper,
    IDeduplicator deduplicator,
    IConfiguration config) : BaseWebSocketAdapter(bus, logger, pipelineProvider)
{
    private readonly TickerMapper _tickerMapper = tickerMapper;
    private readonly IDeduplicator _deduplicator = deduplicator;
    private readonly ILogger<BinanceWebSocketAdapter> _logger = logger;

    protected override Uri Endpoint { get; } = new(config["BinanceUrl"] ?? "ws://localhost:8081/binance");
    protected override ExchangeSource Source => ExchangeSource.Binance;

    private static ReadOnlySpan<byte> StreamProp => "e"u8;
    private static ReadOnlySpan<byte> TimeProp => "E"u8;
    private static ReadOnlySpan<byte> SymbolProp => "s"u8;
    private static ReadOnlySpan<byte> PriceProp => "p"u8;
    private static ReadOnlySpan<byte> VolumeProp => "q"u8;

    protected override Tick? ParseTick(ref Utf8JsonReader reader)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject) return null;

        long timestampMs = 0; 
        int tickerId = 0;
        string tickerStr = string.Empty;
        decimal price = 0;
        decimal volume = 0;

        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName) continue;

            var propName = reader.ValueSpan;
            reader.Read();

            if (propName.SequenceEqual(StreamProp)) continue;

            if (propName.SequenceEqual(SymbolProp))
            {
                _tickerMapper.TryGetIdFromSpan(reader.ValueSpan, out tickerId, out tickerStr);
            }
            else if (propName.SequenceEqual(TimeProp))
            {
                timestampMs = reader.GetInt64();
            }
            else if (propName.SequenceEqual(PriceProp))
            {
                if (!Utf8Parser.TryParse(reader.ValueSpan, out price, out _))
                {
                    _logger.LogWarning("Binance price corruption at parsing tokens.");
                    return null;
                }
            }
            else if (propName.SequenceEqual(VolumeProp))
            {
                if (!Utf8Parser.TryParse(reader.ValueSpan, out volume, out _))
                {
                    _logger.LogWarning("Binance volume corruption at parsing tokens.");
                    return null;
                }
            }
        }

        if (tickerId == 0) return null;

        var tick = new Tick(tickerStr, price, volume, timestampMs, Source);
        return _deduplicator.IsUnique(in tick, tickerId) ? tick : null;
    }
}

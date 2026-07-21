using System.Buffers;
using System.Buffers.Text;
using System.Text.Json;
using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace Aggregator.Infrastructure.Exchanges;

public class CoinbaseWebSocketAdapter(
    TickChannelBus bus,
    ILogger<CoinbaseWebSocketAdapter> logger,
    ResiliencePipelineProvider<string> pipelineProvider,
    TickerMapper tickerMapper,
    IDeduplicator deduplicator,
    IOptions<ExchangeOptions> options) : BaseWebSocketAdapter(bus, logger, pipelineProvider)
{
    private readonly TickerMapper _tickerMapper = tickerMapper;
    private readonly IDeduplicator _deduplicator = deduplicator;
    private readonly ILogger<CoinbaseWebSocketAdapter> _logger = logger;

    protected override Uri Endpoint { get; } = new(options.Value.CoinbaseUrl);
    protected override ExchangeSource Source => ExchangeSource.Coinbase;

    private static ReadOnlySpan<byte> TypeProp => "type"u8;
    private static ReadOnlySpan<byte> ProductProp => "product_id"u8;
    private static ReadOnlySpan<byte> PriceProp => "price"u8;
    private static ReadOnlySpan<byte> VolumeProp => "volume_24h"u8;
    private static ReadOnlySpan<byte> TimeProp => "time"u8;

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

            if (propName.SequenceEqual(TypeProp)) continue;

            if (propName.SequenceEqual(ProductProp))
            {
                _tickerMapper.TryGetIdFromSpan(reader.ValueSpan, out tickerId, out tickerStr);
            }
            else if (propName.SequenceEqual(PriceProp))
            {
                price = reader.GetDecimal();
            }
            else if (propName.SequenceEqual(VolumeProp))
            {
                volume = reader.GetDecimal();
            }
            else if (propName.SequenceEqual(TimeProp))
            {
                if (reader.TryGetDateTimeOffset(out DateTimeOffset dto))
                {
                    timestampMs = dto.ToUnixTimeMilliseconds();
                }
                else
                {
                    _logger.LogWarning("Coinbase timestamp format error.");
                    return null;
                }
            }
        }

        if (tickerId == 0) return null;

        var tick = new Tick(tickerStr, price, volume, timestampMs, Source);
        return _deduplicator.IsUnique(in tick, tickerId) ? tick : null;
    }
}

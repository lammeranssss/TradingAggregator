using System.Threading.Channels;
using Aggregator.Core.Models;
using Microsoft.Extensions.Logging;

namespace Aggregator.Core.Services;

/// <summary>
/// Шина сообщений на базе System.Threading.Channels.
/// Обеспечивает non-blocking backpressure для WebSocket клиентов.
/// </summary>
public class TickChannelBus
{
    private readonly Channel<Tick> _channel;
    private readonly ILogger<TickChannelBus> _logger;

    public int CurrentCount => _channel.Reader.Count;

    public TickChannelBus(ILogger<TickChannelBus> logger, int capacity = 100_000)
    {
        _logger = logger;

        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        };

        _channel = Channel.CreateBounded<Tick>(options);
    }

    public void Publish(in Tick tick)
    {
        _channel.Writer.TryWrite(tick);
    }

    public IAsyncEnumerable<Tick> ReadAllAsync(CancellationToken cancellationToken)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }

    public void Complete() => _channel.Writer.TryComplete();
}

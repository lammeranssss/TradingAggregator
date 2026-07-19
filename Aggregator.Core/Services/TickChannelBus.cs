using System.Threading.Channels;
using Aggregator.Core.Models;
using Microsoft.Extensions.Logging;

namespace Aggregator.Core.Services;

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

    public void Publish(in Tick tick) => _channel.Writer.TryWrite(tick);

    public IAsyncEnumerable<Tick> ReadAllAsync(CancellationToken cancellationToken)
        => _channel.Reader.ReadAllAsync(cancellationToken);

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        => _channel.Reader.WaitToReadAsync(cancellationToken);

    public bool TryRead(out Tick tick)
        => _channel.Reader.TryRead(out tick);

    public void Complete() => _channel.Writer.TryComplete();
}

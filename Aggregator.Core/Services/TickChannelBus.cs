using System.Threading.Channels;
using Aggregator.Core.Models;
using Prometheus;

namespace Aggregator.Core.Services;

public class TickChannelBus
{
    private readonly Channel<Tick> _channel;

    public int Capacity { get; } = 100_000;

    private static readonly string[] SourceLabels = ["unknown", "binance", "coinbase", "kraken"];

    private static readonly Counter TicksIngestedCounter = Metrics.CreateCounter(
        "aggregator_ticks_ingested_total",
        "Total ticks successfully read from exchange sockets.",
        new CounterConfiguration { LabelNames = new[] { "exchange" } });

    private readonly Counter.Child[] _ingestedCounters;

    public int CurrentCount => _channel.Reader.Count;
    public ChannelReader<Tick> Reader => _channel.Reader;

    public TickChannelBus()
    {
        var options = new BoundedChannelOptions(Capacity)
        {
            AllowSynchronousContinuations = false,
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        };
        _channel = Channel.CreateBounded<Tick>(options);
        _ingestedCounters = new Counter.Child[SourceLabels.Length];
        for (int i = 0; i < SourceLabels.Length; i++)
        {
            _ingestedCounters[i] = TicksIngestedCounter.WithLabels(SourceLabels[i]);
        }
    }

    public async ValueTask PublishAsync(Tick tick, CancellationToken ct)
    {
        await _channel.Writer.WriteAsync(tick, ct);

        int sourceIndex = (byte)tick.Source;
        int index = sourceIndex < _ingestedCounters.Length ? sourceIndex : 0;
        _ingestedCounters[index].Inc();
    }

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken)
        => _channel.Reader.WaitToReadAsync(cancellationToken);

    public bool TryRead(out Tick tick)
        => _channel.Reader.TryRead(out tick);

    public void Complete() => _channel.Writer.TryComplete();
}

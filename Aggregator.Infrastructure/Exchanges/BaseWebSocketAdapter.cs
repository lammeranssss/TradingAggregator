using System.Buffers;
using System.IO.Pipelines;
using System.Net.WebSockets;
using System.Text.Json;
using Aggregator.Core.Models;
using Aggregator.Core.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Registry;

namespace Aggregator.Infrastructure.Exchanges;

public abstract class BaseWebSocketAdapter : BackgroundService
{
    private readonly TickChannelBus _bus;
    private readonly ILogger _logger;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly TimeSpan _idleTimeout = TimeSpan.FromSeconds(15);

    protected abstract Uri Endpoint { get; }
    protected abstract ExchangeSource Source { get; }

    protected BaseWebSocketAdapter(TickChannelBus bus, ILogger logger, ResiliencePipelineProvider<string> pipelineProvider)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _retryPipeline = pipelineProvider.GetPipeline("ws-retry");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _retryPipeline.ExecuteAsync(async ct =>
        {
            // ИСПРАВЛЕНИЕ: Чистый блок using управляет жизненным циклом сокета, никаких ручных сомнительных Dispose
            using var webSocket = new ClientWebSocket();
            webSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);

            _logger.LogInformation("Connecting to {Source} at {Endpoint}", Source, Endpoint);
            await webSocket.ConnectAsync(Endpoint, ct);
            _logger.LogInformation("Connected to {Source}", Source);

            await OnConnectedAsync(webSocket, ct);

            var pipe = new Pipe();
            using var loopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            var fillTask = FillPipeAsync(webSocket, pipe.Writer, loopCts.Token);
            var readTask = ReadPipeAsync(pipe.Reader, loopCts.Token);

            await Task.WhenAny(fillTask, readTask);
            await loopCts.CancelAsync();
            await Task.WhenAll(fillTask, readTask);

            throw new WebSocketException($"Connection to {Source} lost.");
        }, stoppingToken);
    }

    private async Task FillPipeAsync(ClientWebSocket webSocket, PipeWriter writer, CancellationToken ct)
    {
        using var idleCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        try
        {
            while (webSocket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                idleCts.CancelAfter(_idleTimeout);

                var memory = writer.GetMemory(4096);
                var result = await webSocket.ReceiveAsync(memory, idleCts.Token);

                if (result.MessageType == WebSocketMessageType.Close) break;

                writer.Advance(result.Count);

                if (result.EndOfMessage)
                {
                    var flushResult = await writer.FlushAsync(idleCts.Token);
                    if (flushResult.IsCompleted || flushResult.IsCanceled) break;
                }
            }
        }
        catch (OperationCanceledException) when (idleCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            _logger.LogWarning("{Source} half-open connection detected via idle timeout.", Source);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing network bytes to pipe for {Source}.", Source);
        }
        finally
        {
            await writer.CompleteAsync();
            // ИСПРАВЛЕНИЕ: Больше не вызываем webSocket.Dispose() здесь, отдаем управление внешнему using scope
        }
    }

    private async Task ReadPipeAsync(PipeReader reader, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                var consumed = buffer.Start;
                var examined = buffer.End;

                while (TryParseJsonMessage(ref buffer, out var message, out var nextPosition))
                {
                    ProcessMessage(in message);
                    consumed = nextPosition;
                    buffer = buffer.Slice(nextPosition);
                }

                reader.AdvanceTo(consumed, examined);

                if (result.IsCompleted) break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading and parsing pipe data for {Source}.", Source);
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    private static bool TryParseJsonMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out SequencePosition nextPosition)
    {
        message = default;
        nextPosition = default;

        var jsonReader = new Utf8JsonReader(buffer);
        try
        {
            if (!jsonReader.Read()) return false;

            int depth = 0;
            do
            {
                if (jsonReader.TokenType == JsonTokenType.StartObject || jsonReader.TokenType == JsonTokenType.StartArray) depth++;
                if (jsonReader.TokenType == JsonTokenType.EndObject || jsonReader.TokenType == JsonTokenType.EndArray) depth--;
            } while (depth > 0 && jsonReader.Read());

            if (depth == 0)
            {
                nextPosition = jsonReader.Position;
                message = buffer.Slice(buffer.Start, nextPosition);
                return true;
            }
        }
        catch (JsonException)
        {
            // Сообщение фрагментировано, поток TCP еще не доставил оставшиеся байты. Ждем.
        }
        return false;
    }

    private void ProcessMessage(in ReadOnlySequence<byte> buffer)
    {
        try
        {
            var jsonReader = new Utf8JsonReader(buffer);
            var parsedTick = ParseTick(ref jsonReader);

            if (parsedTick != null) _bus.Publish(parsedTick.Value);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Malformed JSON packet ignored from {Source}", Source);
        }
    }

    protected virtual Task OnConnectedAsync(ClientWebSocket webSocket, CancellationToken ct) => Task.CompletedTask;
    protected abstract Tick? ParseTick(ref Utf8JsonReader reader);
}

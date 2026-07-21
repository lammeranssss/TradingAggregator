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

public abstract class BaseWebSocketAdapter(TickChannelBus bus, ILogger logger, ResiliencePipelineProvider<string> pipelineProvider) : BackgroundService
{
    protected readonly TickChannelBus Bus = bus ?? throw new ArgumentNullException(nameof(bus));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly ResiliencePipeline _retryPipeline = pipelineProvider.GetPipeline("ws-retry");
    private readonly TimeSpan _idleTimeout = TimeSpan.FromSeconds(15);

    protected abstract Uri Endpoint { get; }
    protected abstract ExchangeSource Source { get; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _retryPipeline.ExecuteAsync(async ct =>
        {
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
        }
    }

    private async Task ReadPipeAsync(PipeReader reader, CancellationToken ct)
    {
        const int MaxMessageSize = 1024 * 1024;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(ct);
                var buffer = result.Buffer;

                if (buffer.Length > MaxMessageSize)
                {
                    _logger.LogCritical("[SECURITY] Poison pill detected! Buffer size {Length} bytes exceeds limit. Aborting stream.", buffer.Length);
                    throw new InvalidDataException("Poison pill detected. Stream state is compromised.");
                }

                var consumed = buffer.Start;
                var examined = buffer.End;

                while (TryParseJsonMessage(ref buffer, out var message, out var nextPosition))
                {
                    await ProcessMessageAsync(message, ct);
                    consumed = nextPosition;
                    buffer = buffer.Slice(nextPosition);
                }

                reader.AdvanceTo(consumed, examined);
                if (result.IsCompleted) break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading pipe data. Forcing reconnect.");
            throw;
        }
        finally { await reader.CompleteAsync(); }
    }

    private static bool TryParseJsonMessage(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> message, out SequencePosition nextPosition)
    {
        message = default;
        nextPosition = default;

        var jsonReader = new Utf8JsonReader(buffer, isFinalBlock: false, state: default);
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
            throw;
        }
        return false;
    }

    private async ValueTask ProcessMessageAsync(ReadOnlySequence<byte> buffer, CancellationToken ct)
    {
        try
        {
            var jsonReader = new Utf8JsonReader(buffer);
            var parsedTick = ParseTick(ref jsonReader);

            if (parsedTick != null)
            {
                await Bus.PublishAsync(parsedTick.Value, ct);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Malformed JSON packet ignored from {Source}", Source);
        }
    }

    protected virtual Task OnConnectedAsync(ClientWebSocket webSocket, CancellationToken ct) => Task.CompletedTask;
    protected abstract Tick? ParseTick(ref Utf8JsonReader reader);
}

using System.Buffers;
using System.Buffers.Text;
using System.Net.WebSockets;
using System.Text.Json;
using Aggregator.Simulator;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(8081);
    options.ListenLocalhost(8082);
});

builder.Services.Configure<ChaosOptions>(builder.Configuration.GetSection("ChaosOptions"));

var app = builder.Build();
app.UseWebSockets();

app.Map("/binance", async (HttpContext context, IOptions<ChaosOptions> options, ILoggerFactory loggerFactory) =>
{
    if (!context.WebSockets.IsWebSocketRequest) return Results.BadRequest();

    var logger = loggerFactory.CreateLogger("BinanceSimulator");
    using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
    await RunExchangeSimulationAsync(webSocket, "Binance", options.Value, logger, context.RequestAborted);
    return Results.Empty;
});

app.Map("/coinbase", async (HttpContext context, IOptions<ChaosOptions> options, ILoggerFactory loggerFactory) =>
{
    if (!context.WebSockets.IsWebSocketRequest) return Results.BadRequest();

    var logger = loggerFactory.CreateLogger("CoinbaseSimulator");
    using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
    await RunExchangeSimulationAsync(webSocket, "Coinbase", options.Value, logger, context.RequestAborted);
    return Results.Empty;
});

app.Run();

async Task RunExchangeSimulationAsync(WebSocket ws, string exchange, ChaosOptions chaos, ILogger logger, CancellationToken ct)
{
    logger.LogInformation("Starting zero-allocation highload simulator for {Exchange}.", exchange);

    var bufferWriter = new ArrayBufferWriter<byte>(512);
    ulong messageCounter = 0;
    var random = new Random(Guid.NewGuid().GetHashCode());

    string[] tickers = exchange == "Binance"
        ? ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
        : ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"];

    ReadOnlyMemory<byte> corruptedBytes = "{\"e\":\"trade\",\"E\":1672531199000,\"s\":\"BTCUSDT\","u8.ToArray();
    Span<byte> numberSpan = stackalloc byte[32];

    try
    {
        while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            messageCounter++;

            if (messageCounter % (ulong)chaos.HalfOpenInterval == 0)
            {
                logger.LogWarning("[CHAOS] {Exchange} entering Half-Open state for {Sec}s...", exchange, chaos.HalfOpenDurationSeconds);
                await Task.Delay(TimeSpan.FromSeconds(chaos.HalfOpenDurationSeconds), ct);
                continue;
            }

            if (messageCounter % (ulong)chaos.ForcedDropInterval == 0)
            {
                logger.LogCritical("[CHAOS] {Exchange} drops connection pipe immediately!", exchange);
                break;
            }

            if (messageCounter % (ulong)chaos.CorruptedJsonInterval == 0)
            {
                logger.LogWarning("[CHAOS] {Exchange} sending corrupted payload tail.", exchange);
                await ws.SendAsync(corruptedBytes, WebSocketMessageType.Text, true, ct);
                await Task.Delay(10, ct);
                continue;
            }

            var ticker = tickers[random.Next(tickers.Length)];

            decimal price = 60000.50m + ((decimal)random.NextDouble() * 1000m);
            decimal volume = (decimal)random.NextDouble() * 5m;

            bufferWriter.Clear();

            using (var jsonWriter = new Utf8JsonWriter(bufferWriter))
            {
                jsonWriter.WriteStartObject();

                if (exchange == "Binance")
                {
                    jsonWriter.WriteString("e"u8, "trade");
                    jsonWriter.WriteNumber("E"u8, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    jsonWriter.WriteString("s"u8, ticker);

                    Utf8Formatter.TryFormat(price, numberSpan, out int priceBytesWritten, new StandardFormat('F', 2));
                    jsonWriter.WriteString("p"u8, numberSpan.Slice(0, priceBytesWritten));

                    Utf8Formatter.TryFormat(volume, numberSpan, out int volBytesWritten, new StandardFormat('F', 4));
                    jsonWriter.WriteString("q"u8, numberSpan.Slice(0, volBytesWritten));
                }
                else
                {
                    jsonWriter.WriteString("type"u8, "ticker");
                    jsonWriter.WriteString("product_id"u8, ticker);
                    jsonWriter.WriteNumber("price"u8, price);
                    jsonWriter.WriteNumber("volume_24h"u8, volume);
                    jsonWriter.WriteString("time"u8, DateTimeOffset.UtcNow);
                }

                jsonWriter.WriteEndObject();
            }

            await ws.SendAsync(bufferWriter.WrittenMemory, WebSocketMessageType.Text, true, ct);

            await Task.Delay(1, ct);
        }
    }
    catch (OperationCanceledException) { }
    catch (Exception ex)
    {
        logger.LogError(ex, "Exception in simulation loop for {Exchange}", exchange);
    }
    finally
    {
        logger.LogWarning("Simulation worker stopped for {Exchange}.", exchange);
    }
}

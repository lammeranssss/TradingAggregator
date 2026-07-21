using System.IO;
using System.Net.Sockets;
using System.Net.WebSockets;
using Aggregator.Core.Interfaces;
using Aggregator.Core.Services;
using Aggregator.Infrastructure.Data;
using Aggregator.Infrastructure.Exchanges;
using Aggregator.Infrastructure.Workers;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;
using Polly;
using Polly.Retry;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ExchangeOptions>(builder.Configuration.GetSection("Exchanges"));

builder.Services.Configure<HostOptions>(opts =>
{
    opts.ShutdownTimeout = TimeSpan.FromSeconds(15);
});

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
  ?? throw new InvalidOperationException("CRITICAL CONFIGURATION ERROR: Database ConnectionString 'DefaultConnection' is missing.");

builder.Services.AddNpgsqlDataSource(connectionString);

builder.Services.AddResiliencePipeline("db-retry", (pipelineBuilder, context) =>
{
    pipelineBuilder.AddRetry(new RetryStrategyOptions
    {
        ShouldHandle = new PredicateBuilder()
            .Handle<TimeoutException>()
            .Handle<NpgsqlException>(ex => ex.IsTransient),
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromSeconds(1),
        MaxDelay = TimeSpan.FromSeconds(10),
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        OnRetry = args =>
        {
            var logger = context.ServiceProvider.GetRequiredService<ILogger<TickRepository>>();
            logger.LogWarning(
              args.Outcome.Exception,
              "Database save failed (Transient). Retrying in {Delay}ms. Attempt {RetryCount} of 3",
              args.RetryDelay.TotalMilliseconds,
              args.AttemptNumber + 1);
            return default;
        }
    });
});

builder.Services.AddResiliencePipeline("ws-retry", (pipelineBuilder, context) =>
{
    pipelineBuilder.AddRetry(new RetryStrategyOptions
    {
        ShouldHandle = new PredicateBuilder()
            .Handle<WebSocketException>()
            .Handle<IOException>()
            .Handle<SocketException>()
            .Handle<InvalidDataException>(),
        MaxRetryAttempts = int.MaxValue,
        Delay = TimeSpan.FromSeconds(2),
        MaxDelay = TimeSpan.FromSeconds(30),
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        OnRetry = args =>
        {
            var logger = context.ServiceProvider.GetRequiredService<ILogger<BaseWebSocketAdapter>>();
            logger.LogWarning(
              args.Outcome.Exception,
              "WebSocket connection lost. Reconnecting in {Delay}ms. Total reconnections attempted: {Attempt}",
              args.RetryDelay.TotalMilliseconds,
              args.AttemptNumber + 1);
            return default;
        }
    });
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<TickChannelBus>();
builder.Services.AddSingleton<TickerMapper>();

builder.Services.AddSingleton<IDeduplicator>(sp =>
{
    var timeProvider = sp.GetRequiredService<TimeProvider>();
    var logger = sp.GetRequiredService<ILogger<LockFreeDeduplicator>>();
    return new LockFreeDeduplicator(timeProvider, logger, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
});

builder.Services.AddSingleton<ITickRepository, TickRepository>();

builder.Services.AddHostedService<BatchProcessorWorker>();
builder.Services.AddHostedService<ChannelMetricsReporter>();
builder.Services.AddHostedService<BinanceWebSocketAdapter>();
builder.Services.AddHostedService<CoinbaseWebSocketAdapter>();

builder.Services.AddHealthChecks()
  .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"])
  .AddNpgSql(
    connectionString: connectionString,
    name: "PostgreSQL_Cluster",
    tags: ["ready"],
    timeout: TimeSpan.FromSeconds(3));

var app = builder.Build();

app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = r => r.Tags.Contains("live")
});

app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = r => r.Tags.Contains("ready")
});

app.MapMetrics("/metrics");

app.Run();

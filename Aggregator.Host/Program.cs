using Aggregator.Core.Interfaces;
using Aggregator.Core.Services;
using Aggregator.Infrastructure.Data;
using Aggregator.Infrastructure.Workers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Polly;
using Polly.Retry;

var builder = WebApplication.CreateBuilder(args);


var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException("CRITICAL: DB ConnectionString 'DefaultConnection' is missing.");

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
        BackoffType = DelayBackoffType.Exponential,
        UseJitter = true,
        OnRetry = args =>
        {
            var logger = context.ServiceProvider.GetRequiredService<ILogger<TickRepository>>();
            logger.LogWarning(
                args.Outcome.Exception,
                "DB save failed (Transient). Retrying in {Delay}ms. Attempt {RetryCount}",
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


builder.Services.AddHealthChecks()
    .AddNpgSql(connectionString, name: "PostgreSQL", tags: new[] { "db", "ready" });

var app = builder.Build();

app.MapHealthChecks("/health");

app.Run();

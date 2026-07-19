using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Polly;
using Polly.Registry;

namespace Aggregator.Infrastructure.Data;

public class TickRepository : ITickRepository
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<TickRepository> _logger;
    private readonly ResiliencePipeline _retryPipeline;

    public TickRepository(
        NpgsqlDataSource dataSource,
        ILogger<TickRepository> logger,
        ResiliencePipelineProvider<string> pipelineProvider)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _logger = logger;
        _retryPipeline = pipelineProvider.GetPipeline("db-retry");
    }

    public async Task SaveBatchAsync(IReadOnlyCollection<Tick> ticks, CancellationToken cancellationToken)
    {
        if (ticks.Count == 0) return;

        await _retryPipeline.ExecuteAsync(async ct =>
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);

            await using var writer = await connection.BeginBinaryImportAsync(
                "COPY Ticks (Ticker, Price, Volume, TimestampMs, SourceId) FROM STDIN (FORMAT BINARY)", ct);

            foreach (var tick in ticks)
            {
                await writer.StartRowAsync(ct);
                await writer.WriteAsync(tick.Ticker, NpgsqlDbType.Text, ct);
                await writer.WriteAsync(tick.Price, NpgsqlDbType.Numeric, ct);
                await writer.WriteAsync(tick.Volume, NpgsqlDbType.Numeric, ct);
                await writer.WriteAsync(tick.TimestampMs, NpgsqlDbType.Bigint, ct);
                await writer.WriteAsync((short)tick.Source, NpgsqlDbType.Smallint, ct);
            }

            await writer.CompleteAsync(ct);

            _logger.LogDebug("Successfully saved {Count} ticks to DB.", ticks.Count);

        }, cancellationToken);
    }
}

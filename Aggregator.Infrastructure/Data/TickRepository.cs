using Aggregator.Core.Interfaces;
using Aggregator.Core.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Polly;
using Polly.Registry;

namespace Aggregator.Infrastructure.Data;

public class TickRepository(
    NpgsqlDataSource dataSource,
    ILogger<TickRepository> logger,
    ResiliencePipelineProvider<string> pipelineProvider) : ITickRepository
{
    private readonly NpgsqlDataSource _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    private readonly ILogger<TickRepository> _logger = logger;
    private readonly ResiliencePipeline _retryPipeline = pipelineProvider.GetPipeline("db-retry");

    public async Task SaveBatchAsync(IReadOnlyCollection<Tick> ticks, CancellationToken cancellationToken)
    {
        if (ticks.Count == 0) return;

        await _retryPipeline.ExecuteAsync(async ct =>
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);

            const string createTempTableSql = @"
    CREATE TEMP TABLE IF NOT EXISTS temp_ticks (
        Ticker VARCHAR(50),
        Price NUMERIC(18, 8),
        Volume NUMERIC(18, 8),
        TimestampMs BIGINT,
        SourceId SMALLINT
    ) ON COMMIT DELETE ROWS;";

            await using (var cmd = new NpgsqlCommand(createTempTableSql, connection, transaction))
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await using (var writer = await connection.BeginBinaryImportAsync(
                "COPY temp_ticks (Ticker, Price, Volume, TimestampMs, SourceId) FROM STDIN (FORMAT BINARY)", ct))
            {
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
            }

            const string mergeSql = @"
                INSERT INTO Ticks (Ticker, Price, Volume, TimestampMs, SourceId)
                SELECT Ticker, Price, Volume, TimestampMs, SourceId FROM temp_ticks
                ON CONFLICT (Ticker, SourceId, TimestampMs) DO NOTHING;";

            await using (var cmd = new NpgsqlCommand(mergeSql, connection, transaction))
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);

            _logger.LogDebug("Successfully saved {Count} ticks to DB using Bulk Upsert.", ticks.Count);

        }, cancellationToken);
    }
}

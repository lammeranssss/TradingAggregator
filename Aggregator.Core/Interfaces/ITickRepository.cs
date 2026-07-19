using Aggregator.Core.Models;

namespace Aggregator.Core.Interfaces;

public interface ITickRepository
{
    Task SaveBatchAsync(IReadOnlyCollection<Tick> ticks, CancellationToken cancellationToken);
}

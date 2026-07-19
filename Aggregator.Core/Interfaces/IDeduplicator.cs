using Aggregator.Core.Models;

namespace Aggregator.Core.Interfaces;

public interface IDeduplicator
{
    bool IsUnique(in Tick tick, int tickerId);
}

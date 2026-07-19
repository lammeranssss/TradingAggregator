namespace Aggregator.Simulator;

public class ChaosOptions
{
    public int HalfOpenInterval { get; set; } = 300; // Каждое N-е сообщение уходит в тишину
    public int HalfOpenDurationSeconds { get; set; } = 20;
    public int ForcedDropInterval { get; set; } = 750; // Каждое N-е сообщение рвет TCP
    public int CorruptedJsonInterval { get; set; } = 130; // Каждое N-е сообщение ломает синтаксис
}
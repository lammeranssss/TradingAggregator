namespace Aggregator.Infrastructure.Exchanges;

public class ExchangeOptions
{
    public string BinanceUrl { get; set; } = "ws://localhost:8081/binance";
    public string CoinbaseUrl { get; set; } = "ws://localhost:8082/coinbase";
}

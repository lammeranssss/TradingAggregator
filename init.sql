
CREATE TABLE IF NOT EXISTS Ticks (
    ticker VARCHAR(32) NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(18, 8) NOT NULL,
    timestamp BIGINT NOT NULL,
    exchange SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_timestamp_brin ON Ticks USING brin (timestamp);

CREATE TABLE IF NOT EXISTS Ticks (
    Id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    Ticker VARCHAR(50) NOT NULL,
    Price NUMERIC(18, 8) NOT NULL,
    Volume NUMERIC(18, 8) NOT NULL,
    TimestampMs BIGINT NOT NULL,
    SourceId SMALLINT NOT NULL
);

ALTER TABLE Ticks 
ADD CONSTRAINT uq_ticks_identity UNIQUE (Ticker, SourceId, TimestampMs);

CREATE INDEX idx_ticks_timestamp_brin ON Ticks USING brin (TimestampMs);
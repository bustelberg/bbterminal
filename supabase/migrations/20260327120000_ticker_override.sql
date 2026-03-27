CREATE TABLE IF NOT EXISTS ticker_override (
  ticker           VARCHAR PRIMARY KEY,
  primary_ticker   VARCHAR NOT NULL,
  primary_exchange VARCHAR NOT NULL,
  source           VARCHAR NOT NULL DEFAULT 'openfigi',
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

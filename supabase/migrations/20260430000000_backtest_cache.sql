-- Replay cache for /api/momentum/backtest. The same BacktestRequest run twice
-- in a row (same config + same UTC day) returns the stored payload instead of
-- re-loading prices, re-building indices, and re-computing signals. The
-- `data_date` column scopes a cache entry to a single calendar day; once it
-- rolls over (after the daily price refresh) the next replay misses and a
-- fresh result is cached. Setting force_recompute=true on the request bypasses
-- the cache entirely and overwrites whatever was stored.

CREATE SEQUENCE IF NOT EXISTS backtest_cache_id_seq;

CREATE TABLE IF NOT EXISTS backtest_cache (
  cache_id        INTEGER PRIMARY KEY DEFAULT nextval('backtest_cache_id_seq'),
  strategy_hash   TEXT NOT NULL,
  data_date       DATE NOT NULL DEFAULT CURRENT_DATE,
  config          JSONB NOT NULL,
  payload         JSONB NOT NULL,             -- {result: BacktestResult.to_dict(), universe: [...]}
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_cache_hash_date
  ON backtest_cache (strategy_hash, data_date);

CREATE INDEX IF NOT EXISTS idx_backtest_cache_created_at_desc
  ON backtest_cache (created_at DESC);

NOTIFY pgrst, 'reload schema';

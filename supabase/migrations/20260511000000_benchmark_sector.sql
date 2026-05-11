-- Optional GICS sector tag on benchmark rows. When set, the benchmark is
-- treated as the "representative ETF" for that sector by the new
-- selection_mode="sector_etf" momentum strategy. Non-sector benchmarks
-- (SPY, ACWI, etc.) leave this NULL and are unaffected.

ALTER TABLE benchmark
  ADD COLUMN IF NOT EXISTS sector TEXT;

-- One benchmark per sector — picking two ETFs for the same sector would be
-- ambiguous for the "Sector ETF" backtest mode. Partial index so the
-- uniqueness only applies to rows that actually tag a sector.
CREATE UNIQUE INDEX IF NOT EXISTS benchmark_sector_unique
  ON benchmark (sector)
  WHERE sector IS NOT NULL;

NOTIFY pgrst, 'reload schema';

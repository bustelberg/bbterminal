-- Per-day persistence of current-picks daily holdings, keyed by strategy.
-- Lets the UI cache picks across re-clicks: same strategy → load from DB,
-- no recompute. Past months accumulate days from prior months' computes —
-- they are read-only (we never compute new days for closed months).
CREATE TABLE IF NOT EXISTS current_picks_day (
  strategy_hash         TEXT NOT NULL,
  target_date           DATE NOT NULL,
  as_of_date            DATE NOT NULL,         -- first of the month for this day
  holdings              JSONB NOT NULL,        -- list of MonthlyHolding-shaped objects
  portfolio_return_pct  NUMERIC,                -- chain-linked cumulative MTD through this day
  next_day_return_pct   NUMERIC,                -- 1-day forward return of this day's portfolio (NULL on the latest day in the panel)
  turnover_abs          INTEGER,
  turnover_pct          NUMERIC,
  config                JSONB NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (strategy_hash, target_date)
);

-- Idempotent for environments where the table predates the column.
ALTER TABLE current_picks_day
  ADD COLUMN IF NOT EXISTS next_day_return_pct NUMERIC;

CREATE INDEX IF NOT EXISTS idx_current_picks_day_hash_month
  ON current_picks_day (strategy_hash, as_of_date);

-- Tag snapshots with the strategy hash so the cache lookup can find a
-- pre-computed snapshot for "this strategy this month" instantly.
ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS strategy_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_current_picks_snapshot_hash_asof
  ON current_picks_snapshot (strategy_hash, as_of_date DESC);

NOTIFY pgrst, 'reload schema';

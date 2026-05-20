-- `company.delisted_at` — first time we detected the listing no longer
-- has fetchable price/volume data. Two upstream signals fold into this:
--   1. GuruFocus 403 "Delisted stocks are available for Professional plan"
--      — the listing was once there, is gone now.
--   2. GuruFocus 404 "Stock not found" — the symbol doesn't (and can't)
--      resolve. Effectively the same outcome for us: ignore in backtests.
--
-- Once a company is marked delisted, the price phase short-circuits
-- (no more API calls) and the /backtest audit drops it from the
-- "missing price data" warning. The /companies UI surfaces it as a
-- DELISTED badge.
--
-- NULL = not delisted (still tradeable). Non-NULL = the timestamp when
-- the pipeline first flagged it.

ALTER TABLE company
  ADD COLUMN IF NOT EXISTS delisted_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS company_delisted_at_idx
  ON company (delisted_at)
  WHERE delisted_at IS NOT NULL;

NOTIFY pgrst, 'reload schema';

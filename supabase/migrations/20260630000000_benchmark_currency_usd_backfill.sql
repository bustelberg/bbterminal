-- Backfill benchmark.currency to the ISO code 'USD'.
--
-- Every `benchmark` row is a US-listed, USD-denominated ETF/index, but the
-- `currency` column (added in 20260629010000) was left NULL on rows created
-- before that column existed, and stored as the bare symbol '$' for some
-- GuruFocus auto-detects. BOTH break the /schedule current-portfolio re-pricer's
-- USD→EUR conversion (`routers/_schedule_snapshots.py`): the fx_rate table keys
-- on the ISO code 'USD', so a NULL currency is treated as EUR (no conversion —
-- EUR mark = local price, FX 1:1) and a '$' currency misses the fx lookup
-- entirely (EUR marks left blank, e.g. the IAU gold ETF). Normalize to 'USD' so
-- every ETF/bond sleeve converts at the real rate. Idempotent.
UPDATE public.benchmark SET currency = 'USD' WHERE currency IS NULL OR currency = '$';

NOTIFY pgrst, 'reload schema';

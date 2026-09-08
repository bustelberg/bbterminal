-- ⚠⚠ WHEN WE LAST ASKED THE VENDOR, WHICH IS A DIFFERENT FACT FROM THE NEWEST CLOSE WE HOLD —
-- and without it the Analyse modal's benchmark tile could not be told from a stale one.
--
-- `provenanceFreshness` has one rule for this and it is the right one: our copy is CURRENT when it
-- matches the source, so "read today, and the source has published nothing since" is not staleness.
-- The tile had no fetch time to give it, so it fell back to comparing the as-of date against the
-- CALENDAR — and GuruFocus runs a day or two behind on index ETFs, so the badge was permanently
-- amber over a figure nothing on the page could improve. That is the "alarm you cannot clear"
-- failure `provenanceFreshness` was rewritten to avoid, arriving through the one input it was not
-- given.
--
-- Measured 2026-09-08: newest ACWI and SPY close held = 2026-09-04, today Tuesday 2026-09-08, and
-- a forced refresh returned `refreshed: true` with the same 09-04 — the vendor had nothing newer.
-- Pressing Refresh could never turn that badge blue, however many times somebody tried.
--
-- ⚠ ON `benchmark`, NOT `benchmark_price`. It is a property of the SERIES ("when did we last ask
-- about this ticker"), not of any one bar; hanging it off the newest row would make it vanish the
-- moment a newer bar arrived from somewhere else.
--
-- ⚠ NULLABLE, AND NULL MEANS "NEVER ASKED THROUGH THIS PATH". Backfilling it to `created_at` or to
-- now() would claim a fetch that did not happen, and the reader would see a current badge over a
-- series nobody has refreshed. Absent falls back to the old calendar comparison, which is exactly
-- the previous behaviour.

ALTER TABLE benchmark ADD COLUMN IF NOT EXISTS proxy_fetched_at timestamptz;

COMMENT ON COLUMN benchmark.proxy_fetched_at IS
  'When routers/_benchmark_etf.ensure_fresh last asked the vendor for this ticker. Feeds the '
  'Analyse modal''s benchmark provenance card: read today = current, whatever date the newest '
  'close carries. NULL = never asked through that path.';

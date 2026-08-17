-- When we last ASKED GuruFocus for this company's ESTIMATES and INDICATORS feeds.
--
-- ⚠⚠ THE SISTER OF `financials_fetched_at`, AND ITS ABSENCE WAS COSTING AN HOUR PER PRESS. The
-- smart refresh decides per feed with "missing OR stale", where stale is `max(recorded_at)` on the
-- feed's sentinel row — i.e. WHEN A ROW LAST APPEARED. For a company GuruFocus has nothing for,
-- no row ever appears, so both halves stay true for ever and the feed is re-asked on every single
-- press. Measured on ACWI, 2026-08-17:
--
--     calls in one smart press                4,326   (1.80 h at the global 1.5s gate)
--       estimates                             1,464   of which 1,125 (77%) hold NOTHING
--       indicators                            1,551   of which 1,267 (82%) hold NOTHING
--
-- Most of those are companies no analyst covers — a permanent fact about the company, asked again
-- every time, for ever. The statements leg has no such problem because `period_due` reads the
-- fiscal calendar and goes quiet once a filing lands; estimates and indicators have no equivalent
-- boundary, so the only honest signal is when WE last asked.
--
-- ⚠ AND `recorded_at` CANNOT BE MADE TO SERVE. `metric_data.recorded_at` defaults to
-- CURRENT_TIMESTAMP on INSERT and there is no update trigger, so an upsert that rewrites an
-- existing row leaves it untouched — it has always meant "when this row first appeared", never
-- "when we last looked". (That is also why the row-diffing added the same day changed nothing
-- here: an unchanged row was never advancing it either.)
--
-- ⚠ SET ON EVERY ATTEMPT, INCLUDING ONE THAT LOADS NOTHING — the same rule as the financials stamp.
-- "We asked" is the fact being recorded; gating it on rows-loaded would leave it NULL for precisely
-- the companies whose emptiness is the thing worth remembering.
--
-- ⚠ NULLABLE, AND NULL MEANS "NEVER ASKED" — a real state, not a missing value. Nothing is
-- backfilled: dating the existing rows `now()` would claim a check we never made, and dating them
-- old would claim a staleness we never measured. Every company is due once after this lands, which
-- is the honest starting position and is exactly one press.
alter table public.company
  add column if not exists estimates_fetched_at timestamptz,
  add column if not exists indicators_fetched_at timestamptz;

comment on column public.company.estimates_fetched_at is
  'UTC time of the last GuruFocus analyst-estimates fetch ATTEMPT (not the last successful write). '
  'NULL = never attempted. Read by the smart refresh so a company analysts do not cover is not '
  're-asked on every press.';

comment on column public.company.indicators_fetched_at is
  'UTC time of the last GuruFocus indicators (forward P/E) fetch ATTEMPT (not the last successful '
  'write). NULL = never attempted. Same purpose as estimates_fetched_at.';

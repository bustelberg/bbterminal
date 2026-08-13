-- When we last ASKED GuruFocus for this company's financials — not when it last filed.
--
-- ⚠⚠ IT EXISTS BECAUSE AN EMPTY CELL HAS TWO OPPOSITE MEANINGS AND NOTHING COULD TELL THEM APART.
-- A blank period in a fundamentals table is either "we asked and GuruFocus publishes nothing" or
-- "we have never asked about a period this recent" — the first is a fact about the company, the
-- second is a fact about us, and they were rendered identically. `is_cache_fresh` cannot answer it:
-- it infers freshness from the DATA's own dates, so a company that stopped filing looks exactly
-- like one we stopped fetching.
--
-- ⚠ SET ON EVERY ATTEMPT, INCLUDING ONE THAT LOADS NOTHING. "We asked" is the fact being recorded;
-- gating it on rows-loaded would leave the timestamp NULL for precisely the companies whose blanks
-- most need explaining.
--
-- ⚠ NULLABLE, AND NULL MEANS "NEVER ASKED" — a real state, not a missing value. Backfilling it with
-- `now()` for rows that already have data would claim we checked them today; backfilling with the
-- oldest possible date would claim a staleness we did not measure. Every existing row stays NULL
-- until the next fetch, which is the honest starting position.
alter table public.company
  add column if not exists financials_fetched_at timestamptz;

comment on column public.company.financials_fetched_at is
  'UTC time of the last GuruFocus financials fetch attempt (not the last successful write). '
  'NULL = never attempted. Distinguishes "no data published" from "not tried yet" in the '
  'per-period fundamentals tables.';

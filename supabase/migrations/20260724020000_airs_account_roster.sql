-- WHICH AIRS accounts exist RIGHT NOW, as of the last Front-Office discovery.
--
-- THE BUG THIS FIXES
--   `list_accounts()` was built from `airs_performance`, which the daily scrape APPENDS to. Rows
--   are never removed, so an account AIRS has since deactivated stays in the list for ever,
--   wearing whatever snapshot it had when it was last seen. Measured 2026-07-24: AIRS's
--   Front-Office list returned 44 portfolios; our table showed 50. The six extras —
--   TOPS_AZTS_L, TOPS_MOTS_L and WTS test 1-4 DYN — had simply stopped being scraped, and
--   nothing in the data said so.
--
--   Worse than a stale row: TOPS_NEU_BEH_DYN's holdings were frozen at 2026-07-16, the last scan
--   before ISIN capture, so its Class column was read-only and 9 of its 12 rows sat Unclassified.
--   Every one of those looked like a bug in the feature rather than an account that no longer
--   exists.
--
-- ⚠ THE SCRAPE IS THE AUTHORITY ON EXISTENCE; THE PERFORMANCE TABLE IS NOT. `airs_performance`
--   answers "what did this book make", which stays true after the book is gone. Only the
--   discovery pass answers "does AIRS still list it", and until now that answer was thrown away
--   the moment it had been used to drive the scrape.
--
-- ⚠ A ROW HERE IS AN OBSERVATION, NOT A DELETION. History is untouched: an account that returns
--   comes back on the next discovery with its figures intact, and `last_seen_at` records exactly
--   when it dropped out — which a delete could never tell you.
CREATE TABLE IF NOT EXISTS public.airs_account_roster (
    portefeuille  text PRIMARY KEY,
    -- Stamped with ONE value for the whole discovery batch, so "the live set" is exactly
    -- `last_seen_at = max(last_seen_at)`. A per-row now() would make that comparison a race.
    last_seen_at  timestamptz NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS airs_account_roster_last_seen
    ON public.airs_account_roster (last_seen_at DESC);

ALTER TABLE public.airs_account_roster ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_account_roster_deny_all ON public.airs_account_roster;
CREATE POLICY airs_account_roster_deny_all ON public.airs_account_roster FOR ALL USING (false);

REVOKE ALL ON public.airs_account_roster FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_account_roster TO service_role;

NOTIFY pgrst, 'reload schema';

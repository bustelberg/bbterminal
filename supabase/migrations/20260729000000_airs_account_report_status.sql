-- WHICH of an account's four AIRS reports the last refresh actually retrieved.
--
-- THE PROBLEM
--   The daily scrape fetches FOUR reports per account — Rendement (ATT) -> airs_performance,
--   Vermogensoverzicht (VOLK) -> airs_holding, Mutaties (MUT) -> airs_mutatie, Model (MODEL) ->
--   airs_model_weight. Measured 2026-07-29: ATT 44/44, VOLK 31/44. So thirteen accounts appear in
--   the portfolios list carrying a return, a holdings count from some earlier snapshot, and no
--   indication that today's valuation never arrived.
--
--   That is worse than an empty row. Every figure shown is real, just assembled from reports of
--   different ages, and nothing on screen says which. An account whose VOLK failed shows last
--   week's holdings against this week's return.
--
-- ⚠ RETRIEVED IS NOT THE SAME AS NON-EMPTY, AND THIS IS THE WHOLE REASON IT IS RECORDED RATHER
--   THAN DERIVED. A book with no transactions this year returns a perfectly valid, EMPTY Mutaties
--   report. Inferring completeness from `count(*) > 0` in each table would hide exactly the quiet,
--   healthy accounts — and would call an account complete on the strength of rows a refresh three
--   weeks ago wrote. Only the fetch knows whether the fetch worked, so the fetch writes it down.
--
-- ⚠ ONE ROW PER ACCOUNT, OVERWRITTEN — NOT A LOG. The question is "is this account whole RIGHT
--   NOW", so history would be a second thing to reason about. `airs_performance` already keeps
--   the history that matters.
--
-- ⚠ NULL MEANS "NEVER MEASURED", NOT "INCOMPLETE". Accounts that existed before this column did
--   have no record, and treating that as a failure would empty the portfolios page on deploy
--   until a full refresh happens to run. Same rule as `_live_accounts` returning None: absence of
--   evidence is shown, never asserted. `_complete_accounts` fails open on it.
ALTER TABLE public.airs_account_roster
    ADD COLUMN IF NOT EXISTS reports_ok  text[],
    ADD COLUMN IF NOT EXISTS reports_at  timestamptz;

COMMENT ON COLUMN public.airs_account_roster.reports_ok IS
    'Report codes the last refresh RETRIEVED for this account: att, volk, mut, model. An empty '
    'array means every one failed; NULL means no refresh has recorded an outcome yet.';
COMMENT ON COLUMN public.airs_account_roster.reports_at IS
    'When that outcome was recorded. Compared against the newest value across the table so a '
    'half-finished refresh cannot retire accounts it never reached.';

GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_account_roster TO service_role;

NOTIFY pgrst, 'reload schema';

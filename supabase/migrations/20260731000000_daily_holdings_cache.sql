-- Cached SELECTIONS for the retrospective "Daily holdings" walk (/schedule).
--
-- WHAT IT IS FOR
--   Re-selecting a strategy's basket for every trading day of a two-month window means running
--   the signal panel + score/select ~42 times over a universe of up to a few thousand names. Run
--   it again tomorrow and 41 of those 42 answers are the ones already computed. This stores the
--   expensive part so only genuinely new days are computed.
--
-- ⚠ IT IS NOT `current_picks_day`, AND IT MUST NEVER BECOME IT.
--   `current_picks_day` is the RECORD of what the pipeline DECIDED on each trading day, using the
--   data available AT THAT MOMENT. This table holds a RECALCULATION of the same days on the data
--   we hold NOW. They are different facts and they legitimately disagree: GuruFocus publishes some
--   closes days late and `ingest/prices.py` writes them with their true (earlier) target_date, so
--   `metric_data` is append-only in `recorded_at` but NOT in `target_date`.
--
--   Both are keyed `(strategy_hash, target_date)`. Merged into one table the upsert would REPLACE
--   the decision with the recalculation — silently, irreversibly, and precisely destroying the
--   comparison the feature exists to show. Two tables is the guarantee, not a convention.
--
-- ⚠ IT STORES THE SELECTION ONLY — NOT THE RETURNS, NOT THE TURNOVER, NOT THE CUMULATIVE.
--   Those are properties of the WINDOW, not of the day: turnover is measured against the previous
--   day in the window and the cumulative return is chain-linked from the window's first day, so a
--   value cached under a two-month window is wrong the moment a three-month one is asked for.
--   They are cheap (price lookups and arithmetic) and are recomputed every run over the full
--   merged day list. What is cached is the part that is both expensive and window-independent:
--   WHICH COMPANIES the strategy would have held, and their scores.
--
-- ⚠ `computed_at` IS LOAD-BEARING, NOT AUDIT TRIM. A day's selection is a function of the prices
--   known before it, and those can still change after the fact — so the reader gets to see how old
--   an answer is, and the backend deliberately refuses to serve the most recent few days from here
--   (see DAILY_HOLDINGS_TAIL_DAYS): that is exactly where a late close lands.
CREATE TABLE IF NOT EXISTS public.daily_holdings_cache (
    strategy_hash text        NOT NULL,
    target_date   date        NOT NULL,
    -- The selected companies for that day: company_id, ticker, name, sector, score, ranks.
    -- Prices, weights and returns are NOT here; they are re-derived per run.
    holdings      jsonb       NOT NULL,
    computed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_hash, target_date)
);

-- The read is always "every cached day for this strategy in this window".
CREATE INDEX IF NOT EXISTS daily_holdings_cache_lookup
    ON public.daily_holdings_cache (strategy_hash, target_date);

ALTER TABLE public.daily_holdings_cache ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS daily_holdings_cache_deny_all ON public.daily_holdings_cache;
CREATE POLICY daily_holdings_cache_deny_all
    ON public.daily_holdings_cache FOR ALL USING (false);

REVOKE ALL ON public.daily_holdings_cache FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.daily_holdings_cache TO service_role;

NOTIFY pgrst, 'reload schema';

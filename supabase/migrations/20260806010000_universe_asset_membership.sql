-- Universe membership in the ASSET world: which benchmarks an asset belongs to.
--
-- ⚠⚠ NOT `asset_universe_member`, AND THE NAMES ARE FOUR LETTERS APART ON PURPOSE-AVOIDANCE.
--   `asset_universe_member` already exists (migration 20260707020000) and means something else
--   entirely: a saved LIQUIDITY SCREEN over the asset grid, whose `universe_id` references
--   `asset_universe(id)`. This table's `universe_id` references `public.universe(universe_id)` —
--   SP500, ACWI, AEX, Leonteq. Two columns with the same name pointing at different parents is a
--   wrong join nobody would question, so this one is named as the sibling of what it mirrors:
--   `universe_membership` (company world) -> `universe_asset_membership` (asset world). They sort
--   together and neither can be mistaken for the screener.
--
-- WHY A SECOND TABLE RATHER THAN A COLUMN ON `asset_execution`
--   An asset sits in several universes at once (SP500 ∩ ACWI ∩ Leonteq), so a scalar cannot
--   express it and a delimited string would have to be parsed at every call site. This is the
--   plain many-to-many; `asset_grid` aggregates it into a `text[]` for display, which is where the
--   convenience belongs.
--
-- ⚠ NO `target_month`, AND THAT IS THE POINT OF THE PRECEDING MIGRATION. LongEquity was the last
--   time-varying universe; collapsing it to a static union is what lets this be a join table with
--   no time dimension. If a time-varying universe is ever reintroduced, it does NOT belong here —
--   it belongs in `metric_data`, where LongEquity's real per-month history still lives and where
--   `longequity_membership_by_month()` reads it.
--
-- ⚠ KEYED ON `analysis_id`, NOT `execution_id`. An analysis asset can have several executions —
--   the same company listed on several venues, one flagged `is_default`. Membership is a property
--   of the COMPANY, not of the venue you would trade it on, so keying per listing would say
--   "Apple is in the S&P 500" once per exchange it trades on.
--
-- ⚠ THE BRIDGE STAYS A JOIN. Nothing here caches `company_id`. The backfill resolves
--   `company.isin -> asset_execution.isin -> analysis_id` at write time and records only the
--   result; re-running it is how the mapping is corrected, not a stored column that rots. (See
--   `_asset_benchmark`'s "the bridge is a JOIN, never a COLUMN".)

CREATE TABLE IF NOT EXISTS public.universe_asset_membership (
    analysis_id bigint  NOT NULL REFERENCES public.asset_analysis(analysis_id) ON DELETE CASCADE,
    universe_id integer NOT NULL REFERENCES public.universe(universe_id)       ON DELETE CASCADE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (analysis_id, universe_id)
);

-- The read this table exists for: "every asset in universe X" (a benchmark rebuild), and
-- "every universe this asset is in" (the grid's chips). The PK serves the second; this serves
-- the first.
CREATE INDEX IF NOT EXISTS universe_asset_membership_universe_idx
    ON public.universe_asset_membership(universe_id);

-- Same posture as the rest of the asset pipeline: RLS on with a deny-all policy, and the service
-- role granted explicitly.
--
-- ⚠ THE GRANT IS NOT OPTIONAL AND ITS ABSENCE DOES NOT LOOK LIKE A MISSING GRANT. A service key
-- bypasses RLS but NOT table privileges, so a new table without this fails in production as
-- "permission denied 42501" — which reads as an RLS problem and sends you to the policy instead.
ALTER TABLE public.universe_asset_membership ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS universe_asset_membership_deny_all ON public.universe_asset_membership;
CREATE POLICY universe_asset_membership_deny_all
    ON public.universe_asset_membership FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.universe_asset_membership TO service_role;

-- ⚠ WITHOUT THIS, THE TABLE EXISTS AND POSTGREST CANNOT SEE IT. Its schema cache is only rebuilt
-- on reload, so the first call after a migration fails with PGRST205 "Could not find the table …
-- in the schema cache" — which reads as a missing migration rather than a stale cache, and sends
-- you looking in the wrong place. `20260707020000_asset_universe.sql` ends the same way.
NOTIFY pgrst, 'reload schema';

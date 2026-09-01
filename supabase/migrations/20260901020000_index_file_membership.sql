-- ACWI MEMBERSHIP THAT DOES NOT GO THROUGH THE COMPANY WORLD (2026-09-01, on request).
--
-- ⚠⚠ THE BUG: A CONSTITUENT GURUFOCUS DOES NOT SELL US WAS NOT IN ACWI AT ALL, ANYWHERE.
--   `universe_asset_membership` is a VIEW (migration 20260806060000) deriving
--   `universe_membership -> company.isin -> asset_execution.isin`. Membership is therefore only
--   ever as wide as the COMPANY world, and the company world only exists where GuruFocus sells to
--   us — `FEASIBLE_GF_EXCHANGES` has no Toronto, no ASX, no LSE, no Johannesburg. So Constellation
--   Software sat in `asset_execution` as `CA21037X1006 / CSU.TO`, priced and healthy by yfinance,
--   and was not a member of the index it is demonstrably in.
--
--   Measured 2026-09-01 against the committed iShares file (2,270 equities): 132 constituents in
--   exactly that state — Canada 44, Australia 39, United Kingdom 28, plus Samsung Electronics and
--   Exxon Mobil. Royal Bank of Canada, Commonwealth Bank, BHP, Toronto-Dominion, Enbridge, BMO.
--
-- ⚠⚠ THE VIEW STAYS A VIEW, AND THAT RULE IS NOT BENT HERE. 20260806060000 deleted the backfilled
--   table precisely because "a mirror that needs a human to remember to re-run it is a mirror that
--   is wrong", and it was right: the stored table had already drifted by one row. So this does NOT
--   reintroduce a mirror. It adds a SECOND AUTHORED SOURCE and unions it in — membership is still
--   authored in exactly two places, both explicit, and the view still derives and still cannot
--   drift from what it reads.
--
-- ⚠ AUTHORED FROM THE PROVIDER'S OWN FILE, WHICH IS WHY IT IS NAMED FOR THAT. iShares blocks
--   scripted downloads, so `iShares-MSCI-ACWI-ETF_fund.xls` is committed to the repo and is the
--   authority on "what is in ACWI". This table is that file, resolved to assets. A row here means
--   "the index provider lists this instrument", not "we decided it belongs".
--
-- ⚠⚠ RESOLVED BY TICKER + EXCHANGE, NEVER BY NAME. That export carries no ISIN column, and a name
--   join on this data is demonstrably unsafe: `scripts/measure_acwi_asset_gap.py` sized the gap
--   with one and its own output matched BERKSHIRE HATHAWAY CLASS B to Berkshire A, NEWMONT (United
--   States) to the Australian CDI line, and MIZUHO FINANCIAL GROUP (Japan) to MAGELLAN FINANCIAL
--   GROUP (Australia). Ticker plus exchange is a deterministic address — see
--   `index_universe/acwi/yahoo_map.py`, which returns NULL for a venue it cannot place rather than
--   pointing at a plausible wrong listing.
--
-- ⚠ KEYED ON `analysis_id` LIKE THE VIEW IT FEEDS. 62 symbols map to more than one
--   `asset_execution` row — a local line and a US ADR of one company (Novartis CH0012005267 +
--   US66987V1098) — and every one of them resolves to a SINGLE `analysis_id`. Membership is a
--   property of the company, not of the venue, so there is nothing to disambiguate.

CREATE TABLE IF NOT EXISTS public.index_file_membership (
  universe_id   integer     NOT NULL REFERENCES public.universe(universe_id) ON DELETE CASCADE,
  analysis_id   integer     NOT NULL,

  -- Provenance, so a surprising member can be traced to the row that produced it without
  -- re-running the resolver. ⚠ The TICKER and EXCHANGE are the file's, verbatim; `yahoo_symbol` is
  -- what they resolved to and is the thing that was actually matched.
  source        text        NOT NULL DEFAULT 'ishares_acwi',
  ticker        text,
  exchange      text,
  yahoo_symbol  text,

  -- The date INSIDE the provider's file, not when we read it. ⚠ A stale file is a real and
  -- invisible failure here (the committed one is dated 15-Apr-2026), so the date travels with the
  -- rows rather than being inferred from `updated_at`.
  source_as_of  text,
  updated_at    timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (universe_id, analysis_id)
);

CREATE INDEX IF NOT EXISTS index_file_membership_universe_idx
  ON public.index_file_membership (universe_id);

ALTER TABLE public.index_file_membership ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS index_file_membership_deny_all ON public.index_file_membership;
CREATE POLICY index_file_membership_deny_all ON public.index_file_membership FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.index_file_membership TO service_role;

-- ⚠ `CREATE OR REPLACE` KEEPS THE GRANTS AND THE DEPENDENT `asset_grid` VIEW. Dropping and
-- recreating would cascade into `asset_grid` (migration 20260806060000 defines it right after
-- this one) and silently take its permissions with it.
CREATE OR REPLACE VIEW public.universe_asset_membership AS
SELECT DISTINCT
       m.universe_id,
       e.analysis_id
  FROM public.universe_membership m
  JOIN public.company co
    ON co.company_id = m.company_id
   AND co.delisted_at IS NULL
   AND co.out_of_scope_at IS NULL
   AND co.isin IS NOT NULL
  JOIN public.asset_execution e
    ON upper(btrim(e.isin)) = upper(btrim(co.isin))
 WHERE e.analysis_id IS NOT NULL
 UNION
-- The second authored source — see the header. `UNION` (not `UNION ALL`) so a constituent reached
-- both ways appears once, which is the common case: 1,409 of the 1,541 resolved rows are already
-- members through the company bridge.
SELECT f.universe_id,
       f.analysis_id
  FROM public.index_file_membership f;

REVOKE ALL ON public.universe_asset_membership FROM anon, authenticated;
GRANT SELECT ON public.universe_asset_membership TO service_role;

COMMENT ON TABLE public.index_file_membership IS
  'Index membership authored directly from the provider''s own holdings file, resolved to assets '
  'by ticker+exchange rather than through the company world. Exists because a constituent outside '
  'the GuruFocus subscription has no company row and so could never be a member, even when '
  'asset_execution prices it. Unioned into universe_asset_membership.';

NOTIFY pgrst, 'reload schema';

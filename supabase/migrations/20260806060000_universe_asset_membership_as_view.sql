-- Turn `universe_asset_membership` from a BACKFILLED TABLE into a DERIVED VIEW.
--
-- ⚠⚠ THE TABLE HAD NO WRITER, AND IT WAS ALREADY STALE. Nothing populated it except a manual
--   script (`scripts/backfill_asset_membership.py`) — no pipeline hook, no trigger, nothing in
--   the freeze path. Meanwhile it had become load-bearing on two read paths: every benchmark in
--   the Analyse modal (`_asset_benchmark.members`) and the `Benchmarks` chips on
--   `/asset-pipeline`. So an ACWI reconstruction, an S&P update or a new freeze would silently
--   desync the asset-side mirror and the benchmark would compute off an outdated constituent
--   set — no error, no empty cell, just a plausible wrong number.
--
--   This was not hypothetical. Measured hours after the backfill ran: the stored table held
--   7,738 rows against 7,739 derived, the missing one being SMIC in the live Leonteq universe,
--   dropped because the post-merge re-backfill was run for the FROZEN universe and SP500 but not
--   the live one. A mirror that needs a human to remember to re-run it is a mirror that is wrong.
--
-- ⚠ AND IT COSTS NOTHING. The table existed only because a join was assumed to be expensive.
--   Measured on the full 16,613-row grid: 28.0 ms view-backed vs 29.9–33.9 ms table-backed —
--   identical within run-to-run noise. The `asset_grid` output is byte-identical except for the
--   one drifted row, where the view is the correct side.
--
--   So this follows the rule the rest of this schema already states twice: the count is a VIEW,
--   never a column; the bridge is a JOIN, never a column. Membership is authored in
--   `universe_membership`; this is that same fact reached through the ISIN bridge, and it cannot
--   drift because there is nothing to keep in step.
--
-- ⚠ `DISTINCT` IS LOAD-BEARING, TWICE OVER. A company appears once per (universe, target_month),
--   so a monthly universe would otherwise yield one row per month; and several executions can
--   share an `analysis_id`. Both collapse to the one fact being asserted: this asset is in this
--   universe. It also restores what the table's `(analysis_id, universe_id)` PK used to enforce.
--
-- ⚠ THE `delisted_at` / `out_of_scope_at` FILTER IS THE BACKFILL'S, PRESERVED EXACTLY. Dropping
--   it would quietly re-admit constituents the pipeline has retired, changing every benchmark's
--   membership the moment this migration ran.
--
-- ⚠ NO PK MEANS THE READER'S ORDER KEY MUST STILL BE UNIQUE. `_universe_analysis_ids` pages with
--   `.eq(universe_id).order(analysis_id)` — unique WITHIN that filter because of the DISTINCT
--   above, so `.range()` cannot serve a row twice or skip one. Any new reader must filter by
--   universe first or add its own tiebreaker.

-- `asset_grid` depends on this object, so it has to come down first and go back up after.
DROP VIEW IF EXISTS public.asset_grid;
DROP TABLE IF EXISTS public.universe_asset_membership;

CREATE VIEW public.universe_asset_membership AS
SELECT DISTINCT
       m.universe_id,
       e.analysis_id
  FROM public.universe_membership m
  JOIN public.company co
    ON co.company_id = m.company_id
   AND co.delisted_at IS NULL
   AND co.out_of_scope_at IS NULL
   AND co.isin IS NOT NULL
  -- `asset_execution.isin` is NOT NULL UNIQUE, so this is 1:1 per company — no ambiguity about
  -- which asset row a company bridges to. Normalised on both sides because the backfill compared
  -- `upper(btrim(...))` and a stored ISIN with stray case/whitespace must not silently miss.
  JOIN public.asset_execution e
    ON upper(btrim(e.isin)) = upper(btrim(co.isin))
 WHERE e.analysis_id IS NOT NULL;

REVOKE ALL ON public.universe_asset_membership FROM anon, authenticated;
GRANT SELECT ON public.universe_asset_membership TO service_role;


-- Unchanged from 20260806050000 — recreated only because dropping the object above required it.
CREATE VIEW public.asset_grid AS
SELECT
    e.execution_id,
    e.isin,
    e.analysis_id,
    e.yahoo_symbol,
    COALESCE(e.name, a.label)              AS name,
    e.exchange,
    e.currency,
    COALESCE(a.asset_class, e.asset_class) AS asset_class,
    a.sector,
    a.short_multiplier,
    a.symbol                               AS analysis_symbol,
    e.listing_country,
    a.domicile_country,
    COALESCE(a.domicile_country, a.listing_country) AS country,
    a.continent,
    a.msci_region,
    e.med_adv_eur,
    e.first_date,
    e.years,
    e.wrapper,
    e.is_leveraged,
    e.is_default,
    e.status,
    e.reason,
    e.openfigi_figi,
    e.openfigi_name,
    e.openfigi_ticker,
    e.openfigi_exch,
    e.openfigi_type,
    e.identity_status,
    a.parquet_path,
    a.parquet_rows,
    e.updated_at,
    -- Yahoo's series (`asset_price`).
    a.price_from,
    a.price_to,
    a.bars,
    a.volume_from,
    a.volume_to,
    a.zero_vol_frac,
    a.market_cap_eur,
    a.market_cap_currency,
    COALESCE(li.name, la.name)                 AS leonteq_name,
    COALESCE(li.currency, la.currency)         AS leonteq_currency,
    COALESCE(li.product_type, la.product_type) AS leonteq_product_type,
    (li.identifier IS NOT NULL OR la.identifier IS NOT NULL) AS leonteq_verified,
    COALESCE(u.labels, '{}')                   AS universes,
    c.company_id,
    c.company_name                             AS gf_company_name,
    c.gurufocus_ticker                         AS gf_ticker,
    c.gf_exchange,
    c.has_financials                           AS gf_has_financials,
    c.has_dividend_payments                    AS gf_has_dividends,
    c.delisted_at,
    c.illiquid_at,
    c.out_of_scope_at,
    c.orphaned_at,
    -- GuruFocus's series (`metric_data`), via `company_price_coverage`.
    pc.price_from                              AS gf_price_from,
    pc.price_to                                AS gf_price_to,
    pc.price_bars                              AS gf_price_bars,
    pc.volume_to                               AS gf_volume_to,
    pc.volume_bars                             AS gf_volume_bars
FROM public.asset_execution e
LEFT JOIN public.asset_analysis  a  ON a.analysis_id = e.analysis_id
LEFT JOIN public.leonteq_universe li ON li.identifier = e.isin
LEFT JOIN public.leonteq_universe la ON la.identifier = a.symbol
LEFT JOIN LATERAL (
    SELECT array_agg(un.label ORDER BY un.label) AS labels
      FROM public.universe_asset_membership m
      JOIN public.universe un ON un.universe_id = m.universe_id
     WHERE m.analysis_id = e.analysis_id
) u ON true
LEFT JOIN LATERAL (
    SELECT co.company_id, co.company_name, co.gurufocus_ticker,
           gx.exchange_code AS gf_exchange,
           co.has_financials, co.has_dividend_payments,
           co.delisted_at, co.illiquid_at, co.out_of_scope_at, co.orphaned_at
      FROM public.company co
      LEFT JOIN public.gurufocus_exchange gx ON gx.exchange_id = co.exchange_id
     WHERE co.isin = e.isin
     ORDER BY co.company_id
     LIMIT 1
) c ON true
LEFT JOIN public.company_price_coverage pc ON pc.company_id = c.company_id;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

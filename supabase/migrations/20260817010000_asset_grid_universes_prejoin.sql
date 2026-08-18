-- `asset_grid` timed out: the universes LATERAL was re-running a VIEW once per grid row.
--
-- ⚠⚠ THE SYMPTOM WAS A 57014 ON `/asset-pipeline`, AND ONLY ON THE LATER PAGES. `GET
--   /api/asset-pipeline/grid` offset-pages the view 1,000 rows at a time; page 1 returned in 1.3s
--   and every page from offset ~8,000 on hit PostgREST's 8s `statement_timeout` (set on the
--   `authenticator` role; `service_role` has no override of its own, so it inherits it). The whole
--   grid never loaded — which is the endpoint the page is built on, so the page is simply blank.
--
-- ⚠⚠ THE CAUSE WAS THE LAST MIGRATION'S ONE MEASUREMENT, AND IT MEASURED THE WRONG THING.
--   `20260806060000` turned `universe_asset_membership` from a stale backfilled table into a
--   derived view — right, for the reasons it states at length — and recorded "AND IT COSTS
--   NOTHING: 28.0 ms view-backed vs 29.9-33.9 ms table-backed on the full 16,613-row grid".
--
--   That timing cannot see this cost. A `LEFT JOIN LATERAL` cannot change the row count, so
--   Postgres elides it entirely for `count(*)` — measured today, `select count(*) from asset_grid`
--   is **0.03s** while `select row_to_json(g) from asset_grid g` is **10.33s**. Any benchmark that
--   does not PROJECT the columns is timing a plan that never runs the laterals. The rule this
--   leaves behind: benchmark a wide view by reading its ROWS, never by counting them.
--
--   What the projection actually ran (EXPLAIN ANALYZE, local, 16,613 rows):
--
--     Nested Loop Left Join                              10,642 ms   <- the whole query
--       Memoize                        loops=16,613      10,250 ms   <- 8,055 misses
--         Aggregate (array_agg)        loops=8,055       10,230 ms
--           Unique / Sort / Hash Join  loops=8,055       10,173 ms   <- the membership VIEW
--             Seq Scan on company      loops=8,055        1,990 ms   <- 2,538 rows, 8,055 times
--
--   The correlated `WHERE m.analysis_id = e.analysis_id` gives the planner no way to evaluate the
--   membership view once: it re-derives the ISIN bridge (`universe_membership` -> `company` ->
--   `asset_execution`, plus its DISTINCT) for every distinct analysis_id in the grid. Memoize
--   halves the executions and cannot fix the shape.
--
-- ⚠ THE FIX IS THE JOIN SHAPE, NOT AN INDEX AND NOT A TABLE COMING BACK. Pre-aggregating to one
--   row per `analysis_id` and joining on it evaluates the membership view EXACTLY ONCE. `GROUP BY
--   m.analysis_id` is what keeps this a plain `LEFT JOIN` rather than a row multiplier — the
--   guarantee the `LIMIT 1` lateral below it needs for a different reason (a duplicate ISIN in
--   `company`), and which does not apply here because the aggregate collapses the group by
--   construction. `COALESCE(u.labels, '{}')` is unchanged, so a member of no universe still reads
--   as an empty array rather than NULL.
--
--   Nothing else about the previous migration is revisited: membership stays DERIVED (a mirror
--   that needs a human to re-run it is a mirror that is wrong), the `delisted_at` /
--   `out_of_scope_at` filter stays exactly as the backfill had it, and the DISTINCT stays
--   load-bearing.
--
-- ⚠ VERIFIED OUTPUT-IDENTICAL, NOT JUST FASTER. All 16,613 rows of (execution_id, universes,
--   company_id) hash the same before and after (sha256 over the ordered result), with 2,470 rows
--   carrying a non-empty universes array on both sides. A performance fix to a view is only a fix
--   if the rows it serves are the same rows.

DROP VIEW IF EXISTS public.asset_grid;

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
-- ⚠ PRE-AGGREGATED AND JOINED, **NOT** A CORRELATED LATERAL — see the header. One row per
--   analysis_id by construction, so this cannot multiply grid rows.
LEFT JOIN (
    SELECT m.analysis_id,
           array_agg(un.label ORDER BY un.label) AS labels
      FROM public.universe_asset_membership m
      JOIN public.universe un ON un.universe_id = m.universe_id
     GROUP BY m.analysis_id
) u ON u.analysis_id = e.analysis_id
LEFT JOIN LATERAL (
    SELECT co.company_id, co.company_name, co.gurufocus_ticker,
           gx.exchange_code AS gf_exchange,
           co.has_financials, co.has_dividend_payments,
           co.delisted_at, co.illiquid_at, co.out_of_scope_at, co.orphaned_at
      FROM public.company co
      LEFT JOIN public.gurufocus_exchange gx ON gx.exchange_id = co.exchange_id
     WHERE co.isin = e.isin
     -- ⚠ STAYS A LATERAL WITH `LIMIT 1`. `company.isin` is nullable and NOT unique — two rows
     --   sharing an ISIN is this app's definition of "the same security" and `dedupe_by_isin`
     --   resolves it, but between ingests the duplicate exists and a plain join would double the
     --   grid row. Cheap regardless: `idx_company_isin` makes it an index lookup, unlike the
     --   membership view above, which had no per-row predicate to index.
     ORDER BY co.company_id
     LIMIT 1
) c ON true
LEFT JOIN public.company_price_coverage pc ON pc.company_id = c.company_id;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

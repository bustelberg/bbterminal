-- Surface benchmark membership on the asset grid: one `universes text[]` per row.
--
-- WHY A COLUMN AT ALL
--   `/asset-pipeline` is becoming the single place a company is looked at, and "which benchmarks
--   is this in" is the last thing `/companies` showed that the grid could not. Reading it as an
--   array here means the page filters and renders chips off the row it already has, instead of a
--   second request per screen.
--
-- ⚠⚠ A LATERAL AGGREGATE, NOT A JOIN. A plain
--       LEFT JOIN universe_asset_membership m ON m.analysis_id = e.analysis_id
--   multiplies the grid: a company in SP500 and ACWI and Leonteq becomes THREE rows, and every
--   count, filter and `med_adv_eur` sum downstream is silently inflated. The subquery collapses to
--   one array before it reaches the row, so the grid keeps exactly one row per execution.
--
-- ⚠ KEYED ON `analysis_id`, WHICH IS NOT UNIQUE IN THIS VIEW. `asset_grid` is one row per
--   EXECUTION (a listing), and several listings share an analysis asset. Membership is a property
--   of the company, so every listing of it correctly shows the same labels — that is intended, not
--   a duplicate. Anything counting index constituents must count DISTINCT analysis_id, exactly as
--   it already must for anything else asset-level.
--
-- ⚠ `universe`, NOT `asset_universe`. These are the benchmark universes (SP500, ACWI, AEX,
--   Leonteq); `asset_universe`/`asset_universe_member` is the saved liquidity screen and a
--   different thing entirely. See the header of 20260806010000.
--
-- ⚠ EMPTY ARRAY, NEVER NULL. `COALESCE(..., '{}')` so a caller can `unnest`, `= ANY` or check
--   `cardinality` without a null branch — and so "in no benchmark" reads as an answer rather than
--   as missing data.

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
    -- Geography: this ROW's listing venue, the issuer's domicile, and the
    -- resolved country (domicile wins) + its continent / MSCI region.
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
    COALESCE(u.labels, '{}')                   AS universes
FROM public.asset_execution e
LEFT JOIN public.asset_analysis  a  ON a.analysis_id = e.analysis_id
LEFT JOIN public.leonteq_universe li ON li.identifier = e.isin
LEFT JOIN public.leonteq_universe la ON la.identifier = a.symbol
LEFT JOIN LATERAL (
    SELECT array_agg(un.label ORDER BY un.label) AS labels
      FROM public.universe_asset_membership m
      JOIN public.universe un ON un.universe_id = m.universe_id
     WHERE m.analysis_id = e.analysis_id
) u ON true;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

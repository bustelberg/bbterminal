-- The last things `/companies` shows that `asset_grid` could not: the GuruFocus handle, its
-- coverage flags, and the price-status markers.
--
-- ⚠⚠ SURFACED THROUGH THE VIEW, NOT COPIED ONTO `asset_execution`. The migration plan said "move
--   these columns"; copying them would give every one a second home that the ingest has to keep in
--   step — `company.has_financials` is written by the earnings backfill, `delisted_at` by the
--   staleness sweep, `gurufocus_ticker` by ticker resolution — and a stale copy of "is this
--   delisted" is worse than no copy, because the price refresh reads it to decide what to skip.
--   The same rule the rest of this schema follows: the count is a VIEW, never a column; the bridge
--   is a JOIN, never a column. `company` stays the writer's table, this is the reader's.
--
-- ⚠ A LATERAL WITH `LIMIT 1`, NOT A PLAIN JOIN. `company.isin` is nullable and not unique — two
--   rows sharing an ISIN is the app's definition of "the same security" and `dedupe_by_isin` is
--   what resolves it, but between ingests the duplicate exists. A plain join would then silently
--   double those grid rows. Verified clean today (2,563 distinct ISINs, zero duplicates), which is
--   exactly why the guard has to be structural rather than a reliance on that staying true.
--
-- ⚠ PREFIXED `gf_`, AND THE MARKET CAP IS DELIBERATELY NOT HERE. `asset_grid.market_cap_eur`
--   already exists and is YAHOO's, via `asset_analysis`. `company` carries its own GuruFocus cap
--   plus its native value, currency and FX rate. Putting those beside the Yahoo figure would place
--   two different market caps on one row with nothing to say which is which — the class of thing
--   that produces a confident wrong number. If the GuruFocus cap is ever needed here it comes with
--   an unmissable name, not as `market_cap_*` next to another `market_cap_eur`.
--
-- ⚠ `company_id` IS THE POINT OF THE WHOLE JOIN. It is the key `metric_data` hangs off — every
--   GuruFocus price, volume and fundamental line. Exposing it here is what lets a caller start
--   from the asset grid and still reach the fundamentals, which is the last dependency keeping
--   `company` alive.

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
    -- ── The company world, reached by ISIN. NULL for the ~14,000 asset rows with no company
    --    behind them (bonds, futures, most ETFs) — which is an answer, not a gap.
    c.company_id,
    c.company_name                             AS gf_company_name,
    c.gurufocus_ticker                         AS gf_ticker,
    c.gf_exchange,
    c.has_financials                           AS gf_has_financials,
    c.has_dividend_payments                    AS gf_has_dividends,
    -- Price-status markers. Every one excludes the row from the freshness measure, and
    -- `delisted_at` also drops it from price refreshes — so a reader of this grid can finally see
    -- WHY a series stopped moving instead of inferring it from a stale date.
    c.delisted_at,
    c.illiquid_at,
    c.out_of_scope_at,
    c.orphaned_at
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
     -- ⚠ DETERMINISTIC, NOT ARBITRARY. With a duplicate ISIN present the LIMIT alone would pick a
     --   different row run to run; ordering makes the grid stable while dedupe catches up.
     ORDER BY co.company_id
     LIMIT 1
) c ON true;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

-- Surface the GuruFocus price/volume coverage on `asset_grid`, beside Yahoo's `bars`/`price_from`
-- /`price_to`. This is the last piece of "one row per company, both vendors visible".
--
-- ⚠⚠ TWO VENDORS, TWO SERIES, AND THEY ARE NOT THE SAME NUMBER. `bars`/`price_from`/`price_to`
--   are YAHOO's (`asset_price`, keyed `analysis_id`); the new `gf_*` columns are GURUFOCUS's
--   (`metric_data`, keyed `company_id`). `/backtest` and `/schedule` price off GuruFocus; the
--   AIRS model portfolios and the asset benchmarks price off Yahoo. A row showing 5,529 Yahoo
--   bars tells you NOTHING about whether the momentum engine can price it — which is exactly the
--   confusion these columns exist to end, so they are prefixed and never merged into one figure.
--
-- ⚠ REACHED VIA `company_id`, WHICH THIS VIEW ALREADY RESOLVES. The `c` LATERAL below is the same
--   ISIN bridge added in 20260806030000; the coverage join hangs off its `company_id` rather than
--   re-deriving the bridge, so the two can never disagree about which company a row means.
--
-- ⚠ NULL MEANS "NO COMPANY ROW OR NO SERIES", AND THAT IS AN ANSWER. Roughly 14,000 asset rows
--   (bonds, futures, most ETFs) have no company behind them at all and never will — GuruFocus
--   ingests operating companies. A blank here is "not in that world", not a gap to go fill.

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

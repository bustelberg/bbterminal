-- Inverse/short ETF leverage: the multiplier (1 = -1x, 2 = -2x, 3 = -3x) for a
-- security tagged with a "Short …" sector. Kept SEPARATE from the sector name so
-- the sector stays a clean category (Short Real Estate, Short Bonds, …) and
-- leverage is a filterable/badge attribute. NULL for everything not a short.

ALTER TABLE public.asset_analysis
    ADD COLUMN IF NOT EXISTS short_multiplier smallint;

-- Re-create asset_grid to surface the new column (views don't auto-pick up
-- table columns). Definition copied from 20260707030000 + short_multiplier.
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
    (li.identifier IS NOT NULL OR la.identifier IS NOT NULL) AS leonteq_verified
FROM public.asset_execution e
LEFT JOIN public.asset_analysis  a  ON a.analysis_id = e.analysis_id
LEFT JOIN public.leonteq_universe li ON li.identifier = e.isin
LEFT JOIN public.leonteq_universe la ON la.identifier = a.symbol;

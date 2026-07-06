-- Add zero_vol_frac to the asset grid (etoro-style data-quality/liquidity
-- signal): the fraction of stored daily bars that have zero (or null) traded
-- volume. A liquid equity should read ~0; a high value flags an illiquid name
-- or a Yahoo data gap (FX/index series carry no volume at all → ~1.0). Appended
-- to the end of asset_grid so CREATE OR REPLACE stays valid.

CREATE OR REPLACE VIEW public.asset_grid AS
SELECT
    e.execution_id,
    e.isin,
    e.analysis_id,
    e.yahoo_symbol,
    COALESCE(e.name, a.label)                 AS name,
    e.exchange,
    e.currency,
    COALESCE(a.asset_class, e.asset_class)    AS asset_class,
    a.sector,
    a.symbol                                  AS analysis_symbol,
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
    a.parquet_path,
    a.parquet_rows,
    e.updated_at,
    (SELECT min(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_from,
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_to,
    (SELECT count(*)           FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS bars,
    (SELECT min(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id AND p.volume > 0) AS volume_from,
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id AND p.volume > 0) AS volume_to,
    (SELECT count(*) FILTER (WHERE COALESCE(p.volume, 0) = 0)::numeric / NULLIF(count(*), 0)
       FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS zero_vol_frac
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

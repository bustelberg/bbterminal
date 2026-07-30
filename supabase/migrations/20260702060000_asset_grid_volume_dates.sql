-- Add volume-coverage dates to the asset grid (like the etoro vol_from/vol_to):
-- the first/last date the asset actually had traded VOLUME (> 0), which can be a
-- narrower span than the price coverage (an index/FX series has prices but no
-- volume). Appended to the end of asset_grid so CREATE OR REPLACE stays valid.

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
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id AND p.volume > 0) AS volume_to
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

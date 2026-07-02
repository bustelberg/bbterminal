-- Add a volume-coverage count to asset_catalog: rows with a POSITIVE traded
-- volume (equities ≈ price_rows; FX/indices = 0 since they carry no real
-- volume). Appended at the end so CREATE OR REPLACE VIEW is valid.

CREATE OR REPLACE VIEW public.asset_catalog AS
SELECT
    a.analysis_id,
    a.symbol,
    a.asset_class,
    a.label,
    a.sector,
    a.currency,
    a.first_date,
    a.years,
    a.updated_at,
    (SELECT count(*) FROM public.asset_execution e WHERE e.analysis_id = a.analysis_id) AS executions,
    (SELECT count(*) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id) AS price_rows,
    (SELECT min(p.target_date) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id) AS price_from,
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id) AS price_to,
    (SELECT count(*) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id AND p.volume > 0) AS volume_rows
FROM public.asset_analysis a;

REVOKE ALL ON public.asset_catalog FROM anon, authenticated;
GRANT SELECT ON public.asset_catalog TO service_role;

NOTIFY pgrst, 'reload schema';

-- Catalog view for the asset pipeline: one row per analysis asset with its
-- execution count + price coverage (rows + date span), aggregated in Postgres
-- so the browse endpoint is one round-trip regardless of asset count.
--
-- Security: the view owner (postgres) bypasses the base tables' deny-all RLS,
-- so we must NOT expose it to anon/authenticated — only the backend's
-- service_role reads it. Explicit revoke + grant below.

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
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id) AS price_to
FROM public.asset_analysis a;

REVOKE ALL ON public.asset_catalog FROM anon, authenticated;
GRANT SELECT ON public.asset_catalog TO service_role;

NOTIFY pgrst, 'reload schema';

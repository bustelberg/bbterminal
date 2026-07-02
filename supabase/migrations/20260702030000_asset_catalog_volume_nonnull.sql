-- volume_rows should count rows that HAVE a volume value (close+volume are
-- stored per bar, so this equals price_rows) — not rows with volume > 0, which
-- wrongly excluded legitimate zero-volume no-trade days and made price/volume
-- counts disagree. count(p.volume) counts non-null volumes.

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
    (SELECT count(p.volume) FROM public.asset_price p WHERE p.analysis_id = a.analysis_id) AS volume_rows
FROM public.asset_analysis a;

REVOKE ALL ON public.asset_catalog FROM anon, authenticated;
GRANT SELECT ON public.asset_catalog TO service_role;

NOTIFY pgrst, 'reload schema';

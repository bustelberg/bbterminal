-- Asset pipeline: persist EVERY input ISIN as a row (mapped OR unmapped) so the
-- flat per-ISIN grid can show bonds / not-found / errored ISINs, not just the
-- ones that resolved to a Yahoo listing. Extends `asset_execution` (already one
-- row per ISIN, isin UNIQUE) rather than adding a parallel table.
--
--   * analysis_id becomes NULLABLE — an unmapped ISIN has no analysis asset.
--   * status   — ok | bond | not_found | error  (ok = resolved + priced).
--   * reason   — the human-readable resolve reason for a non-ok row.
--   * asset_class — identity class for an unmapped row (no analysis to join to).
--
-- Existing rows keep status='ok' via the DEFAULT — no backfill needed.

ALTER TABLE public.asset_execution ALTER COLUMN analysis_id DROP NOT NULL;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'ok';
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS reason text;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS asset_class text;

-- Flat per-ISIN grid: one row per input ISIN, joined (LEFT — unmapped rows have
-- no analysis) to its analysis asset for class/sector/analysis-symbol, plus the
-- analysis series' price coverage (span + bar count). Same subselect idiom as
-- the existing asset_catalog view.
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
    e.updated_at,
    (SELECT min(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_from,
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_to,
    (SELECT count(*)           FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS bars
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

-- View owner (postgres) bypasses the base tables' deny-all RLS, so keep it off
-- anon/authenticated — only the backend's service_role reads it.
REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

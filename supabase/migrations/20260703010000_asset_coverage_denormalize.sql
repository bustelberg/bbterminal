-- The asset_grid view computed SIX correlated subqueries per row over
-- asset_price (price_from/to, bars, volume_from/to, zero_vol_frac). With
-- asset_price now at 14M+ rows and ~2.8k executions, evaluating that for every
-- grid page started blowing the statement timeout (57014). Denormalize the
-- per-asset coverage stats onto asset_analysis — written once at store time
-- (store.store_series) — so the grid view just READS columns (O(1) per row).

ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS price_from    date;
ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS price_to      date;
ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS bars          integer;
ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS volume_from   date;
ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS volume_to     date;
ALTER TABLE public.asset_analysis ADD COLUMN IF NOT EXISTS zero_vol_frac numeric;

-- Backfill existing assets in ONE grouped scan of asset_price. This runs via the
-- migration's DIRECT Postgres connection (not PostgREST), so it isn't bound by
-- the API statement timeout — and we disable the DB one for this heavy one-off
-- (session-level SET; the supabase CLI runs migrations outside a txn block).
SET statement_timeout = 0;
UPDATE public.asset_analysis a SET
    price_from    = s.pf,
    price_to      = s.pt,
    bars          = s.n,
    volume_from   = s.vf,
    volume_to     = s.vt,
    zero_vol_frac = CASE WHEN s.n > 0 THEN (s.n - s.nvol)::numeric / s.n ELSE NULL END
FROM (
    SELECT
        analysis_id,
        min(target_date)                          AS pf,
        max(target_date)                          AS pt,
        count(*)                                  AS n,
        min(target_date) FILTER (WHERE volume > 0) AS vf,
        max(target_date) FILTER (WHERE volume > 0) AS vt,
        count(*)         FILTER (WHERE volume > 0) AS nvol
    FROM public.asset_price
    GROUP BY analysis_id
) s
WHERE a.analysis_id = s.analysis_id;

-- Rebuild the grid view reading the denormalized columns (no subqueries). Same
-- output column names, so the API model + frontend are unchanged.
DROP VIEW IF EXISTS public.asset_grid;
CREATE VIEW public.asset_grid AS
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
    a.price_from,
    a.price_to,
    a.bars,
    a.volume_from,
    a.volume_to,
    a.zero_vol_frac
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';

-- Persist the expensive /momentum daily rank calculation across backend
-- restarts and deployments. The cache key includes calculation + universe
-- version inputs, while calculation_date naturally advances each Amsterdam day.
CREATE TABLE IF NOT EXISTS public.sector_timeline_cache (
    calculation_version integer NOT NULL,
    history_days integer NOT NULL,
    max_assets integer NOT NULL,
    calculation_date date NOT NULL,
    payload jsonb NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (calculation_version, history_days, max_assets, calculation_date)
);

ALTER TABLE public.sector_timeline_cache ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS sector_timeline_cache_deny_all ON public.sector_timeline_cache;
CREATE POLICY sector_timeline_cache_deny_all ON public.sector_timeline_cache
    FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.sector_timeline_cache TO service_role;
NOTIFY pgrst, 'reload schema';

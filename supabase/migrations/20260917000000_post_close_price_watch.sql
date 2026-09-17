-- Sold AIRS positions still need a price series: their post-sale return is shown
-- in Analyse. Keep this intentionally small, durable watch list separate from
-- current holdings so the daily price job can refresh only names that matter.
CREATE TABLE IF NOT EXISTS public.asset_post_close_watch (
    isin text PRIMARY KEY,
    last_sale_date date,
    observed_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.asset_post_close_watch ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS asset_post_close_watch_deny_all ON public.asset_post_close_watch;
CREATE POLICY asset_post_close_watch_deny_all ON public.asset_post_close_watch FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_post_close_watch TO service_role;

NOTIFY pgrst, 'reload schema';

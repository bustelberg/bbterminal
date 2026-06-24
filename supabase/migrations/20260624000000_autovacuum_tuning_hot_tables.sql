-- Tune autovacuum / autoanalyze for the two largest, most-churned tables so the
-- planner keeps fresh statistics and dead tuples are reclaimed promptly.
--
-- Why this matters for Disk IO budget:
--   metric_data is ~26M rows. With Postgres defaults
--   (autovacuum_analyze_scale_factor = 0.1, autovacuum_vacuum_scale_factor = 0.2)
--   ANALYZE only fires after ~2.6M changed rows and VACUUM after ~5.2M. Under the
--   daily price ingest that leaves the planner on stale row-count estimates, so it
--   can switch a covered index scan to a SEQUENTIAL SCAN of the multi-GB heap --
--   exactly the kind of repeated full read that exhausts a small instance's Disk
--   IO budget. Dead tuples from upserts also accumulate and bloat the heap (more
--   pages per scan = more IO).
--
--   Switching to absolute thresholds (scale_factor = 0 + a fixed threshold) makes
--   both fire after a predictable number of changed rows, keeping stats accurate
--   and bloat bounded. Cost-based throttling is left at the cluster default so the
--   maintenance passes themselves stay gentle on IO.
--
-- This changes only FUTURE autovacuum behavior. To fix the current stale stats +
-- bloat once, run (outside this migration, e.g. on prod):
--   VACUUM (ANALYZE) public.metric_data;
--   VACUUM (ANALYZE) public.universe_membership;

ALTER TABLE public.metric_data SET (
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 50000,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 100000
);

ALTER TABLE public.universe_membership SET (
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 20000,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 50000
);

-- Refresh planner statistics NOW so the fix takes effect immediately on apply
-- (the ALTERs above only change FUTURE autovacuum cadence). ANALYZE only samples
-- rows, so it is cheap and -- unlike VACUUM -- is allowed inside the transaction
-- that `supabase db push` wraps migrations in. To also reclaim heap bloat, run
-- `VACUUM (ANALYZE) public.metric_data;` manually during a low-traffic window.
ANALYZE public.metric_data;
ANALYZE public.universe_membership;

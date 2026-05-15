-- Drop `idx_metric_data_metric_company` — redundant with the primary key.
--
-- The index covers `(metric_code, company_id, target_date)` while the PK
-- covers `(company_id, metric_code, source_code, target_date)`. Every
-- hot query in this codebase already filters by company_id (alongside
-- metric_code), so the PK's leading column is a tight match and the
-- planner picks the PK unconditionally — confirmed by EXPLAIN on the
-- bulk-loader shape used by `momentum.data._helpers._load_metric_chunks`:
--
--     SELECT ... FROM metric_data
--     WHERE metric_code = '...' AND source_code = '...'
--       AND company_id IN (...) AND target_date BETWEEN ...
--     → Index Scan using metric_data_pkey
--
-- `pg_stat_user_indexes` over a 2h window showed:
--   metric_data_pkey                 384,452 scans   (workhorse)
--   idx_metric_data_metric_company         8 scans   (this one)
--   idx_metric_data_source_date           15 scans   (used by RPCs)
--
-- Dropping it frees ~800 MB on a ~3.8 GB DB (a 21% reduction in disk
-- usage) and removes one tree-maintenance cost from every INSERT/UPSERT
-- to metric_data — so the scheduled price/volume cron writes a bit
-- faster too.
--
-- Reversible: the original CREATE INDEX statement from
-- 20240101000000_full_schema.sql:41 is:
--   CREATE INDEX IF NOT EXISTS idx_metric_data_metric_company
--     ON metric_data (metric_code, company_id, target_date);
--
-- DROP INDEX without CONCURRENTLY here: this migration runs in an
-- implicit transaction (Supabase CLI default), which forbids CONCURRENTLY.
-- A non-concurrent DROP takes a brief ACCESS EXCLUSIVE lock — for a
-- single index drop it's a sub-second metadata operation, so the lock
-- window is negligible in practice.

DROP INDEX IF EXISTS public.idx_metric_data_metric_company;

NOTIFY pgrst, 'reload schema';

-- Covering index for the momentum price/volume bulk read.
--
-- The hot query (load_all_prices / load_all_volumes, now streamed via COPY)
-- filters: metric_code = $ AND source_code = 'gurufocus' AND company_id = ANY($)
-- AND target_date BETWEEN $ AND $, ORDER BY company_id, target_date.
--
-- The PK (company_id, metric_code, source_code, target_date) handles the
-- per-company seeks but is NOT covering — every matched row does a heap fetch
-- for numeric_value (millions of fetches per backtest). This index leads with
-- the equality columns, then company_id + target_date (matching both the
-- filter and the ORDER BY), and INCLUDEs numeric_value so the scan is
-- index-only: no heap I/O. Biggest win on a cold cache / in prod.
--
-- Building this on the ~20M-row prod table takes longer than the default
-- statement_timeout (which is why a plain `db push` 57014'd), so lift the
-- timeout for this migration only. A non-CONCURRENT build briefly locks the
-- table against WRITES (reads are unaffected) for the ~1-few minutes it runs —
-- fine for a one-off, but prefer to run it while the ingest pipeline is idle.
--
-- Zero-write-lock alternative (skip this migration, do it by hand): connect
-- with psql and run `SET statement_timeout=0;` then
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS ...` (CONCURRENTLY can't run inside
-- a migration's transaction), then
-- `supabase migration repair --status applied 20260604000000`.
--
-- IF NOT EXISTS makes this idempotent / a no-op once the index is present.
SET statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_metric_data_metric_source_company_date
    ON public.metric_data (metric_code, source_code, company_id, target_date)
    INCLUDE (numeric_value);

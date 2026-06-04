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
-- NOTE: on the full ~20M-row prod table a plain CREATE INDEX briefly locks
-- writes while it builds (~a minute). If that's a problem, run the equivalent
-- `CREATE INDEX CONCURRENTLY ...` by hand instead (it can't live inside a
-- migration transaction). IF NOT EXISTS makes this migration idempotent and a
-- no-op once the index is present.
CREATE INDEX IF NOT EXISTS idx_metric_data_metric_source_company_date
    ON public.metric_data (metric_code, source_code, company_id, target_date)
    INCLUDE (numeric_value);

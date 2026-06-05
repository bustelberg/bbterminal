-- Fix the 500 on GET /api/longequity/snapshots (get_distinct_dates RPC timing
-- out under the backend's 30s PostgREST client timeout).
--
-- The endpoint runs `SELECT DISTINCT target_date FROM metric_data WHERE
-- source_code = 'longequity'`. Two things make that slow on the ~20M-row prod
-- metric_data:
--
--   1. If the (source_code, target_date) index is missing (e.g. it didn't
--      survive a db reset), the filter seq-scans all 20M rows → 30s timeout.
--   2. Even WITH the index, a plain DISTINCT reads every matching row to
--      produce the handful of distinct snapshot dates — O(rows).
--
-- This migration fixes both:
--   * ensures idx_metric_data_source_date exists (idempotent; instant no-op
--     when already present), and
--   * rewrites get_distinct_dates as a recursive "loose index scan" (skip
--     scan): seek min(target_date), then repeatedly seek the next date
--     strictly greater. Each step is one index seek, so the cost is
--     O(distinct dates) regardless of row count. Output (ascending distinct
--     target_dates) is byte-identical — verified locally against the old
--     DISTINCT for both 'longequity' (9 dates) and 'gurufocus' (9138).

-- Building/locking on the ~20M-row table can exceed the default
-- statement_timeout; lift it for this migration only (mirrors
-- 20260604000000_metric_data_covering_index.sql). IF NOT EXISTS keeps it a
-- no-op once the index is present; a non-CONCURRENT build briefly locks the
-- table against WRITES (reads unaffected) — prefer to run while the ingest
-- pipeline is idle.
SET statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_metric_data_source_date
    ON public.metric_data (source_code, target_date);

CREATE OR REPLACE FUNCTION public.get_distinct_dates(p_source_code text)
    RETURNS TABLE(target_date date)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  WITH RECURSIVE skip AS (
    -- Anchor: the earliest snapshot date for this source (NULL if none).
    SELECT (
      SELECT min(md.target_date)
      FROM metric_data md
      WHERE md.source_code = p_source_code
    ) AS d
    UNION ALL
    -- Step: the next distinct date strictly greater than the previous one.
    SELECT (
      SELECT min(md.target_date)
      FROM metric_data md
      WHERE md.source_code = p_source_code
        AND md.target_date > skip.d
    )
    FROM skip
    WHERE skip.d IS NOT NULL
  )
  SELECT skip.d
  FROM skip
  WHERE skip.d IS NOT NULL
  ORDER BY skip.d;
$$;

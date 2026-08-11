-- Indexes this script dropped from prod, with the statement that puts each back.
-- Replay with:  ./scripts/prod-reclaim-disk.ps1 -RebuildDropped -Apply
-- Every line is IF NOT EXISTS, so replaying is a no-op for anything already built.

-- dropped 2026-08-11 10:34 from prod to free 5999 MB (189364 lifetime scans)
-- rebuild with CONCURRENTLY so it does not lock the table; needs free space
-- roughly the size above, so grow the disk FIRST.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_metric_data_metric_source_company_date ON public.metric_data USING btree (metric_code, source_code, company_id, target_date) INCLUDE (numeric_value)
;

-- ============================================================================
-- dropped 2026-08-11 (471 MB, 30 lifetime scans) -- AND THAT WAS A MISTAKE.
--
-- !! 30 SCANS MEANT RARE, NOT UNUSED, AND THE DIFFERENCE IS A 500. This index
-- exists for ONE caller: `get_distinct_dates`, the RPC behind
-- GET /api/longequity/snapshots. Migration 20260606000000 was written precisely
-- because that endpoint timed out at 30s without it -- and the skip-scan
-- rewrite in the same migration is BUILT ON this index: each recursive step is
-- one seek on (source_code, target_date). Without it every step degrades to a
-- scan of ~26M rows, so the endpoint does not get slower, it 500s.
--
-- The lesson is the one already written for the covering index two lines up and
-- then not applied here: a scan COUNT measures how often an index is used, not
-- how much the query that uses it needs it. The page is opened rarely; when it
-- is opened, this is the difference between a snapshot list and an error.
--
-- The definition is `supabase/migrations/20260606000000_get_distinct_dates_skip_scan.sql`,
-- which also re-creates it idempotently -- so a fresh environment is fine; only
-- an already-migrated prod needs this line.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_metric_data_source_date ON public.metric_data USING btree (source_code, target_date)
;

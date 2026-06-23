-- Company-scoped metric COVERAGE RPC — for the per-universe "is every member's
-- price/volume up to date AND ≥1 year deep without gaps?" readout on /schedule.
--
-- Per company (scoped to an id array, so it rides the
-- (metric_code, source_code, company_id, target_date) index):
--   earliest_target_date / latest_target_date  — full-history span
--   points_since                               — datapoints in the window (≥ p_since)
--   max_gap_days                               — largest consecutive-day gap WITHIN
--                                                the window (a big gap = missing data)
--
-- Gaps are measured only inside the window (LAG over rows ≥ p_since) so the
-- window's leading boundary isn't mistaken for a gap. Chunk the id array
-- caller-side to stay under the PostgREST statement timeout.

CREATE OR REPLACE FUNCTION public.company_metric_coverage_for(
    p_company_ids integer[], p_metric_code text, p_since date)
    RETURNS TABLE(company_id integer, earliest_target_date text,
                  latest_target_date text, points_since integer, max_gap_days integer)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  WITH rows AS (
    SELECT md.company_id, md.target_date
    FROM metric_data md
    WHERE md.metric_code = p_metric_code
      AND md.source_code = 'gurufocus'
      AND md.company_id = ANY(p_company_ids)
  ),
  spans AS (
    SELECT r.company_id, MIN(r.target_date) AS earliest, MAX(r.target_date) AS latest
    FROM rows r GROUP BY r.company_id
  ),
  windowed AS (
    SELECT r.company_id, r.target_date,
           r.target_date - LAG(r.target_date)
             OVER (PARTITION BY r.company_id ORDER BY r.target_date) AS gap
    FROM rows r WHERE r.target_date >= p_since
  ),
  wagg AS (
    SELECT w.company_id, COUNT(*) AS pts, COALESCE(MAX(w.gap), 0) AS maxgap
    FROM windowed w GROUP BY w.company_id
  )
  SELECT s.company_id, s.earliest::text, s.latest::text,
         COALESCE(w.pts, 0)::int, COALESCE(w.maxgap, 0)::int
  FROM spans s LEFT JOIN wagg w ON w.company_id = s.company_id;
$$;

GRANT EXECUTE ON FUNCTION public.company_metric_coverage_for(integer[], text, date)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';

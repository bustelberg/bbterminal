-- Optimize the two price/volume coverage RPCs that dominate prod Disk IO.
--
-- Both backed /api/data/{price,universe}-coverage by doing
--   SELECT company_id, MAX(target_date) ... GROUP BY company_id
-- which reads EVERY date row per company (the whole close_price/gurufocus range,
-- ~13M rows / ~2.3 GB) just to find each company's latest date. On prod that was
-- ~6.2 s/call (full-table, 27% of all DB time) and ~3.3 s/call (scoped, 19%) at
-- single-digit cache-hit rates -- i.e. read from disk almost every time.
--
-- Replace the GROUP BY with a per-company lateral "loose index scan":
--   ... ORDER BY target_date DESC LIMIT 1
-- The (company_id, metric_code, source_code, target_date) PK index lets Postgres
-- seek straight to each company's range and read ONE row (its latest date) rather
-- than aggregating the whole range. Reads drop from O(rows) to O(companies).
-- Result shape + semantics are unchanged (companies with no rows are omitted,
-- exactly as the GROUP BY did).

-- Full-universe variant (price-coverage global freshest/most-stale).
CREATE OR REPLACE FUNCTION public.company_latest_close_price_dates()
    RETURNS TABLE(company_id integer, latest_target_date text)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  SELECT c.company_id, l.latest_target_date
  FROM public.company c
  CROSS JOIN LATERAL (
    SELECT md.target_date::text AS latest_target_date
    FROM public.metric_data md
    WHERE md.company_id = c.company_id
      AND md.metric_code = 'close_price'
      AND md.source_code = 'gurufocus'
    ORDER BY md.target_date DESC
    LIMIT 1
  ) l;
$$;

-- Scoped variant (universe-coverage, per metric, over a company-id array).
CREATE OR REPLACE FUNCTION public.company_latest_metric_dates_for(
    p_company_ids integer[], p_metric_code text)
    RETURNS TABLE(company_id integer, latest_target_date text)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  SELECT cid AS company_id, l.latest_target_date
  FROM unnest(p_company_ids) AS cid
  CROSS JOIN LATERAL (
    SELECT md.target_date::text AS latest_target_date
    FROM public.metric_data md
    WHERE md.company_id = cid
      AND md.metric_code = p_metric_code
      AND md.source_code = 'gurufocus'
    ORDER BY md.target_date DESC
    LIMIT 1
  ) l;
$$;

GRANT EXECUTE ON FUNCTION public.company_latest_close_price_dates()
    TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.company_latest_metric_dates_for(integer[], text)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';

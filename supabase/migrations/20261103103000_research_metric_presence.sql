-- Compact, alias-aware audit of actual values; no row cap or per-company HTTP reads.
CREATE OR REPLACE FUNCTION public.research_metric_presence(company_ids integer[], metric_codes text[], code_keys jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb LANGUAGE sql STABLE SECURITY INVOKER SET search_path = public AS $$
WITH requested_companies AS MATERIALIZED (SELECT DISTINCT unnest(company_ids) AS company_id),
requested_codes AS MATERIALIZED (SELECT DISTINCT unnest(metric_codes) AS metric_code),
selected AS MATERIALIZED (
  SELECT d.company_id, d.metric_code, d.target_date, d.numeric_value
  FROM public.metric_data d
  JOIN requested_companies c ON c.company_id = d.company_id
  JOIN requested_codes m ON m.metric_code = d.metric_code
  WHERE d.target_date >= '2017-01-01' AND d.numeric_value IS NOT NULL
), presence AS (
  SELECT company_id, jsonb_agg(DISTINCT metric_code) AS codes FROM selected GROUP BY company_id
), periods AS (
  SELECT coalesce(code_keys->>metric_code, metric_code) AS metric,
         extract(year FROM target_date)::integer AS year,
         count(DISTINCT company_id) AS companies,
         count(DISTINCT company_id) FILTER (WHERE numeric_value > 0) AS positive_companies
  FROM selected GROUP BY 1, 2
)
SELECT jsonb_build_object(
  'presence', coalesce((SELECT jsonb_object_agg(company_id::text, codes) FROM presence), '{}'::jsonb),
  'periods', coalesce((SELECT jsonb_agg(to_jsonb(periods) ORDER BY metric, year) FROM periods), '[]'::jsonb)
);
$$;
REVOKE ALL ON FUNCTION public.research_metric_presence(integer[], text[], jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.research_metric_presence(integer[], text[], jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';

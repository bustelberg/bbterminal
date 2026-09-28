-- Attribute external request volume without turning api_usage into a row-per-request event log.
ALTER TABLE public.api_usage
    ADD COLUMN source text NOT NULL DEFAULT 'gurufocus',
    ADD COLUMN job text NOT NULL DEFAULT 'legacy',
    ADD COLUMN outcome text NOT NULL DEFAULT 'unknown';

ALTER TABLE public.api_usage DROP CONSTRAINT api_usage_month_region_key;
ALTER TABLE public.api_usage
    ADD CONSTRAINT api_usage_dimensions_key
    UNIQUE (month, region, source, job, outcome);

-- Keep the old RPC valid during rolling deploys. Old instances land in an explicit legacy bucket.
CREATE OR REPLACE FUNCTION public.increment_api_usage(
    p_month text, p_region text, p_count integer
) RETURNS void
LANGUAGE plpgsql
SET search_path TO 'public', 'pg_temp'
AS $$
BEGIN
  INSERT INTO api_usage (month, region, source, job, outcome, request_count)
  VALUES (p_month, p_region, 'gurufocus', 'legacy', 'unknown', p_count)
  ON CONFLICT (month, region, source, job, outcome)
  DO UPDATE SET request_count = api_usage.request_count + EXCLUDED.request_count;
END;
$$;

CREATE FUNCTION public.increment_api_usage_attributed(
    p_month text, p_region text, p_source text, p_job text, p_outcome text, p_count integer
) RETURNS void
LANGUAGE plpgsql
SET search_path TO 'public', 'pg_temp'
AS $$
BEGIN
  INSERT INTO api_usage (month, region, source, job, outcome, request_count)
  VALUES (p_month, p_region, p_source, p_job, p_outcome, p_count)
  ON CONFLICT (month, region, source, job, outcome)
  DO UPDATE SET request_count = api_usage.request_count + EXCLUDED.request_count;
END;
$$;

REVOKE ALL ON FUNCTION public.increment_api_usage_attributed(
    text, text, text, text, text, integer
) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.increment_api_usage_attributed(
    text, text, text, text, text, integer
) TO service_role;

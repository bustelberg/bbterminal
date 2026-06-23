-- Company-scoped "latest target_date per company for a metric" RPC.
--
-- The per-universe price/volume coverage readout (/api/data/universe-coverage)
-- needs each company's latest close_price AND volume date. The existing
-- `company_latest_close_price_dates()` GROUPs over the whole 26M-row
-- metric_data table — too slow for PostgREST's statement timeout once you also
-- want it for volume. This variant is SCOPED to a company-id array, so it rides
-- the (metric_code, source_code, company_id, target_date) index and returns in
-- milliseconds. The coverage endpoint passes the union of the tradable
-- universes' members (a few thousand ids), in chunks.
--
-- search_path locked + EXECUTE granted, matching the existing date RPCs.

CREATE OR REPLACE FUNCTION public.company_latest_metric_dates_for(
    p_company_ids integer[], p_metric_code text)
    RETURNS TABLE(company_id integer, latest_target_date text)
    LANGUAGE sql STABLE
    SET search_path TO 'public', 'pg_temp'
    AS $$
  SELECT md.company_id, MAX(md.target_date::TEXT) AS latest_target_date
  FROM metric_data md
  WHERE md.metric_code = p_metric_code
    AND md.source_code = 'gurufocus'
    AND md.company_id = ANY(p_company_ids)
  GROUP BY md.company_id;
$$;

GRANT EXECUTE ON FUNCTION public.company_latest_metric_dates_for(integer[], text)
    TO anon, authenticated, service_role;

NOTIFY pgrst, 'reload schema';

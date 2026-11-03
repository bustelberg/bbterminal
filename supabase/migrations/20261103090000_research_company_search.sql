-- Research picks a company/price series, not every execution ISIN linked to it.
-- Match the small execution table before joining price metadata. Avoid asset_grid's
-- unrelated universe, company coverage and structured-product joins.
CREATE OR REPLACE FUNCTION public.search_research_companies(search_term text, result_limit integer DEFAULT 25)
RETURNS TABLE(isin text, analysis_id bigint, name text, yahoo_symbol text,
              exchange text, currency text, sector text, bars bigint)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = public
AS $$
WITH term AS (
  SELECT '%' || replace(replace(replace(btrim(search_term), '\', '\\'), '%', '\%'), '_', '\_') || '%' AS pattern
), matches AS MATERIALIZED (
  SELECT DISTINCT e.analysis_id
  FROM public.asset_execution e, term t
  WHERE e.status = 'ok' AND e.analysis_id IS NOT NULL AND btrim(search_term) <> ''
    AND (e.name ILIKE t.pattern OR e.isin ILIKE t.pattern OR e.yahoo_symbol ILIKE t.pattern)
), ranked AS (
  SELECT e.isin::text, a.analysis_id::bigint, coalesce(e.name, a.label)::text AS name,
         e.yahoo_symbol::text, e.exchange::text, e.currency::text, a.sector::text, a.bars::bigint,
         row_number() OVER (PARTITION BY a.analysis_id ORDER BY
           (e.openfigi_type = 'Common Stock') DESC NULLS LAST,
           e.is_default DESC NULLS LAST,
           EXISTS (SELECT 1 FROM public.company c WHERE c.isin = e.isin) DESC,
           e.med_adv_eur DESC NULLS LAST, e.isin) AS rank
  FROM matches m
  JOIN public.asset_analysis a ON a.analysis_id = m.analysis_id AND a.bars > 0
  JOIN public.asset_execution e ON e.analysis_id = a.analysis_id AND e.status = 'ok'
)
SELECT r.isin, r.analysis_id, r.name, r.yahoo_symbol, r.exchange, r.currency, r.sector, r.bars
FROM ranked r WHERE r.rank = 1
ORDER BY (lower(r.yahoo_symbol) = lower(btrim(search_term))) DESC,
         (lower(r.name) = lower(btrim(search_term))) DESC,
         (strpos(lower(r.name), lower(btrim(search_term))) = 1) DESC, r.bars DESC, r.name, r.isin
LIMIT greatest(1, least(coalesce(result_limit, 25), 100)) + 1;
$$;
REVOKE ALL ON FUNCTION public.search_research_companies(text, integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.search_research_companies(text, integer) TO service_role;
NOTIFY pgrst, 'reload schema';

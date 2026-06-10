-- Latest known sector per company, for the /companies Sector column + filter.
--
-- Sector lives in `universe_membership` (per company, per month) — there's no
-- sector on `company` itself. This returns each company's most recent non-null
-- membership sector via DISTINCT ON, which the indexed company_id + month sort
-- makes cheap. Mirrors the `company_universe_labels` pattern used by the
-- /companies memberships column.
CREATE OR REPLACE FUNCTION public.company_latest_sector()
RETURNS TABLE(company_id integer, sector text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT ON (um.company_id) um.company_id, um.sector
  FROM public.universe_membership um
  WHERE um.sector IS NOT NULL AND um.sector <> ''
  ORDER BY um.company_id, um.target_month DESC;
$$;

GRANT EXECUTE ON FUNCTION public.company_latest_sector() TO anon, authenticated, service_role;

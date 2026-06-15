-- Per-company sector PLUS the universe it came from, for the /companies page.
--
-- Sector lives on universe_membership (per company, per month) — a company can
-- have a sector in several universes. Preference order:
--   1. Leonteq (the live LEONTEQ template) — the user's canonical sector source
--   2. otherwise the most-recent month's sector from any universe
-- Ties broken by universe label (so the live universe wins over its own frozen
-- copy, e.g. "ACWI" before "ACWI (as of 2026-06)"). `source_label` is the
-- universe the chosen sector came from, surfaced as an annotation in the UI.
CREATE OR REPLACE FUNCTION public.company_sector_with_source()
RETURNS TABLE(company_id integer, sector text, source_label text)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT ON (um.company_id)
         um.company_id,
         um.sector,
         u.label AS source_label
    FROM public.universe_membership um
    JOIN public.universe u USING (universe_id)
   WHERE um.sector IS NOT NULL AND um.sector <> ''
   ORDER BY um.company_id,
            (coalesce(u.template_key, '') = 'LEONTEQ') DESC,  -- Leonteq first
            (u.frozen_at IS NULL) DESC,                        -- live universes before frozen copies
            um.target_month DESC,                              -- else latest month
            u.label ASC;
$$;

GRANT EXECUTE ON FUNCTION public.company_sector_with_source() TO anon, authenticated, service_role;

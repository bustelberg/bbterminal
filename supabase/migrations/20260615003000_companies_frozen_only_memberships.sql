-- /companies should reference ONLY frozen static snapshots (the "(as of …)"
-- universes) — not the live template / time-series universes. So:
--   1. company_universe_labels (Memberships column) → frozen universes only.
--   2. company_sector_with_source → label the FROZEN snapshot as the sector
--      source (Leonteq family first), while keeping full coverage.
--   3. Remove the ACWI ∩ Leonteq intersection universe entirely.
--   4. Rename any "LongEquity (frozen …)" snapshot to "LongEquity (as of …)".

-- 1. Memberships: frozen snapshots only. (search_path preserved — see
--    20260522020000_fix_linter_security_findings.sql.)
CREATE OR REPLACE FUNCTION public.company_universe_labels()
RETURNS TABLE(company_id integer, labels text[])
LANGUAGE sql
STABLE
SET search_path TO 'public', 'pg_temp'
AS $$
  SELECT m.company_id,
         array_agg(DISTINCT u.label ORDER BY u.label) AS labels
    FROM universe_membership m
    JOIN universe u USING (universe_id)
   WHERE u.frozen_at IS NOT NULL
   GROUP BY m.company_id;
$$;
GRANT EXECUTE ON FUNCTION public.company_universe_labels() TO anon, authenticated, service_role;

-- 2. Sector source: Leonteq family first, then prefer the frozen snapshot as
--    the labelled source (so the annotation matches the frozen memberships),
--    then latest month, then label. No frozen-only filter here — sector
--    coverage is kept (live universes still provide a value when a company
--    isn't in a frozen snapshot's captured month).
CREATE OR REPLACE FUNCTION public.company_sector_with_source()
RETURNS TABLE(company_id integer, sector text, source_label text)
LANGUAGE sql
STABLE
SET search_path TO 'public', 'pg_temp'
AS $$
  SELECT DISTINCT ON (um.company_id)
         um.company_id,
         um.sector,
         u.label AS source_label
    FROM universe_membership um
    JOIN universe u USING (universe_id)
   WHERE um.sector IS NOT NULL AND um.sector <> ''
   ORDER BY um.company_id,
            (coalesce(u.frozen_from, '') = 'LEONTEQ' OR coalesce(u.template_key, '') = 'LEONTEQ') DESC,
            (u.frozen_at IS NOT NULL) DESC,
            um.target_month DESC,
            u.label ASC;
$$;
GRANT EXECUTE ON FUNCTION public.company_sector_with_source() TO anon, authenticated, service_role;

-- 3. Remove ACWI ∩ Leonteq entirely (membership first, then the universe row).
--    Also deregistered from the template registry so the pipeline won't recreate it.
DELETE FROM public.universe_membership
 WHERE universe_id IN (SELECT universe_id FROM public.universe WHERE template_key = 'ACWI_LEONTEQ');
DELETE FROM public.universe WHERE template_key = 'ACWI_LEONTEQ';

-- 4. Rename frozen LongEquity snapshots to the "(as of …)" convention.
UPDATE public.universe
   SET label = replace(label, '(frozen ', '(as of ')
 WHERE label LIKE 'LongEquity (frozen %';

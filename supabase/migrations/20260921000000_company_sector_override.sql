-- A manual sector is an editorial classification, not a correction to a
-- vendor/universe source.  Keep it in its own table so an ingest can refresh
-- source data without ever overwriting a management decision.
CREATE TABLE IF NOT EXISTS public.company_sector_override (
    company_id integer PRIMARY KEY REFERENCES public.company(company_id) ON DELETE CASCADE,
    sector text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

REVOKE ALL ON public.company_sector_override FROM anon, authenticated;
GRANT ALL ON public.company_sector_override TO service_role;

NOTIFY pgrst, 'reload schema';

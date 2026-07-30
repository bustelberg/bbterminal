-- Manual override of a holding's asset-class Class (bucket), keyed by ISIN so it is a property of
-- the INSTRUMENT and remembered forever. When present it BEATS the calculated `classify_bucket`.
CREATE TABLE IF NOT EXISTS public.asset_bucket_override (
    isin text PRIMARY KEY,
    bucket text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON public.asset_bucket_override FROM anon, authenticated;
GRANT ALL ON public.asset_bucket_override TO service_role;
NOTIFY pgrst, 'reload schema';

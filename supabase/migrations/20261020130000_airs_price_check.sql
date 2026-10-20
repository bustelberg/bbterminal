CREATE TABLE public.airs_price_check (
    portefeuille text PRIMARY KEY,
    checked_at timestamptz NOT NULL,
    result jsonb NOT NULL
);
ALTER TABLE public.airs_price_check ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_price_check FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_price_check TO service_role;

-- Preserve the existing overview computation and add saved checks in the same round trip.
ALTER FUNCTION public.airs_overview_payload() RENAME TO airs_overview_base_payload;
CREATE FUNCTION public.airs_overview_payload()
RETURNS TABLE (payload jsonb)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT base.payload || jsonb_build_object('price_checks',
    COALESCE((SELECT jsonb_object_agg(c.portefeuille, to_jsonb(c)) FROM airs_price_check c), '{}'::jsonb))
  FROM airs_overview_base_payload() base;
$$;
REVOKE ALL ON FUNCTION public.airs_overview_payload() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.airs_overview_payload() TO service_role;
NOTIFY pgrst, 'reload schema';

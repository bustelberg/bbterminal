CREATE TABLE public.airs_weekly_returns_bundle (
  portefeuille text PRIMARY KEY,
  prepared_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
ALTER TABLE public.airs_weekly_returns_bundle ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_weekly_returns_bundle FROM anon, authenticated;
GRANT ALL ON public.airs_weekly_returns_bundle TO service_role;
NOTIFY pgrst, 'reload schema';

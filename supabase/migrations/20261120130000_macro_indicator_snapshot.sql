CREATE TABLE IF NOT EXISTS public.macro_indicator_snapshot (
  key text NOT NULL,
  year integer NOT NULL,
  points jsonb NOT NULL CHECK (jsonb_typeof(points) = 'array'),
  fetched_at timestamptz NOT NULL,
  PRIMARY KEY (key, year)
);
ALTER TABLE public.macro_indicator_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.macro_indicator_snapshot FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.macro_indicator_snapshot TO service_role;
NOTIFY pgrst, 'reload schema';

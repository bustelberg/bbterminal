CREATE TABLE public.benchmark_factsheet (
  key text PRIMARY KEY,
  prepared_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
ALTER TABLE public.benchmark_factsheet ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.benchmark_factsheet FROM anon, authenticated;
GRANT ALL ON public.benchmark_factsheet TO service_role;
NOTIFY pgrst, 'reload schema';

CREATE TABLE IF NOT EXISTS public.leonteq_performance_snapshot (
  account_key text NOT NULL,
  portfolio_name text NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  retrieved_at timestamptz NOT NULL,
  PRIMARY KEY (account_key, portfolio_name)
);
ALTER TABLE public.leonteq_performance_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_performance_snapshot FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.leonteq_performance_snapshot TO service_role;
NOTIFY pgrst, 'reload schema';

CREATE TABLE IF NOT EXISTS public.leonteq_portfolio_snapshot (
  account_key text PRIMARY KEY,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  retrieved_at timestamptz NOT NULL
);
ALTER TABLE public.leonteq_portfolio_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_portfolio_snapshot FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.leonteq_portfolio_snapshot TO service_role;
NOTIFY pgrst, 'reload schema';

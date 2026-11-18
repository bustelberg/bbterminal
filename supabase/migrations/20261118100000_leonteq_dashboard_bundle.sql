CREATE TABLE public.leonteq_dashboard_bundle (
  account_key text PRIMARY KEY,
  report_month text NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'array')
);
ALTER TABLE public.leonteq_dashboard_bundle ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_dashboard_bundle FROM anon, authenticated;
GRANT ALL ON public.leonteq_dashboard_bundle TO service_role;
NOTIFY pgrst, 'reload schema';

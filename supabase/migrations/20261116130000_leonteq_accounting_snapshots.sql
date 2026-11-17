CREATE TABLE IF NOT EXISTS public.leonteq_accounting_snapshot (
  account_key text NOT NULL,
  portfolio_name text NOT NULL,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  retrieved_at timestamptz NOT NULL,
  PRIMARY KEY (account_key, portfolio_name)
);
ALTER TABLE public.leonteq_accounting_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_accounting_snapshot FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.leonteq_accounting_snapshot TO service_role;
DROP TRIGGER IF EXISTS analyse_source_changed ON public.leonteq_accounting_snapshot;
CREATE TRIGGER analyse_source_changed
  AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.leonteq_accounting_snapshot
  FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision('holdings','risk');
NOTIFY pgrst, 'reload schema';

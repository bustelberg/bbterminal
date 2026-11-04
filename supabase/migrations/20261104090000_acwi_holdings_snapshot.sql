CREATE TABLE IF NOT EXISTS public.acwi_holdings_snapshot (
  as_of date PRIMARY KEY,
  source_url text NOT NULL,
  source_sha256 text NOT NULL,
  holdings jsonb NOT NULL CHECK (jsonb_typeof(holdings) = 'array' AND jsonb_array_length(holdings) >= 1000),
  resolved_count integer NOT NULL CHECK (resolved_count > 0),
  imported_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE public.acwi_holdings_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.acwi_holdings_snapshot FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE ON public.acwi_holdings_snapshot TO service_role;

CREATE TRIGGER acwi_snapshot_source_changed
  AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.acwi_holdings_snapshot
  FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision('acwi', 'holdings', 'risk');

NOTIFY pgrst, 'reload schema';

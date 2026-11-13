CREATE TABLE public.portfolio_valuation_bundle (
  portfolio_id bigint PRIMARY KEY REFERENCES public.airs_model_portfolio(id) ON DELETE CASCADE,
  prepared_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);
ALTER TABLE public.portfolio_valuation_bundle ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.portfolio_valuation_bundle FROM anon, authenticated;
GRANT ALL ON public.portfolio_valuation_bundle TO service_role;
NOTIFY pgrst, 'reload schema';

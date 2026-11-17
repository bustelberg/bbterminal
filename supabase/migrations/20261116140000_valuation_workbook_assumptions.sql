CREATE TABLE public.portfolio_valuation_assumptions (
  portfolio_id bigint PRIMARY KEY REFERENCES public.airs_model_portfolio(id) ON DELETE CASCADE,
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.portfolio_valuation_assumptions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.portfolio_valuation_assumptions FROM anon, authenticated;
GRANT ALL ON public.portfolio_valuation_assumptions TO service_role;
NOTIFY pgrst, 'reload schema';

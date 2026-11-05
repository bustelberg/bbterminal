CREATE TABLE IF NOT EXISTS public.leonteq_airs_portfolio_link (
  account_key text NOT NULL,
  leonteq_name text NOT NULL,
  airs_portfolio_id integer NOT NULL REFERENCES public.airs_model_portfolio(id),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_key, leonteq_name, airs_portfolio_id)
);
ALTER TABLE public.leonteq_airs_portfolio_link ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_airs_portfolio_link FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.leonteq_airs_portfolio_link TO service_role;
NOTIFY pgrst, 'reload schema';

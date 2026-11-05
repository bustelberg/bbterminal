CREATE TABLE IF NOT EXISTS public.leonteq_holding_connection (
  account_key text NOT NULL,
  leonteq_name text NOT NULL,
  airs_portfolio_id integer NOT NULL,
  leonteq_key text NOT NULL CHECK (length(leonteq_key) BETWEEN 1 AND 512),
  airs_key text CHECK (length(airs_key) BETWEEN 1 AND 512),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_key, leonteq_name, airs_portfolio_id, leonteq_key),
  UNIQUE (account_key, leonteq_name, airs_portfolio_id, airs_key),
  FOREIGN KEY (account_key, leonteq_name, airs_portfolio_id)
    REFERENCES public.leonteq_airs_portfolio_link(account_key, leonteq_name, airs_portfolio_id) ON DELETE CASCADE
);
ALTER TABLE public.leonteq_holding_connection ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.leonteq_holding_connection FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.leonteq_holding_connection TO service_role;
NOTIFY pgrst, 'reload schema';

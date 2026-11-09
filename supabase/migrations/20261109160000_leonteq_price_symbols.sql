CREATE TABLE public.leonteq_price_symbol (
    isin text NOT NULL,
    currency text NOT NULL,
    symbol text NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (isin, currency)
);
ALTER TABLE public.leonteq_price_symbol ENABLE ROW LEVEL SECURITY;
GRANT ALL ON public.leonteq_price_symbol TO service_role;
NOTIFY pgrst, 'reload schema';

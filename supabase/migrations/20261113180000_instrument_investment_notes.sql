-- Notes do not require a vendor fundamentals/company record to exist.
CREATE TABLE public.instrument_investment_note (
  isin text NOT NULL,
  user_id uuid NOT NULL,
  user_email text,
  thesis text NOT NULL DEFAULT '',
  pillars jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(pillars) = 'array'),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (isin, user_id)
);
ALTER TABLE public.instrument_investment_note ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.instrument_investment_note FROM anon, authenticated;
GRANT ALL ON public.instrument_investment_note TO service_role;
NOTIFY pgrst, 'reload schema';

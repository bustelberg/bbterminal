-- Instrument-only historical loading must not publish a partial portfolio snapshot.
CREATE TABLE public.airs_weekly_instrument_snapshot (
  portefeuille text NOT NULL,
  instrument_id text NOT NULL,
  target_date date NOT NULL,
  as_of_date date,
  holdings jsonb NOT NULL CHECK (jsonb_typeof(holdings)='array' AND jsonb_array_length(holdings)<=1),
  status text NOT NULL CHECK (status IN ('observed','unavailable')),
  checked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(portefeuille,instrument_id,target_date),
  CHECK ((status='observed' AND as_of_date IS NOT NULL) OR (status='unavailable' AND as_of_date IS NULL)),
  CHECK (as_of_date IS NULL OR as_of_date BETWEEN date_trunc('week',target_date)::date AND target_date)
);
ALTER TABLE public.airs_weekly_instrument_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_weekly_instrument_snapshot FROM anon,authenticated;
GRANT ALL ON public.airs_weekly_instrument_snapshot TO service_role;
NOTIFY pgrst,'reload schema';

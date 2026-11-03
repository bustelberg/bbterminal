-- One complete AIRS quote snapshot per account/week. Daily captures advance the
-- current week; historical downloads cannot overwrite a newer observation.
CREATE TABLE public.airs_weekly_holding_snapshot (
  portefeuille text NOT NULL,
  week_end date NOT NULL,
  as_of_date date NOT NULL,
  holdings jsonb NOT NULL CHECK (jsonb_typeof(holdings) = 'array'),
  captured_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (portefeuille, week_end),
  CHECK (extract(isodow FROM week_end) = 5),
  CHECK (as_of_date BETWEEN week_end - 4 AND week_end + 2)
);
ALTER TABLE public.airs_weekly_holding_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_weekly_holding_snapshot FROM anon, authenticated;
GRANT ALL ON public.airs_weekly_holding_snapshot TO service_role;

CREATE FUNCTION public.save_airs_weekly_holding_snapshot(
  p_portefeuille text, p_as_of date, p_holdings jsonb
) RETURNS void LANGUAGE sql SET search_path = public AS $$
  INSERT INTO public.airs_weekly_holding_snapshot(portefeuille,week_end,as_of_date,holdings)
  VALUES(p_portefeuille,date_trunc('week',p_as_of)::date+4,p_as_of,p_holdings)
  ON CONFLICT(portefeuille,week_end) DO UPDATE SET
    as_of_date=EXCLUDED.as_of_date, holdings=EXCLUDED.holdings, captured_at=now()
  WHERE EXCLUDED.as_of_date >= airs_weekly_holding_snapshot.as_of_date;
$$;
REVOKE ALL ON FUNCTION public.save_airs_weekly_holding_snapshot(text,date,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.save_airs_weekly_holding_snapshot(text,date,jsonb) TO service_role;

-- Reuse real saved AIRS observations, including instruments since sold.
WITH cutoff AS (
  SELECT (now() AT TIME ZONE 'Europe/Amsterdam')::date-1 AS day
), dates AS (
  SELECT portefeuille,date_trunc('week',as_of_date)::date+4 AS week_end,max(as_of_date) AS as_of_date
  FROM public.airs_holding CROSS JOIN cutoff
  WHERE as_of_date <= cutoff.day-greatest(extract(isodow FROM cutoff.day)::integer-5,0)
  GROUP BY 1,2
)
INSERT INTO public.airs_weekly_holding_snapshot(portefeuille,week_end,as_of_date,holdings,captured_at)
SELECT d.portefeuille,d.week_end,d.as_of_date,
  jsonb_agg(jsonb_build_object('isin',h.isin,'holding_name',h.holding_name,
    'currency',h.currency,'current_price_local',h.current_price_local,
    'quantity',h.quantity,'current_value_eur',h.current_value_eur,
    'valuation_component',to_jsonb(h)->'valuation_component')),
  coalesce(max(h.retrieved_at),now())
FROM dates d JOIN public.airs_holding h USING(portefeuille,as_of_date)
GROUP BY d.portefeuille,d.week_end,d.as_of_date
ON CONFLICT DO NOTHING;
NOTIFY pgrst, 'reload schema';

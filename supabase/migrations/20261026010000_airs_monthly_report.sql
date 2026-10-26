-- Historical report bundles are separate from the current account snapshots.
CREATE TABLE IF NOT EXISTS public.airs_monthly_report (
    portefeuille text NOT NULL,
    month date NOT NULL CHECK (extract(day FROM month) = 1),
    datum_tot date NOT NULL CHECK (datum_tot >= month AND datum_tot < (month + interval '1 month')),
    fetched_at timestamptz NOT NULL DEFAULT now(),
    performance jsonb,
    report jsonb NOT NULL,
    PRIMARY KEY (portefeuille, month)
);
ALTER TABLE public.airs_monthly_report ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_monthly_report FROM anon, authenticated;
GRANT ALL ON public.airs_monthly_report TO service_role;
NOTIFY pgrst, 'reload schema';

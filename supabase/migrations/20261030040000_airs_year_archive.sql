ALTER TABLE public.airs_monthly_report ADD COLUMN performance_basis text;
UPDATE public.airs_monthly_report SET performance_basis = report->>'performance_basis';

-- Live caches remain single-window. Archives retain each year's actual source coverage.
CREATE TABLE public.airs_year_archive (
    portefeuille text NOT NULL,
    year integer NOT NULL,
    kind text NOT NULL CHECK (kind IN ('transactions', 'journal', 'year_end')),
    datum_van date NOT NULL,
    datum_tot date NOT NULL,
    payload jsonb NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (portefeuille, year, kind),
    CHECK (datum_van <= datum_tot AND extract(year FROM datum_van) = year
           AND extract(year FROM datum_tot) = year)
);
ALTER TABLE public.airs_year_archive ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.airs_year_archive FROM anon, authenticated;
GRANT ALL ON public.airs_year_archive TO service_role;

-- Concurrent or narrower reads must not replace a more complete source window.
CREATE FUNCTION public.airs_archive_no_regression() RETURNS trigger
LANGUAGE plpgsql SET search_path = public AS $$
BEGIN
    IF NEW.datum_tot < OLD.datum_tot OR NEW.datum_van > OLD.datum_van
       OR (OLD.kind = 'year_end' AND OLD.payload->>'complete' = 'true') THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER airs_archive_no_regression BEFORE UPDATE ON public.airs_year_archive
FOR EACH ROW EXECUTE FUNCTION public.airs_archive_no_regression();

INSERT INTO public.airs_year_archive(portefeuille, year, kind, datum_van, datum_tot, payload, fetched_at)
SELECT portefeuille, extract(year FROM datum_van)::integer, 'transactions', datum_van, datum_tot,
       jsonb_build_object('columns', columns, 'kinds', kinds, 'rows', rows), fetched_at
FROM public.airs_transactie_snapshot
WHERE extract(year FROM datum_van) = extract(year FROM datum_tot)
ON CONFLICT DO NOTHING;
-- Existing journals have no requested-window metadata. Preserve their rows,
-- explicitly without claiming full-year coverage. The finaliser replaces them
-- with a measured January--December export.
INSERT INTO public.airs_year_archive(portefeuille, year, kind, datum_van, datum_tot, payload)
SELECT portefeuille, extract(year FROM boekdatum)::integer, 'journal', min(boekdatum), max(boekdatum),
       jsonb_build_object('rows', jsonb_agg(to_jsonb(m)), 'coverage_unknown', true)
FROM public.airs_mutatie m WHERE boekdatum IS NOT NULL
GROUP BY portefeuille, extract(year FROM boekdatum)
ON CONFLICT DO NOTHING;

-- A daily preparation using new-year live caches cannot replace a closed-year bundle.
CREATE FUNCTION public.airs_closed_month_no_overwrite() RETURNS trigger
LANGUAGE plpgsql SET search_path = public AS $$
BEGIN
    IF extract(month FROM OLD.month) = 12 AND EXISTS (SELECT 1 FROM public.airs_year_archive a
               WHERE a.portefeuille = OLD.portefeuille AND a.year = extract(year FROM OLD.month)
                 AND a.kind = 'year_end' AND a.payload->>'complete' = 'true') THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER airs_closed_month_no_overwrite BEFORE UPDATE ON public.airs_monthly_report
FOR EACH ROW EXECUTE FUNCTION public.airs_closed_month_no_overwrite();
NOTIFY pgrst, 'reload schema';

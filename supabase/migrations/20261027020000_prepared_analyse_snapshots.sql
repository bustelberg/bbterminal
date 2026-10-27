-- Prepared results are private service-side caches, never a source of truth.
CREATE TABLE IF NOT EXISTS public.analyse_source_revision (
    scope text PRIMARY KEY CHECK (scope IN ('holdings', 'acwi')),
    version bigint NOT NULL DEFAULT 1,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO public.analyse_source_revision(scope) VALUES ('holdings'), ('acwi') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS public.analyse_prepared_snapshot (
    cache_key text PRIMARY KEY,
    scope text NOT NULL REFERENCES public.analyse_source_revision(scope),
    source_version bigint NOT NULL,
    code_version text NOT NULL,
    calculation_day date NOT NULL,
    prepared_at timestamptz NOT NULL DEFAULT now(),
    payload jsonb NOT NULL
);
ALTER TABLE public.analyse_source_revision ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.analyse_prepared_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.analyse_source_revision, public.analyse_prepared_snapshot FROM anon, authenticated;
GRANT ALL ON public.analyse_source_revision, public.analyse_prepared_snapshot TO service_role;

CREATE OR REPLACE FUNCTION public.bump_analyse_source_revision()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    -- Every writer locks holdings before ACWI, including transactions touching several
    -- source tables. A stable order avoids deadlocks between price and AIRS refreshes.
    PERFORM 1 FROM public.analyse_source_revision WHERE scope = ANY(TG_ARGV)
      ORDER BY CASE WHEN scope = 'holdings' THEN 0 ELSE 1 END FOR UPDATE;
    UPDATE public.analyse_source_revision
       SET version = version + 1, changed_at = clock_timestamp()
     WHERE scope = ANY(TG_ARGV);
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION public.bump_analyse_source_revision() FROM PUBLIC;

-- Statement triggers invalidate atomically with the source transaction, including deletes
-- and truncates. They do not depend on eventually refreshed pg_stat counters.
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'asset_price', 'asset_execution', 'asset_analysis', 'asset_isin_alias', 'fx_rate',
    'country', 'company', 'company_sector_override', 'universe', 'universe_membership',
    'index_file_membership', 'universe_asset_membership', 'asset_bucket_override'
  ] LOOP
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = table_name AND c.relkind IN ('r','p')) THEN
      EXECUTE format('DROP TRIGGER IF EXISTS analyse_source_changed ON public.%I', table_name);
      EXECUTE format('CREATE TRIGGER analyse_source_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision(''holdings'',''acwi'')', table_name);
    END IF;
  END LOOP;
  FOREACH table_name IN ARRAY ARRAY[
    'airs_model_portfolio', 'airs_model_portfolio_position', 'airs_model_portfolio_link',
    'airs_holding', 'airs_holding_isin_override', 'airs_mutatie', 'airs_model_weight',
    'airs_allocation_band', 'airs_account_model_link', 'airs_account_display_name',
    'airs_transactie_snapshot', 'airs_performance', 'airs_account_roster',
    'benchmark', 'benchmark_price', 'relative_momentum'
  ] LOOP
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = table_name AND c.relkind IN ('r','p')) THEN
      EXECUTE format('DROP TRIGGER IF EXISTS analyse_source_changed ON public.%I', table_name);
      EXECUTE format('CREATE TRIGGER analyse_source_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision(''holdings'')', table_name);
    END IF;
  END LOOP;
END $$;
NOTIFY pgrst, 'reload schema';

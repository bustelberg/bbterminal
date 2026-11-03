-- Risk weights are already part of the request identity. A different account's
-- AIRS holdings, transactions or display-name update cannot invalidate them.
INSERT INTO public.analyse_source_revision(scope) VALUES ('risk') ON CONFLICT DO NOTHING;

-- Keep existing Analyse invalidation triggers; add a dedicated market/identity
-- revision for prepared Risk, bumped atomically with real source transactions.
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'asset_price', 'asset_execution', 'asset_analysis', 'asset_isin_alias', 'fx_rate',
    'country', 'company', 'company_sector_override', 'universe', 'universe_membership',
    'index_file_membership', 'universe_asset_membership', 'asset_bucket_override',
    'benchmark', 'benchmark_price'
  ] LOOP
    IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'public' AND c.relname = table_name AND c.relkind IN ('r','p')) THEN
      EXECUTE format('DROP TRIGGER IF EXISTS risk_source_changed ON public.%I', table_name);
      EXECUTE format('CREATE TRIGGER risk_source_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision(''risk'')', table_name);
    END IF;
  END LOOP;
END $$;
NOTIFY pgrst, 'reload schema';

-- Leonteq now owns TopSelecties constituents, including certificate look-through.
-- Invalidate prepared holdings and risk when any Leonteq source snapshot changes.
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'leonteq_portfolio_snapshot', 'leonteq_composition_snapshot', 'leonteq_performance_snapshot'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS analyse_source_changed ON public.%I', table_name);
    EXECUTE format('CREATE TRIGGER analyse_source_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.bump_analyse_source_revision(''holdings'',''risk'')', table_name);
  END LOOP;
  UPDATE public.analyse_source_revision
     SET version = version + 1, changed_at = clock_timestamp()
   WHERE scope IN ('holdings', 'risk');
END $$;

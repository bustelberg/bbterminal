-- Dashboard fallback needs return metadata and only the verified closing valuation.
-- Do not transfer all dated holdings, transactions and attribution reports to Python.
CREATE OR REPLACE FUNCTION public.leonteq_dashboard_accounting_inputs(
  p_account_key text, p_names text[]
)
RETURNS TABLE (portfolio_name text, payload jsonb)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path = public AS $$
  SELECT s.portfolio_name, jsonb_build_object(
    'version', s.payload->'version',
    'success', s.payload->'success',
    'result', s.payload->'result',
    'history', s.payload->'history',
    'index_details', s.payload->'index_details',
    'retrieved_at', s.payload->'retrieved_at',
    'valuations', CASE WHEN s.payload #>> '{result,to}' IS NOT NULL
      THEN jsonb_build_object(s.payload #>> '{result,to}',
        s.payload->'valuations'->(s.payload #>> '{result,to}'))
      ELSE '{}'::jsonb END
  )
  FROM public.leonteq_accounting_snapshot s
  WHERE s.account_key = p_account_key AND s.portfolio_name = ANY(p_names);
$$;
REVOKE ALL ON FUNCTION public.leonteq_dashboard_accounting_inputs(text, text[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.leonteq_dashboard_accounting_inputs(text, text[]) TO service_role;
NOTIFY pgrst, 'reload schema';

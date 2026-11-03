-- Return only the prepared statistics needed by this chart comparison. Large window lookup
-- tables stay in PostgreSQL; series and selected statistics come from the same snapshot row.
CREATE OR REPLACE FUNCTION public.read_benchmark_chart_snapshot(
  p_label text, p_cadence text, p_version integer, p_own_periods jsonb DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path = public AS $$
DECLARE
  v_payload jsonb;
  v_stats jsonb := '{}'::jsonb;
  v_metric text;
  v_prepared jsonb;
  v_selected jsonb;
  v_low double precision;
  v_high double precision;
BEGIN
  SELECT payload INTO v_payload FROM public.benchmark_chart_snapshot
    WHERE label = p_label AND cadence = p_cadence AND version = p_version;
  IF v_payload IS NULL THEN RETURN NULL; END IF;
  FOR v_metric, v_prepared IN SELECT key, value FROM jsonb_each(v_payload #> '{growth,chart_statistics}') LOOP
    v_selected := v_prepared->'full';
    IF p_own_periods IS NOT NULL THEN
      SELECT min((point->>'year')::double precision), max((point->>'year')::double precision)
        INTO v_low, v_high
        FROM jsonb_array_elements(v_prepared->'points') point
        WHERE p_own_periods->v_metric @> jsonb_build_array((point->>'year')::double precision);
      IF v_low IS NOT NULL THEN
        v_selected := v_prepared->'windows'->(v_low::text || ':' || v_high::text);
      END IF;
    END IF;
    IF v_selected IS NULL THEN RAISE EXCEPTION 'Incomplete saved statistics for %', v_metric; END IF;
    v_stats := v_stats || jsonb_build_object(v_metric, v_selected);
  END LOOP;
  v_payload := v_payload || jsonb_build_object('growth',
    (v_payload->'growth') || jsonb_build_object('chart_statistics', v_stats));
  IF p_own_periods IS NOT NULL THEN
    RETURN jsonb_build_object('growth', v_payload->'growth', 'built_at', v_payload->'built_at');
  END IF;
  RETURN v_payload;
END;
$$;
REVOKE ALL ON FUNCTION public.read_benchmark_chart_snapshot(text,text,integer,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.read_benchmark_chart_snapshot(text,text,integer,jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';

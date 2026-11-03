CREATE TABLE IF NOT EXISTS public.benchmark_chart_snapshot (
  label text NOT NULL,
  cadence text NOT NULL CHECK (cadence IN ('annual', 'quarterly')),
  version integer NOT NULL,
  started_at timestamptz NOT NULL,
  built_at timestamptz NOT NULL,
  payload jsonb NOT NULL,
  PRIMARY KEY (label, cadence, version)
);
ALTER TABLE public.benchmark_chart_snapshot ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.benchmark_chart_snapshot FROM anon, authenticated;
GRANT ALL ON public.benchmark_chart_snapshot TO service_role;

CREATE OR REPLACE FUNCTION public.publish_benchmark_chart_snapshot(
  p_label text, p_cadence text, p_version integer, p_started_at timestamptz,
  p_built_at timestamptz, p_payload jsonb
) RETURNS void LANGUAGE sql SET search_path = public AS $$
  INSERT INTO public.benchmark_chart_snapshot(label, cadence, version, started_at, built_at, payload)
  VALUES (p_label, p_cadence, p_version, p_started_at, p_built_at, p_payload)
  ON CONFLICT (label, cadence, version) DO UPDATE SET
    started_at = EXCLUDED.started_at, built_at = EXCLUDED.built_at, payload = EXCLUDED.payload
  WHERE EXCLUDED.started_at >= benchmark_chart_snapshot.started_at;
$$;
REVOKE ALL ON FUNCTION public.publish_benchmark_chart_snapshot(text,text,integer,timestamptz,timestamptz,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.publish_benchmark_chart_snapshot(text,text,integer,timestamptz,timestamptz,jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';

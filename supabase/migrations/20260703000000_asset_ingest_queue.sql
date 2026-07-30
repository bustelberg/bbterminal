-- Async ingest queue for the asset pipeline. Uploading a CSV now just writes the
-- ISINs here as `pending` (instant); a SINGLE in-process background worker (the
-- only Yahoo/OpenFIGI consumer) drains the queue through the throttled resolver,
-- so nothing competes for the Yahoo throttle and resolutions never run on
-- throttle-degraded data.
--
--   status: pending  → not yet processed
--           done     → resolved + stored (or cleanly recorded unmapped)
--           failed   → errored (kept with the reason; re-queue to retry)

CREATE TABLE IF NOT EXISTS public.asset_ingest_queue (
    isin        text PRIMARY KEY,
    status      text NOT NULL DEFAULT 'pending',   -- pending | done | failed
    attempts    integer NOT NULL DEFAULT 0,
    reason      text,
    added_at    timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- The worker polls WHERE status='pending' every tick — index it.
CREATE INDEX IF NOT EXISTS asset_ingest_queue_status_idx ON public.asset_ingest_queue(status);

ALTER TABLE public.asset_ingest_queue ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS asset_ingest_queue_deny_all ON public.asset_ingest_queue;
CREATE POLICY asset_ingest_queue_deny_all ON public.asset_ingest_queue FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_ingest_queue TO service_role;

NOTIFY pgrst, 'reload schema';

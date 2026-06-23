-- Raw daily snapshot of the AIRS CRM "Alle relaties" Excel export. Stored
-- UNPARSED (the whole .xls, base64-encoded in a text column) so another site
-- can read it straight from Supabase and decode the original file. One row per
-- working day; the daily AIRS job upserts (no duplicate per date).
--
-- base64 in a `text` column (not bytea) keeps inserts/reads simple across the
-- stack — supabase-py inserts a plain string, any client base64-decodes it.

CREATE TABLE IF NOT EXISTS airs_crm_relaties_raw (
    as_of_date      date PRIMARY KEY,
    filename        text,
    content_base64  text NOT NULL,        -- the raw .xls, base64-encoded
    byte_size       integer,              -- decoded size, for a quick sanity check
    retrieved_at    timestamptz NOT NULL DEFAULT now()
);

-- RLS: deny-all (backend service key bypasses) — matches the other AIRS tables.
ALTER TABLE airs_crm_relaties_raw ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_crm_relaties_raw_deny_all ON airs_crm_relaties_raw;
CREATE POLICY airs_crm_relaties_raw_deny_all ON airs_crm_relaties_raw FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON airs_crm_relaties_raw TO service_role;

NOTIFY pgrst, 'reload schema';

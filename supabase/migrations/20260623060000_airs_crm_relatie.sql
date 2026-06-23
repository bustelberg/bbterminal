-- Human-readable, queryable form of the CRM "Alle relaties" export: one row per
-- relation per snapshot date, with typed columns (instead of the base64 .xlsx
-- blob in airs_crm_relaties_raw, which is kept only for byte-for-byte re-download).
-- The daily AIRS refresh writes BOTH: the raw blob and these parsed rows.
--
-- `extra` (jsonb) collects any Excel column not mapped to a typed column, so an
-- AIRS column add/rename is never silently lost.

CREATE TABLE IF NOT EXISTS airs_crm_relatie (
    as_of_date          date    NOT NULL,
    crm_id              integer NOT NULL,        -- AIRS internal relation id ("id")
    portefeuille        text,
    zoekveld            text,
    naam                text,
    contact_tijd        integer,
    depotbank           text,
    accountmanager      text,
    risicoklasse        text,
    model_portefeuille  text,
    laatste_waarde      numeric,
    rendement           numeric,
    rendement_qtd       numeric,
    startdatum          date,
    email               text,
    adres               text,
    plaats              text,
    land                text,
    roepnaam            text,
    achternaam          text,
    geboortedatum       date,
    part_roepnaam       text,
    part_achternaam     text,
    part_geboortedatum  date,
    extra               jsonb,                   -- any unmapped Excel columns
    retrieved_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_date, crm_id)
);

CREATE INDEX IF NOT EXISTS airs_crm_relatie_portefeuille_idx
    ON airs_crm_relatie (portefeuille);

-- RLS: deny-all (backend service key bypasses) — matches the other AIRS tables.
ALTER TABLE airs_crm_relatie ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_crm_relatie_deny_all ON airs_crm_relatie;
CREATE POLICY airs_crm_relatie_deny_all ON airs_crm_relatie FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON airs_crm_relatie TO service_role;

NOTIFY pgrst, 'reload schema';

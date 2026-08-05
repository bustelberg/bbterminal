-- The AIRS Transacties report (`rapport_types=TRANS`): what a book actually BOUGHT and SOLD.
--
-- VOLK says what a book holds, MUT what it earned, MODEL what its strategy asks for. None of them
-- says what it DID — so a position that appeared mid-year, one that was sold out entirely, and a
-- weight that drifted purely because the market moved are indistinguishable from the outside.
--
-- ⚠ STORED AS THE SHEET, NOT AS COLUMNS, AND THAT IS DELIBERATE. `TRANS` was probed on 2026-07-23
-- and returns an XLS, but no column of it has ever been measured. Every other AIRS table here
-- (airs_mutatie, airs_holding, airs_model_weight) names its columns because somebody read the
-- report first. Writing a typed schema against a sheet nobody has opened would commit the one
-- mistake this codebase keeps re-learning — `Bedrag` vs `Bedrag eur` is a single word apart and
-- decides whether a figure carries the FX leg, and the wrong pick yields a plausible number
-- rather than an error.
--
-- So the parsed sheet lands here verbatim: `columns` in the report's own order, `kinds` per
-- column, `rows` as an array of objects keyed by column name. Nothing is dropped, nothing is
-- renamed, nothing is interpreted.
--
-- ⚠ THIS IS A STARTING POINT, NOT THE DESTINATION. Once the real sheet is on screen, the columns
-- that matter get promoted to a typed `airs_transactie` table with real semantics (signs, the
-- currency leg, the join to a holding) — and those ⚠ comments can then be written from
-- measurement instead of from hope. Until then, a blob that is honestly a blob beats a schema
-- that is confidently wrong.
--
-- ⚠ ONE ROW PER ACCOUNT — THE NEWEST FETCH, delete-then-insert on write. Same rule as the model
-- portfolios' position cache: we hold ONE snapshot, so a transaction removed upstream actually
-- disappears rather than lingering from an earlier run. `datum_van`/`datum_tot` are the window
-- that was ASKED FOR, stored beside the rows, because a transaction list is only meaningful
-- against the period it covers — a cached year-to-date served as though it were a full history
-- is the same lie as a stale price shown as fresh.
CREATE TABLE IF NOT EXISTS airs_transactie_snapshot (
    portefeuille  text PRIMARY KEY,
    datum_van     date NOT NULL,        -- the window requested, not the first row's date
    datum_tot     date NOT NULL,
    columns       jsonb NOT NULL DEFAULT '[]'::jsonb,   -- the sheet's own column order
    kinds         jsonb NOT NULL DEFAULT '{}'::jsonb,   -- column -> 'number' | 'date' | 'text'
    rows          jsonb NOT NULL DEFAULT '[]'::jsonb,   -- [{column: value}], values never NaN
    -- ⚠ A COUNT COLUMN WOULD BE A SECOND SOURCE OF TRUTH for something `jsonb_array_length(rows)`
    -- already answers, and this repo's rule is that a count is a VIEW, never a column. Absent on
    -- purpose.
    fetched_at    timestamptz NOT NULL DEFAULT now()
);

-- ⚠ ZERO ROWS IS AN ANSWER, NOT A MISS. A book that has not traded this year has an empty
-- Transacties report, and a cached empty row is what stops the UI re-asking AIRS (seconds, behind
-- a headless browser) every single time somebody expands it. Distinguishing "no snapshot stored"
-- from "stored, and it was empty" is the whole reason this row exists even when `rows` is `[]`.

ALTER TABLE airs_transactie_snapshot ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_transactie_snapshot_deny_all ON airs_transactie_snapshot;
CREATE POLICY airs_transactie_snapshot_deny_all ON airs_transactie_snapshot FOR ALL USING (false);

NOTIFY pgrst, 'reload schema';

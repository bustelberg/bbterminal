-- An AIRS account that should not appear in the portfolios list.
--
-- WHY A TABLE AND NOT A DELETE
--   The list is built from `airs_performance`, which the daily scrape appends to. Deleting the
--   rows would remove real history AND achieve nothing: the next scrape that still sees the
--   account puts them straight back. And when AIRS drops an account for good, the opposite
--   happens — the scrape simply stops mentioning it while every row it ever wrote stays behind,
--   so the account lingers in the list for ever with a frozen snapshot. Neither case is fixed by
--   touching the data; both are fixed by recording a DECISION about it.
--
--   Same shape and same reason as `company_override` kind='exclude', which marks an
--   unwanted-but-real index constituent so it stays suppressed after the next reconstruction
--   re-creates it.
--
-- ⚠ HIDDEN, NOT DELETED. The performance rows, holdings snapshots and links are untouched, so
--   un-hiding is one DELETE and nothing had to be re-scraped. A row here is an editorial choice
--   about the LIST, never a claim that the account did not exist.
CREATE TABLE IF NOT EXISTS public.airs_account_hidden (
    portefeuille text PRIMARY KEY,
    -- WHY it is hidden. Without this, a year from now the row is indistinguishable from a bug.
    note         text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- AIRS's own spelling is stored, but two rows differing only in case or padding would be two
-- conflicting answers to one question — the same rule `airs_holding_isin_override` follows.
CREATE UNIQUE INDEX IF NOT EXISTS airs_account_hidden_key
    ON public.airs_account_hidden (lower(btrim(portefeuille)));

ALTER TABLE public.airs_account_hidden ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_account_hidden_deny_all ON public.airs_account_hidden;
CREATE POLICY airs_account_hidden_deny_all ON public.airs_account_hidden FOR ALL USING (false);

REVOKE ALL ON public.airs_account_hidden FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_account_hidden TO service_role;

-- ⚠ SEEDED WITH NOTHING, DELIBERATELY. TOPS_NEU_BEH_DYN was hidden here first, before we knew
-- WHY it looked broken: AIRS had stopped listing it, and `airs_performance` kept it visible
-- because that table is append-only. `airs_account_roster` (migration 20260724020000) now
-- answers "does AIRS still list this" from the discovery pass itself, so a manual hide for the
-- same fact would be a second source of truth — and the one that cannot notice a re-activation.
--
-- This table is for the OTHER case: an account AIRS does still list, that you do not want in the
-- table. Nothing currently qualifies.

NOTIFY pgrst, 'reload schema';

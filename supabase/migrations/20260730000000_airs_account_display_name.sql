-- A human-chosen name for an AIRS ACCOUNT.
--
-- WHY A SECOND PLACE FOR A NAME
--   `airs_model_portfolio.display_name` already exists, and it names a MODEL. An account is named
--   through its pairing with one — so a book paired with no model had no way to be named at all,
--   and fell back to AIRS's own code (`BUS_Ris_bepOff_Kl_AFS_Dy`). That is exactly backwards: the
--   books most in need of a readable name are the ones nothing else names.
--
--   It is also the wrong OBJECT. A nickname is a fact about this book, not about the strategy it
--   happens to run; two accounts running one model may deserve different names, and renaming the
--   model should not silently rename every account paired with it.
--
-- ⚠ IT WINS OVER THE MODEL'S NAME, AND THAT IS THE POINT. A human typed it for this row; the
--   model's `display_name` is a name for something else that we borrow when nothing better exists.
--   Precedence is account name > model display_name > model name > AIRS code, and every step down
--   that chain is a fallback, never a preference.
--
-- ⚠ HIDDEN NOWHERE, DELETED NEVER. Clearing the nickname is a DELETE of this row, which restores
--   the fallback chain — the same shape as `airs_account_hidden`: an editorial decision, stored
--   apart from the scraped data so a re-scrape cannot overwrite it and a delete cannot lose it.
CREATE TABLE IF NOT EXISTS public.airs_account_display_name (
    portefeuille text PRIMARY KEY,
    display_name text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- AIRS's own spelling is stored, but two rows differing only in case or padding would be two
-- conflicting answers to one question — the rule `airs_account_hidden` and
-- `airs_holding_isin_override` both follow.
CREATE UNIQUE INDEX IF NOT EXISTS airs_account_display_name_key
    ON public.airs_account_display_name (lower(btrim(portefeuille)));

ALTER TABLE public.airs_account_display_name ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS airs_account_display_name_deny_all ON public.airs_account_display_name;
CREATE POLICY airs_account_display_name_deny_all
    ON public.airs_account_display_name FOR ALL USING (false);

REVOKE ALL ON public.airs_account_display_name FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_account_display_name TO service_role;

NOTIFY pgrst, 'reload schema';

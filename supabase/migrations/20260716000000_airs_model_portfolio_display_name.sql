-- A name WE choose for a model portfolio.
--
-- WHY
--   AIRS's `Portefeuille` is a 24-char code, and that cap is AIRS's, not our truncation:
--   "BUS_BM_AAN_kw_USD_2026_d", "MoTopSelectie_FX", "TOPS_OFF_BEH". It is an identifier built to
--   survive a legacy form field, not a label anyone would choose. This column is the label — set
--   by hand, shown BESIDE the code rather than instead of it (the code is what you search for in
--   AIRS itself, so replacing it outright would cost more than it gives).
--
-- ⚠ IT IS A COLUMN, AND THAT IS ONLY SAFE BECAUSE THE SCAN UPSERTS A NAMED PAYLOAD.
--   `_airs_portfolio_store.save_portfolios` builds an EXPLICIT payload (id, name, truncated,
--   omschrijving, portfolio_type, fixed_datum, scanned_at) and upserts `on_conflict="id"`, so
--   PostgREST SETs only the columns it names and this one is untouched by a rescan.
--
--   ⚠⚠ ADDING `display_name` TO THAT PAYLOAD WOULD DESTROY EVERY CHOSEN NAME ON THE NEXT SCAN —
--   silently, irreversibly, and with no second copy to restore from. It is the one edit that must
--   never be made "for consistency". Pinned by tests/test_airs_display_name.py.
--
-- ⚠ KEYED ON THE ID, NOT THE NAME.
--   The ask was "a mapping from portfolio name to a name we choose", but `id` is AirSPMS's own
--   and is documented as the stable PK, while `name` is precisely the thing someone might edit in
--   AIRS. An alias keyed on the name would be orphaned by a rename — the row would quietly revert
--   to its code with nothing to say why. Same rule as the link table keying on the holding rather
--   than on (parent, holding): key on the fact that does not move.
--
-- ⚠ NULL IS MEANINGFUL AND IS THE DEFAULT: "no name chosen — fall back to AIRS's". It is not "".
--   An empty string is a CHOSEN name that happens to be blank; it would render an empty cell and
--   read as a bug. The API maps "" -> NULL on write for exactly that reason.
ALTER TABLE public.airs_model_portfolio
    ADD COLUMN IF NOT EXISTS display_name text;

COMMENT ON COLUMN public.airs_model_portfolio.display_name IS
    'A human-chosen label for this model. NULL = none chosen, fall back to `name` (AIRS''s '
    '24-char code). NEVER written by the scan: adding it to `save_portfolios`'' payload would '
    'wipe every chosen name on the next rescan.';

-- ⚠ THIS BODY IS COPIED FROM 20260713080000 (the LAST migration to define the view), NOT from
-- the view's original 20260713060000 — with `p.display_name` added and NOTHING else changed.
-- Rebuilding it from the original would have silently regressed four things that landed later:
-- the `positions_dates` and `no_snapshot` columns, the `positions_datum IS NULL` guard, and
-- `count(DISTINCT pos.isin)` — which is the fix for one instrument listed on two lines
-- (VTopSelectie OFF FX holds CapitaLand at 2% AND 3%; `count(*)` reported 29 resolved of 28
-- held). A DROP+CREATE from a stale template is how a fixed bug comes back.
DROP VIEW IF EXISTS public.airs_model_portfolio_grid;

CREATE VIEW public.airs_model_portfolio_grid AS
SELECT
    p.id,
    p.name,
    p.display_name,
    p.truncated,
    p.omschrijving,
    p.portfolio_type,
    p.fixed_datum,
    p.positions_datum,
    p.positions_dates,
    p.positions_scanned_at,
    p.positions_error,
    p.scanned_at,
    (p.portfolio_type ILIKE 'fixed%') AS has_fixed_model,
    (p.positions_scanned_at IS NOT NULL AND p.positions_datum IS NULL) AS no_snapshot,
    CASE
        WHEN p.portfolio_type NOT ILIKE 'fixed%' THEN NULL
        WHEN p.positions_scanned_at IS NULL THEN NULL
        WHEN p.positions_datum IS NULL THEN NULL
        -- DISTINCT ISINs: one instrument on two lines is one instrument. Cash has no ISIN.
        ELSE (
            SELECT count(DISTINCT pos.isin)
            FROM public.airs_model_portfolio_position pos
            WHERE pos.portfolio_id = p.id
              AND pos.datum = p.positions_datum
              AND pos.isin IS NOT NULL
        )
    END AS holdings
FROM public.airs_model_portfolio p;

-- A DROP resets grants, so both halves are restated. The REVOKE is the 060000 intent: the page
-- reads this through the backend (service_role), never with the anon/publishable key.
REVOKE ALL ON public.airs_model_portfolio_grid FROM anon, authenticated;
GRANT SELECT ON public.airs_model_portfolio_grid TO service_role;

NOTIFY pgrst, 'reload schema';

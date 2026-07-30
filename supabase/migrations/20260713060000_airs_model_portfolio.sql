-- The AIRS model portfolios, persisted.
--
-- WHY
--   Until now /portfolios scraped AirSPMS on every visit. The list alone is ~6s (paginated
--   HTML + one XLS for the full names), and the holdings count is another edit-page GET +
--   XLS download PER portfolio — minutes. Nothing was kept, so every page load paid it
--   again. These two tables make the page an instant DB read, with "Rescan" as the explicit
--   refresh.
--
--   Storing the POSITIONS costs no extra AIRS traffic: the count phase already downloads
--   each portfolio's XLS to count it. Throwing that away and keeping only the number would
--   discard the valuable half — `isin` is the exact join into `asset_execution`, the one the
--   AIRS *holdings* sheet never gave us (it carries only a fund name like "Alphabet - C").
--
-- WHY THE COUNT IS A VIEW AND NOT A COLUMN
--   A stored `holdings` integer is a second source of truth that can drift from the rows it
--   claims to count. `airs_model_portfolio_grid` derives it from the positions themselves,
--   so it cannot.
--
-- THE THREE STATES THAT LOOK ALIKE AND ARE NOT (the whole point of the column)
--   no model  — AirSPMS only stores a composition for a portfolio of type `fixed (…)`. A
--               `normaal` (31 of 95) or `meervoudig` (6) one — the benchmarks and
--               multi-model wrappers — has NONE. Reporting "0 holdings" would be a claim
--               about a model that does not exist.  -> has_fixed_model = false
--   not yet   — a fixed portfolio we have not counted.  -> positions_scanned_at IS NULL
--   0         — a real, EMPTY fixed model. Measured: 58 portfolios are `fixed (…)` but only
--               57 have a composition, so exactly ONE is genuinely empty. That single row is
--               why `NULL` and `0` are not allowed to collapse into each other.
--
--   ⚠ `fixed (0)` IS NOT A HOLDINGS COUNT. 24 portfolios carry that type and they hold 20,
--     9, 1… instruments. The parenthesised figure is the portfolio's own number.

CREATE TABLE IF NOT EXISTS public.airs_model_portfolio (
    -- AirSPMS's own id (the `?action=edit&id=` on every row). Stable, so it's the PK.
    id                   integer PRIMARY KEY,
    -- The FULL name. AIRS caps `Portefeuille` at 24 chars ("BUS_BM_AAN_kw_USD_2026_d") and
    -- the LIST page truncates it further with a literal "..." — `truncated` says the value
    -- here is the CLIPPED one, i.e. the edit-page fallback failed. Should always be false.
    name                 text NOT NULL,
    truncated            boolean NOT NULL DEFAULT false,
    omschrijving         text,
    -- The raw type cell: 'fixed (14.5)' / 'normaal' / 'meervoudig'.
    portfolio_type       text,
    fixed_datum          text,
    -- The snapshot date the stored positions were taken from. AirSPMS's date dropdown always
    -- LEADS with today, an empty placeholder, so this is the newest date that actually had
    -- rows — never simply options[0].
    positions_datum      text,
    -- NULL = we have never counted this portfolio. NOT the same as "it holds nothing".
    positions_scanned_at timestamptz,
    -- Set when a positions fetch FAILED. Distinguishes "we asked and it broke" from
    -- "we never asked" and from "it holds nothing" — writing 0 for any of these is a
    -- fabricated fact.
    positions_error      text,
    scanned_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.airs_model_portfolio_position (
    id           bigserial PRIMARY KEY,
    portfolio_id integer NOT NULL REFERENCES public.airs_model_portfolio(id) ON DELETE CASCADE,
    datum        text NOT NULL,
    -- NULL for the cash line ("Liquiditeiten") — cash has no ISIN, and that is CORRECT, not
    -- a missing value. It is therefore not an instrument and is never counted as one.
    isin         text,
    fonds        text,
    percentage   double precision,
    valuta       text,
    categorie    text,
    sector       text,
    regio        text
);

-- One row per instrument per snapshot. Cash (isin IS NULL) is excluded from the constraint —
-- NULLs don't collide in a unique index anyway — and is kept unique by the refresh being a
-- delete-then-insert of the whole (portfolio_id, datum) slice.
CREATE UNIQUE INDEX IF NOT EXISTS airs_mpp_portfolio_datum_isin_idx
    ON public.airs_model_portfolio_position(portfolio_id, datum, isin)
    WHERE isin IS NOT NULL;

CREATE INDEX IF NOT EXISTS airs_mpp_portfolio_idx
    ON public.airs_model_portfolio_position(portfolio_id, datum);
CREATE INDEX IF NOT EXISTS airs_mpp_isin_idx
    ON public.airs_model_portfolio_position(isin) WHERE isin IS NOT NULL;

-- The grid the /portfolios page reads. `holdings` is DERIVED from the positions, so it can
-- never disagree with them; `has_fixed_model` + `positions_scanned_at` are what let the UI
-- tell the three absences apart.
CREATE OR REPLACE VIEW public.airs_model_portfolio_grid AS
SELECT
    p.id,
    p.name,
    p.truncated,
    p.omschrijving,
    p.portfolio_type,
    p.fixed_datum,
    p.positions_datum,
    p.positions_scanned_at,
    p.positions_error,
    p.scanned_at,
    (p.portfolio_type ILIKE 'fixed%') AS has_fixed_model,
    CASE
        -- No model exists -> not zero, not unknown. There is nothing to count.
        WHEN p.portfolio_type NOT ILIKE 'fixed%' THEN NULL
        -- Never counted -> unknown. Do not invent a 0.
        WHEN p.positions_scanned_at IS NULL THEN NULL
        -- Counted. May legitimately be 0. Cash is not an instrument.
        ELSE (
            SELECT count(*)
            FROM public.airs_model_portfolio_position pos
            WHERE pos.portfolio_id = p.id
              AND pos.datum = p.positions_datum
              AND pos.isin IS NOT NULL
        )
    END AS holdings
FROM public.airs_model_portfolio p;

REVOKE ALL ON public.airs_model_portfolio FROM anon, authenticated;
REVOKE ALL ON public.airs_model_portfolio_position FROM anon, authenticated;
REVOKE ALL ON public.airs_model_portfolio_grid FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_model_portfolio TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_model_portfolio_position TO service_role;
GRANT SELECT ON public.airs_model_portfolio_grid TO service_role;
GRANT USAGE, SELECT ON SEQUENCE public.airs_model_portfolio_position_id_seq TO service_role;

NOTIFY pgrst, 'reload schema';

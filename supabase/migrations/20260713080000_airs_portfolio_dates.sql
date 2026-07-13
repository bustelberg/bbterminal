-- Serve a portfolio's positions from OUR cache instead of re-scraping AirSPMS on every
-- expand. The rows were already stored; two things were missing to be able to READ them back
-- as a complete answer.
--
-- 1. `positions_dates` — the snapshot dates AirSPMS offers for this portfolio. Without them
--    the cached response cannot populate the date picker, and the UI would silently collapse
--    to the single date we happen to hold, hiding the fact that others exist. We store ONE
--    snapshot (the newest with rows); the picker still reaches the rest, live, on demand.
--
-- 2. `positions_fetched_at` is already covered by `positions_scanned_at`, so the UI can say
--    how old the cached answer is rather than presenting it as if it were fresh.
--
-- Note we deliberately do NOT cache `known_instrument` (whether an ISIN is in
-- `asset_execution`). That is a join against a table which changes independently — every
-- ISIN we add makes a previously-unknown holding known — so it is recomputed on read. A
-- cached "not in grid" would go stale the moment the instrument grid grows.

ALTER TABLE public.airs_model_portfolio
    ADD COLUMN IF NOT EXISTS positions_dates jsonb;

DROP VIEW IF EXISTS public.airs_model_portfolio_grid;

CREATE VIEW public.airs_model_portfolio_grid AS
SELECT
    p.id,
    p.name,
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

GRANT SELECT ON public.airs_model_portfolio_grid TO service_role;

NOTIFY pgrst, 'reload schema';

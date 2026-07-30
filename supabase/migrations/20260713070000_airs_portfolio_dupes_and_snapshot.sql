-- Two things the first live scan proved wrong. Both were assumptions I made about AIRS that
-- AIRS does not honour.
--
-- 1. A PORTFOLIO CAN LIST THE SAME ISIN TWICE.
--    Measured, VTopSelectie OFF FX @ 2026-07-08:
--        CapitaLand Integr Comm Tr   SG1M51904654   2.0%   SGD
--        CapitaLand Integr Comm Tr   SG1M51904654   3.0%   SGD
--    One instrument, two lines, 5% in total. The UNIQUE(portfolio_id, datum, isin) index
--    rejected the second row — and because the insert is one batch, the WHOLE portfolio
--    failed to save and surfaced as an error. So the index goes.
--
--    But then the COUNT has to change with it: counting ROWS would report 29 instruments for
--    a portfolio that holds 28. `holdings` is now COUNT(DISTINCT isin) — an instrument held
--    on two lines is one instrument.
--
-- 2. "0 HOLDINGS" WAS NOT AN EMPTY MODEL — IT WAS NO SNAPSHOT AT ALL.
--    Two `fixed (…)` portfolios came back with zero rows, and their date dropdown contained
--    exactly ONE option: today. Today is the EMPTY PLACEHOLDER that AirSPMS always leads the
--    dropdown with (the trap `fetch_portfolio_positions_sync` probes newest-first to avoid).
--    So those portfolios have no dated composition on record at all. Reporting `0` there
--    claims they hold nothing, which we did not learn and cannot say.
--
--    (My earlier "exactly one portfolio is genuinely empty" was arithmetic coincidence —
--    58 fixed minus 57 with a composition — not an observation. In fact ZERO portfolios are
--    observed empty. The state is kept expressible anyway, because AIRS could produce one.)
--
--    `positions_datum IS NULL` while `positions_scanned_at IS NOT NULL` now means exactly:
--    we looked, and AIRS offers no dated composition. Distinct from never-looked, from a
--    real empty model, and from an error.

DROP INDEX IF EXISTS public.airs_mpp_portfolio_datum_isin_idx;

-- DROP, not CREATE OR REPLACE: `no_snapshot` is a NEW column and Postgres only lets REPLACE
-- append to the end of an unchanged column list, never insert into it.
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
    p.positions_scanned_at,
    p.positions_error,
    p.scanned_at,
    (p.portfolio_type ILIKE 'fixed%') AS has_fixed_model,
    -- We looked, and AIRS had no dated composition to give (only the empty "today"
    -- placeholder in its dropdown). Not zero, not unknown-because-unasked.
    (p.positions_scanned_at IS NOT NULL AND p.positions_datum IS NULL) AS no_snapshot,
    CASE
        -- No model exists at all (`normaal` / `meervoudig`). Nothing to count.
        WHEN p.portfolio_type NOT ILIKE 'fixed%' THEN NULL
        -- Never counted.
        WHEN p.positions_scanned_at IS NULL THEN NULL
        -- Counted, but AIRS had no dated composition — see `no_snapshot`.
        WHEN p.positions_datum IS NULL THEN NULL
        -- Counted. DISTINCT ISINs: one instrument on two lines is one instrument.
        -- Cash has no ISIN and is not an instrument. 0 here would be a real empty model.
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

-- Daily precompute for the public "relative momentum" indicator: where each company's 12-1 month
-- return ranks against the universe it competes with, as of one date.
--
-- ⚠⚠ IT IS A PRECOMPUTE BECAUSE THE COST IS THE PRICE LOAD, NOT THE STATISTICS. Measured on the
--   live 1,998-name ACWI panel:
--
--       load 13 months of closes (one COPY, 970k rows)   ~2,600 ms
--       to_panel                                             46 ms
--       12-1 + percentile + robust z + 7 buckets            2.4 ms
--
--   So computing this per request would spend ~2.6s to redo work whose answer changes once a day,
--   while the part anybody would worry about costs 2.4ms. Written once after the 05:00 UTC
--   `price_update` tick, read as one indexed lookup afterwards.
--
-- ⚠⚠ BOTH SCALES ARE STORED, AND THAT IS DELIBERATE. `pct_rank` is what a reader is shown — it
--   needs no explanation beyond "where this ranks in the universe" and it is insensitive to the
--   distribution's shape. `robust_z` is the richer number (how UNUSUALLY far from the pack, which a
--   percentile cannot express: the 99th and the 100th percentile are one rank apart whether the
--   gap is 1% or 1000%), and it is what a model should consume. Deriving either from the other is
--   impossible, so both are columns rather than one plus a convention.
--
-- ⚠⚠ AND `raw_return_pct` IS STORED SO THE UI CANNOT MISLEAD. A company at +8.0% in a market whose
--   median is +27% is genuinely WEAK relative to its peers and will render at `--`. Shown as a red
--   orb alone that reads as "this stock fell", which is false. The raw figure is kept beside the
--   rank precisely so the hover can say "+8.0% return, 14th percentile" and be honest about which
--   of the two it is ranking.
--
-- ⚠ `universe_n` IS NOT OPTIONAL. This is a RELATIVE measure, so the population it is relative to
--   is part of the reading — the same house rule as the Long Equity cards printing "n of m
--   companies". ACWI has 1,998 members and only ~1,750 of them can carry a 12-1 return, and a
--   percentile silently computed over 1,750 while a page says 1,998 is a wrong denominator nobody
--   can see.
--
-- ⚠⚠ THE SEVEN STATES HAVE FIXED POPULATIONS BY CONSTRUCTION, which is a property to state rather
--   than a bug to fix. The cut points (10/25/40/60/75/90) mean exactly 10% of the universe is
--   `+++` every single day — this indicator can never say "the whole market is strong", because it
--   does not measure that. Measured on ACWI: 113/171/170/227/171/170/114 of 1,136.
--
-- ⚠ ONE ROW PER (universe, date, company). The universe is part of the KEY, not a filter: the same
--   company ranks differently against ACWI than against the AEX, and both are legitimate answers to
--   different questions. A schema keyed on company alone would have to pick one silently.
CREATE TABLE IF NOT EXISTS public.relative_momentum (
    universe_label  text        NOT NULL,
    as_of_date      date        NOT NULL,
    company_id      integer     NOT NULL REFERENCES public.company(company_id) ON DELETE CASCADE,
    -- The signal itself, in percent, exactly as `signal_engine.daily` computes it. Never NULL: a
    -- company that cannot carry a 12-1 return is ABSENT from this table rather than present with a
    -- hole, so a reader never has to distinguish "no data" from "zero momentum".
    raw_return_pct  double precision NOT NULL,
    -- Percentile rank within (universe_label, as_of_date), 0-1, ties averaged. The display scale.
    pct_rank        double precision NOT NULL,
    -- (x - median) / (1.4826 * MAD), clipped to +/-2. The model scale. NULL only when the
    -- cross-section has no dispersion at all, which is not the same fact as a z-score of 0.
    robust_z        double precision,
    -- The seven-state bucket, derived from `pct_rank`. Stored rather than derived on read so every
    -- surface shows the same state and the cut points live in ONE place.
    state           smallint    NOT NULL CHECK (state BETWEEN -3 AND 3),
    -- How many companies the rank was computed over. See the note above.
    universe_n      integer     NOT NULL,
    computed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (universe_label, as_of_date, company_id)
);

-- The read the page makes: one universe, its newest date. The PK's leading columns already serve
-- it, so this index exists only for the OTHER read — one company's history across dates, which the
-- PK cannot answer without scanning a whole universe-date.
CREATE INDEX IF NOT EXISTS relative_momentum_company_idx
    ON public.relative_momentum (company_id, as_of_date DESC);

-- ⚠ Same posture as every other table here: the service role reads and writes it, nobody else
--   reaches it directly. The API is the only door.
ALTER TABLE public.relative_momentum ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS relative_momentum_deny_all ON public.relative_momentum;
CREATE POLICY relative_momentum_deny_all ON public.relative_momentum
    AS RESTRICTIVE USING (false) WITH CHECK (false);
REVOKE ALL ON public.relative_momentum FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.relative_momentum TO service_role;

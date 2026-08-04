-- The allocation POLICY: per risk profile, per asset class, the minimum / default / maximum share
-- that class is allowed to take. One row per (variant, bucket) — a 4x4 grid.
--
-- ⚠ THE KEYS ARE THE ONES THE REST OF THE APP ALREADY USES, NOT NEW SPELLINGS.
--   `variant` is one of `_airs_portfolio_variant.VARIANTS` (Offensief / Beperkt Offensief /
--   Neutraal / Defensief) — the same classifier the correlation matrix filters by, which reads the
--   profile out of AIRS's own portfolio name. `bucket` is a `_airs_holding_isin.BUCKET_ORDER` key
--   (Equity / Equity ETF / Bonds / Alternatives) — the STORED key, never the display label: the
--   reader sees "Stocks" but every join, colour and filter in the app keys off "Equity". Storing
--   the label here would make this table the one place that disagrees.
--
-- ⚠ EVERY PERCENT IS NULLABLE, AND NULL IS NOT ZERO. An unset band means "no policy recorded";
--   a 0 means "this class is not allowed" — for a minimum those are the same, but for a DEFAULT
--   and a MAXIMUM they are opposites, and seeding the grid with zeros would publish a policy
--   nobody wrote that reads as "hold none of this". The API returns null for an unset cell and the
--   editor shows it empty.
--
-- ⚠ NO CHECK THAT THE DEFAULTS SUM TO 100. They are not required to: these four are the INVESTED
--   classes, and cash (and anything unclassifiable) takes the rest. A constraint would forbid a
--   perfectly ordinary 5%-cash policy. The editor shows the sum so a reader can judge it.
CREATE TABLE IF NOT EXISTS public.airs_allocation_band (
    variant      text NOT NULL,
    bucket       text NOT NULL,
    min_pct      numeric,
    default_pct  numeric,
    max_pct      numeric,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (variant, bucket),
    -- Each bound in range on its own...
    CONSTRAINT airs_allocation_band_min_range
        CHECK (min_pct IS NULL OR (min_pct >= 0 AND min_pct <= 100)),
    CONSTRAINT airs_allocation_band_default_range
        CHECK (default_pct IS NULL OR (default_pct >= 0 AND default_pct <= 100)),
    CONSTRAINT airs_allocation_band_max_range
        CHECK (max_pct IS NULL OR (max_pct >= 0 AND max_pct <= 100)),
    -- ...and ordered against each other WHERE BOTH ARE SET. Written as three pairwise checks
    -- rather than one min <= default <= max, so a half-filled row (a max with no minimum yet) is
    -- still storable: the editor is a grid someone fills in over time, and refusing a partial row
    -- would make it unusable until every cell was complete.
    CONSTRAINT airs_allocation_band_min_le_default
        CHECK (min_pct IS NULL OR default_pct IS NULL OR min_pct <= default_pct),
    CONSTRAINT airs_allocation_band_default_le_max
        CHECK (default_pct IS NULL OR max_pct IS NULL OR default_pct <= max_pct),
    CONSTRAINT airs_allocation_band_min_le_max
        CHECK (min_pct IS NULL OR max_pct IS NULL OR min_pct <= max_pct)
);

COMMENT ON TABLE public.airs_allocation_band IS
    'Allocation policy per risk profile per asset class: the min / default / max share that class '
    'may take. `variant` is a _airs_portfolio_variant.VARIANTS label; `bucket` is a BUCKET_ORDER '
    'key (Equity, not "Stocks"). NULL means no policy recorded, which is not the same as 0.';

COMMENT ON COLUMN public.airs_allocation_band.default_pct IS
    'The target weight. NULL = unset. The four invested classes are NOT required to sum to 100 — '
    'cash takes the remainder.';

-- The backend reaches Postgres as service_role; nothing else touches this table directly.
-- (A missing GRANT surfaces as "permission denied 42501" even with BYPASSRLS — see the copy-local
-- -to-prod note in the repo memory.)
GRANT SELECT, INSERT, UPDATE, DELETE ON public.airs_allocation_band TO service_role;

NOTIFY pgrst, 'reload schema';

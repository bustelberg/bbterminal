-- Retire the `Equity ETF` asset-class bucket: fold it into `Equity`.
--
-- An equity ETF invests in equity. Every other bucket already named what a holding INVESTS IN — a
-- bond ETF has always been `Bonds`, never "ETF Bonds" — so the equity sleeve was the only one
-- split on the WRAPPER, and a book's equity exposure could not be read off the allocation bar
-- without adding two slices together.
--
-- Two tables carry the retired key. Both are small and both are hand-written policy, so each is
-- folded rather than dropped.

-- ── 1. Manual class overrides ────────────────────────────────────────────────────────────────
-- A pin of "this instrument is an equity ETF" is still true about the instrument; it just names a
-- bucket that no longer exists. Repointing it at `Equity` preserves the reader's decision.
--
-- ⚠ ON CONFLICT DO NOTHING IS NOT REACHABLE HERE and the constraint is on `isin` alone, so a plain
-- UPDATE is safe: an ISIN has at most one override row, and moving its bucket cannot collide.
UPDATE asset_bucket_override
   SET bucket = 'Equity',
       updated_at = now()
 WHERE bucket = 'Equity ETF';

-- ── 2. Allocation policy bands ───────────────────────────────────────────────────────────────
-- ⚠⚠ SUMMED, NOT DROPPED. A 0 / 10 / 25 ETF band sitting beside a 60 / 70 / 80 equity band is ONE
-- intent about equity exposure written across two rows; deleting the ETF row would silently cut
-- every profile's equity policy by its ETF allowance.
--
-- ⚠⚠ AND THE SUMMED MAXIMUM IS CAPPED AT 100, WHICH IS A REAL EDIT RATHER THAN ARITHMETIC.
-- Offensief is 80 + 25 = 105, and 105% is not a policy. The two maxima were never meant to be
-- reached together — that is what made them separate bands — so their sum overstates the combined
-- ceiling by construction. The cap lands the grid in a valid state and RAISES A NOTICE naming
-- every profile it touched, so the ceiling is reviewed by the person whose policy it is instead of
-- a number nobody chose looking deliberate. (There is no column to record it on — this table is
-- variant/bucket/min/default/max/updated_at and nothing else.)

--
-- Minima and defaults are summed without a cap: two floors compose (both must be met), and the
-- defaults are what the profile actually intends to hold, which was already under 100 together.

DO $$
DECLARE capped text;
BEGIN
  SELECT string_agg(eq.variant || ' (' || coalesce(eq.max_pct, 0)
                    || ' + ' || coalesce(etf.max_pct, 0) || ')', ', ' ORDER BY eq.variant)
    INTO capped
    FROM airs_allocation_band eq
    JOIN airs_allocation_band etf
      ON etf.variant = eq.variant AND etf.bucket = 'Equity ETF'
   WHERE eq.bucket = 'Equity'
     AND coalesce(eq.max_pct, 0) + coalesce(etf.max_pct, 0) > 100;
  IF capped IS NOT NULL THEN
    RAISE NOTICE 'REVIEW the Stocks maximum for: % - summed above 100%% and capped there.', capped;
  END IF;
END $$;
UPDATE airs_allocation_band AS eq
   SET min_pct     = coalesce(eq.min_pct, 0)     + coalesce(etf.min_pct, 0),
       default_pct = coalesce(eq.default_pct, 0) + coalesce(etf.default_pct, 0),
       max_pct     = least(100, coalesce(eq.max_pct, 0) + coalesce(etf.max_pct, 0)),
       updated_at  = now()
  FROM airs_allocation_band AS etf
 WHERE etf.variant = eq.variant
   AND etf.bucket  = 'Equity ETF'
   AND eq.bucket   = 'Equity';

-- Any ETF band for a profile with NO equity row is repointed rather than lost.
UPDATE airs_allocation_band
   SET bucket = 'Equity', updated_at = now()
 WHERE bucket = 'Equity ETF'
   AND variant NOT IN (SELECT variant FROM airs_allocation_band WHERE bucket = 'Equity');

DELETE FROM airs_allocation_band WHERE bucket = 'Equity ETF';

-- ── 3. Nothing may recreate it ───────────────────────────────────────────────────────────────
-- The application validates against `_airs_holding_isin.BUCKET_ORDER`, which no longer lists the
-- key; this is the database saying the same thing, so a stale deploy writing the old value fails
-- loudly instead of reintroducing a sixth bucket nothing renders.
ALTER TABLE asset_bucket_override
  DROP CONSTRAINT IF EXISTS asset_bucket_override_bucket_not_etf;
ALTER TABLE asset_bucket_override
  ADD CONSTRAINT asset_bucket_override_bucket_not_etf CHECK (bucket <> 'Equity ETF');

ALTER TABLE airs_allocation_band
  DROP CONSTRAINT IF EXISTS airs_allocation_band_bucket_not_etf;
ALTER TABLE airs_allocation_band
  ADD CONSTRAINT airs_allocation_band_bucket_not_etf CHECK (bucket <> 'Equity ETF');

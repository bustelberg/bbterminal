-- Enforce the frozen/monthly invariant at the DB level.
--
-- 2026-06-14 made universes "a set of companies as of a date": every
-- universe is a single frozen snapshot EXCEPT LongEquity (is_monthly = true),
-- which keeps per-month membership. Until now that invariant was convention
-- only — nothing stopped a frozen (is_monthly = false) universe from
-- accumulating multiple target_month values (the SP500 import still does, and
-- a frozen-basket backtest would then silently use only the latest month via
-- broadcast_constant). These two triggers make the invalid state impossible:
--
--   1. universe_membership writes  → a frozen universe may hold at most one
--      distinct target_month.
--   2. universe.is_monthly flips to false → the universe must already be a
--      single snapshot.
--
-- LongEquity (is_monthly = true) is exempt from both: it is allowed many months.

-- (1) Membership guard. Statement-level + transition table so a bulk snapshot
-- insert pays the check once per statement, not once per row.
CREATE OR REPLACE FUNCTION public.enforce_frozen_universe_single_month()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  bad record;
BEGIN
  FOR bad IN
    SELECT m.universe_id, count(DISTINCT m.target_month) AS months
      FROM public.universe_membership m
      JOIN public.universe u ON u.universe_id = m.universe_id
     WHERE u.is_monthly = false
       AND m.universe_id IN (SELECT DISTINCT universe_id FROM newrows)
     GROUP BY m.universe_id
    HAVING count(DISTINCT m.target_month) > 1
  LOOP
    RAISE EXCEPTION
      'frozen universe % has % distinct target_month values; a frozen '
      '(is_monthly=false) universe must hold exactly one snapshot month. '
      'Collapse membership to a single month or set is_monthly=true.',
      bad.universe_id, bad.months;
  END LOOP;
  RETURN NULL;
END;
$$;

-- One trigger per event: Postgres forbids a transition table on a trigger
-- that fires for more than one event, so INSERT and UPDATE get their own
-- (both bound to the same function).
DROP TRIGGER IF EXISTS universe_membership_frozen_single_month_ins ON public.universe_membership;
CREATE TRIGGER universe_membership_frozen_single_month_ins
  AFTER INSERT ON public.universe_membership
  REFERENCING NEW TABLE AS newrows
  FOR EACH STATEMENT
  EXECUTE FUNCTION public.enforce_frozen_universe_single_month();

DROP TRIGGER IF EXISTS universe_membership_frozen_single_month_upd ON public.universe_membership;
CREATE TRIGGER universe_membership_frozen_single_month_upd
  AFTER UPDATE ON public.universe_membership
  REFERENCING NEW TABLE AS newrows
  FOR EACH STATEMENT
  EXECUTE FUNCTION public.enforce_frozen_universe_single_month();

-- (2) Flag guard. Block flipping a multi-month universe to is_monthly=false.
CREATE OR REPLACE FUNCTION public.enforce_universe_monthly_flag()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  months int;
BEGIN
  SELECT count(DISTINCT target_month) INTO months
    FROM public.universe_membership
   WHERE universe_id = NEW.universe_id;
  IF months > 1 THEN
    RAISE EXCEPTION
      'cannot set universe % to is_monthly=false: it has % distinct months. '
      'Collapse membership to a single snapshot first.',
      NEW.universe_id, months;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS universe_monthly_flag_guard ON public.universe;
CREATE TRIGGER universe_monthly_flag_guard
  BEFORE UPDATE OF is_monthly ON public.universe
  FOR EACH ROW
  WHEN (NEW.is_monthly = false AND OLD.is_monthly IS DISTINCT FROM NEW.is_monthly)
  EXECUTE FUNCTION public.enforce_universe_monthly_flag();

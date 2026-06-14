-- Universes become single dated snapshots; LongEquity stays monthly.
--
-- 2026-06-14 decision: a universe is "a set of companies as of a date"
-- (universe.as_of_date). The per-month membership mechanism (target_month)
-- is RETAINED but used only by the LongEquity universe (is_monthly = true);
-- every other universe is collapsed to its single latest captured month.
-- The backtester selects only frozen (non-monthly) universes and no longer
-- time-travels their membership — ACWI / S&P 500 / Leonteq lose their
-- historical months here (intentional; see db-backups/ for the pre-collapse
-- snapshot).

ALTER TABLE public.universe
  ADD COLUMN IF NOT EXISTS as_of_date date,
  ADD COLUMN IF NOT EXISTS is_monthly boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN public.universe.as_of_date IS
  'Date the membership set represents (a frozen snapshot). For is_monthly universes, the latest captured month.';
COMMENT ON COLUMN public.universe.is_monthly IS
  'True only for the time-series LongEquity universe, which retains per-month membership. Every other universe is a single frozen set.';

-- LongEquity is the only time-series universe.
UPDATE public.universe
SET is_monthly = true
WHERE template_key = 'LONGEQUITY';

-- Collapse every non-monthly universe to ONLY its latest captured month.
DELETE FROM public.universe_membership um
USING public.universe u,
      (SELECT universe_id, max(target_month) AS latest_month
         FROM public.universe_membership
        GROUP BY universe_id) lm
WHERE um.universe_id = u.universe_id
  AND um.universe_id = lm.universe_id
  AND u.is_monthly = false
  AND um.target_month < lm.latest_month;

-- Stamp as_of_date from the latest remaining month per universe (left(...,7)
-- tolerates both 'YYYY-MM' and 'YYYY-MM-DD' target_month values).
UPDATE public.universe u
SET as_of_date = to_date(left(sub.latest_month, 7) || '-01', 'YYYY-MM-DD')
FROM (SELECT universe_id, max(target_month) AS latest_month
        FROM public.universe_membership
       GROUP BY universe_id) sub
WHERE u.universe_id = sub.universe_id;

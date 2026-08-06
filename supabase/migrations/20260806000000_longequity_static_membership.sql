-- LongEquity membership becomes STATIC: one row per company, not one per company per month.
--
-- WHY
--   LongEquity was the last time-varying universe — 3,219 rows over 9 months for 400 distinct
--   companies, while the other seven each hold exactly one month. Every one of those rows was a
--   materialised copy of something derivable: the source of truth is `metric_data` with
--   `source_code='longequity'`, which is what `longequity_membership_by_month()` reads and what
--   `/longequity-universe` already renders its per-month view from. Verified before this ran: the
--   stored union and the RPC's union agree exactly at 400 companies.
--
--   So the point-in-time detail is not lost here, it stays where it is authoritative. What is
--   removed is the duplication — and with it the reason `monthly_universe_labels()`, the
--   `?month=` query param and `FrozenUniversesPanel`'s carve-out all exist. Membership becomes
--   yes/no everywhere, which is what lets the asset-side membership table be a plain join with no
--   time dimension.
--
-- ⚠ THE ORDER IS FORCED BY THE DATABASE. `enforce_universe_monthly_flag` refuses `is_monthly =
--   false` while more than one distinct `target_month` exists — its own error says "Collapse
--   membership to a single snapshot first". So: delete the older months, THEN clear the flag.
--   Reversing these two statements fails loudly, which is the correct behaviour.
--
-- ⚠ THE SURVIVING ROWS ARE THE UNION, NOT THE NEWEST MONTH. Deleting everything but the latest
--   month would drop 400 - 498 companies that appeared earlier and not since; "ever in LongEquity"
--   is the definition being adopted. The newest month's label is kept as the stamp so the row
--   carries a meaningful "as of", matching how every other universe uses the column.
--
-- ⚠ THIS IS ONE OF THREE COORDINATED CHANGES. `ingest/longequity_universe.py` had to stop writing
--   per-month rows AND stop forcing `is_monthly = true` in the same commit — without that, the
--   next LongEquity ingest would rewrite all nine months and flip the flag back, silently undoing
--   everything below.

BEGIN;

-- 1. Collapse to the union, stamped with the newest report month.
--    `DISTINCT ON` keeps one row per company; the sector comes from its most recent month, which
--    is the same rule `_latest_sector_per_company` applies on the write path.
WITH le AS (
    SELECT universe_id FROM public.universe WHERE label = 'LongEquity' AND is_monthly
),
newest AS (
    SELECT max(m.target_month) AS stamp
      FROM public.universe_membership m
      JOIN le ON le.universe_id = m.universe_id
),
keep AS (
    SELECT DISTINCT ON (m.company_id)
           m.company_id, m.sector, m.universe_ticker, m.industry
      FROM public.universe_membership m
      JOIN le ON le.universe_id = m.universe_id
     ORDER BY m.company_id, m.target_month DESC
)
INSERT INTO public.universe_membership
       (universe_id, company_id, target_month, sector, universe_ticker, industry)
SELECT (SELECT universe_id FROM le), k.company_id, (SELECT stamp FROM newest),
       k.sector, k.universe_ticker, k.industry
  FROM keep k
    ON CONFLICT (universe_id, company_id, target_month) DO NOTHING;

-- 2. Drop every month that is not the stamp.
DELETE FROM public.universe_membership m
 USING public.universe u
 WHERE m.universe_id = u.universe_id
   AND u.label = 'LongEquity'
   AND u.is_monthly
   AND m.target_month <> (
       SELECT max(m2.target_month)
         FROM public.universe_membership m2
        WHERE m2.universe_id = m.universe_id
   );

-- 3. Only now may the flag be cleared — see the ⚠ above.
UPDATE public.universe
   SET is_monthly = false
 WHERE label = 'LongEquity';

COMMIT;

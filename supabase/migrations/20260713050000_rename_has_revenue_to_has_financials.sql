-- has_revenue -> has_financials.
--
-- The flag was never about revenue. It answers "does GuruFocus hold a financials blob
-- for this listing at all", which is what decides whether the cell offers a chart — and
-- ONE blob carries EVERY income-statement line, so a per-line flag would be both wrong
-- and unbounded (one column per column).
--
-- The distinction that actually matters is a different one, and it is not a boolean:
--   no blob            -> NO DATA   (a gap: a dead OTC line of an acquired company)
--   blob, no such line -> N/A       (an ANSWER: a bank has no gross profit. JPMorgan's
--                                    ind_template is 'B' — it reports Interest Income and
--                                    Net Interest Income, and has no cost of goods sold,
--                                    so no "Gross Profit" key exists in its statement)
-- The second is computed from the blob per request (`applicable=false`), not stored.
--
-- Safe rename: the column shipped hours ago in 20260713040000 and nothing reads it yet.

ALTER TABLE public.gurufocus_listing RENAME COLUMN has_revenue TO has_financials;
ALTER TABLE public.company           RENAME COLUMN has_revenue TO has_financials;

NOTIFY pgrst, 'reload schema';

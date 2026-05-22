-- Fix the actual finding the Supabase linter flagged on portfolio_weight.
--
-- Background: 20260522040000_index_unindexed_foreign_keys.sql tried to
-- address the "Unindexed foreign keys" linter finding on
-- public.portfolio_weight by adding an index on `portfolio_id`. But
-- portfolio_weight's PK is `(portfolio_id, company_id)`, so PostgreSQL
-- already serves single-column lookups on `portfolio_id` from the PK's
-- leading column. The actually-missing index is on the OTHER FK column:
-- `company_id`. Without it, "find every portfolio holding company X"
-- and the FK cascade on `company` deletion both seqscan portfolio_weight.
--
-- The redundant `portfolio_weight_portfolio_id_idx` from the prior
-- migration is left in place — it doesn't hurt anything (a few KB) and
-- dropping it risks breaking a deploy that's already applied 040000.
-- The linter will not complain about a duplicate covering index.

CREATE INDEX IF NOT EXISTS portfolio_weight_company_id_idx
    ON public.portfolio_weight (company_id);

NOTIFY pgrst, 'reload schema';

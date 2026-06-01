-- Per-strategy configurable start date.
--
-- Semantics: the "go-live" date for a scheduled strategy. Rendered as a
-- red dashed marker line on the source-backtest equity curve, and used as
-- the live cutoff in the run-history view (snapshots dated before it are
-- treated as backtest/pre-live, on-or-after it as live forward performance).
--
-- Nullable: a NULL start_date means "not explicitly set" — callers default
-- the marker to the strategy's created_at (when it was scheduled).
ALTER TABLE public.scheduled_strategy
    ADD COLUMN IF NOT EXISTS start_date date;

COMMENT ON COLUMN public.scheduled_strategy.start_date IS
    'Configurable go-live date: red dashed marker on the equity curve + live cutoff for run history. NULL falls back to created_at.';

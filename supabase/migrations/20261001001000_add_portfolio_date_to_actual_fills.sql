-- Upgrade databases that received the initial actual-fills table before fills
-- became portfolio-period specific.
ALTER TABLE public.scheduled_strategy_actual_fill
    ADD COLUMN IF NOT EXISTS portfolio_date date;

-- Legacy rows were previously one fill per strategy/company and therefore
-- represented the currently held portfolio. Attach them to that strategy's
-- newest snapshot so they remain visible after the period-specific upgrade.
UPDATE public.scheduled_strategy_actual_fill AS fill
SET portfolio_date = COALESCE(
    (
        SELECT snapshot.as_of_date
        FROM public.current_picks_snapshot AS snapshot
        WHERE snapshot.scheduled_strategy_id = fill.scheduled_strategy_id
        ORDER BY snapshot.created_at DESC
        LIMIT 1
    ),
    CURRENT_DATE
)
WHERE fill.portfolio_date IS NULL;

ALTER TABLE public.scheduled_strategy_actual_fill
    ALTER COLUMN portfolio_date SET NOT NULL;

ALTER TABLE public.scheduled_strategy_actual_fill
    DROP CONSTRAINT IF EXISTS scheduled_strategy_actual_fill_scheduled_strategy_id_company_id_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'scheduled_strategy_actual_fill_strategy_period_company_key'
    ) THEN
        ALTER TABLE public.scheduled_strategy_actual_fill
            ADD CONSTRAINT scheduled_strategy_actual_fill_strategy_period_company_key
            UNIQUE (scheduled_strategy_id, portfolio_date, company_id);
    END IF;
END $$;

NOTIFY pgrst, 'reload schema';

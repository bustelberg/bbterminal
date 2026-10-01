CREATE TABLE IF NOT EXISTS public.scheduled_strategy_actual_fill (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scheduled_strategy_id integer NOT NULL REFERENCES public.scheduled_strategy(id) ON DELETE CASCADE,
    portfolio_date date NOT NULL,
    -- Holdings use negative ids for ETF benchmark sleeves, so this deliberately
    -- cannot be a foreign key to `company`.
    company_id integer NOT NULL CHECK (company_id <> 0),
    entry_price numeric,
    entry_date date,
    exit_price numeric,
    exit_date date,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT scheduled_strategy_actual_fill_price_check CHECK (
        (entry_price IS NULL OR entry_price > 0) AND (exit_price IS NULL OR exit_price > 0)
    ),
    CONSTRAINT scheduled_strategy_actual_fill_dates_check CHECK (
        exit_date IS NULL OR entry_date IS NULL OR exit_date >= entry_date
    ),
    UNIQUE (scheduled_strategy_id, portfolio_date, company_id)
);

CREATE INDEX IF NOT EXISTS scheduled_strategy_actual_fill_strategy_idx
    ON public.scheduled_strategy_actual_fill (scheduled_strategy_id);

ALTER TABLE public.scheduled_strategy_actual_fill ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS scheduled_strategy_actual_fill_deny_all ON public.scheduled_strategy_actual_fill;
CREATE POLICY scheduled_strategy_actual_fill_deny_all ON public.scheduled_strategy_actual_fill
    FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.scheduled_strategy_actual_fill TO service_role;
NOTIFY pgrst, 'reload schema';

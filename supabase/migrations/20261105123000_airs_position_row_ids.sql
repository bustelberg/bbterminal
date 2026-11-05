ALTER TABLE public.airs_model_portfolio_position ADD COLUMN IF NOT EXISTS airs_row_id bigint;
NOTIFY pgrst, 'reload schema';

ALTER TABLE public.bc_log_entry DROP COLUMN IF EXISTS actions;

NOTIFY pgrst, 'reload schema';

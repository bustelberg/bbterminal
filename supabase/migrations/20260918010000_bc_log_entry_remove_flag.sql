ALTER TABLE public.bc_log_entry DROP COLUMN IF EXISTS flag;

NOTIFY pgrst, 'reload schema';

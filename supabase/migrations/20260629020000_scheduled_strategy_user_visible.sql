-- Per-strategy "visible to non-admin users" flag. The /schedule page is now
-- visible to regular users in READ-ONLY mode, but starts EMPTY: a user only
-- sees the scheduled strategies an admin has explicitly flagged here. Defaults
-- to false so nothing is exposed until the admin opts a strategy in.
ALTER TABLE public.scheduled_strategy
    ADD COLUMN IF NOT EXISTS user_visible boolean NOT NULL DEFAULT false;

NOTIFY pgrst, 'reload schema';

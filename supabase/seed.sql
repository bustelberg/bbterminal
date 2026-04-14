-- Seed file for local development
-- Add any seed data here that should be loaded after migrations

-- Create the gurufocus-raw storage bucket (used for caching API responses)
INSERT INTO storage.buckets (id, name, public)
VALUES ('gurufocus-raw', 'gurufocus-raw', false)
ON CONFLICT (id) DO NOTHING;

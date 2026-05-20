# DB ops scripts

Two PowerShell scripts for managing the gap between your local Supabase
(docker-Compose) and the remote prod Supabase project. Both go through the
local `supabase_db_bbterminal` container, so the only host-side dependencies
are PowerShell + Docker.

## Prereqs

- Local Supabase running (`npx supabase start`)
- `$env:PROD_DB_URL` set to your prod direct-connection URI:
  ```
  $env:PROD_DB_URL = 'postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres'
  ```
  Get this from Supabase Dashboard → Database settings → Connect → Direct
  connection. The password is the DB password (separate from your Supabase
  account login). Reset it under the same page if you forgot it.

## Scripts

### `copy-local-to-prod.ps1` — destructive replicate

Wipes prod's `public` schema and replaces it with a byte-for-byte copy of
local (schema + all data). Keeps `auth.users`, Storage buckets, API keys,
and the Supabase project itself intact.

```powershell
./scripts/copy-local-to-prod.ps1
# Or non-interactive:
./scripts/copy-local-to-prod.ps1 -Force
```

Use only while the project has no real users — it nukes every prod row.
After it runs, prod's `schema_migrations` matches local's, so
`supabase migration list` reports clean.

### `apply-migration.ps1` — additive migration

Applies a single `supabase/migrations/<timestamp>_<name>.sql` file to local
and prod, in that order, and records it in `schema_migrations` on both. Idempotent:
re-running on a version that's already recorded skips the apply.

```powershell
# After creating a migration file (e.g. via 'npx supabase migration new'):
./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/20260601120000_add_foo.sql

# Local-only (iterate before pushing to prod):
./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/20260601120000_add_foo.sql -LocalOnly
```

The script does NOT enforce that your SQL is non-destructive. If your
migration drops a table or column, it'll drop it on prod too. Write
additive migrations (`CREATE TABLE`, `ALTER TABLE ADD COLUMN`,
`CREATE FUNCTION`) when prod has real data you want to keep.

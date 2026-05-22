# DB ops scripts

Two PowerShell scripts for managing the gap between your local Supabase
(docker-Compose) and the remote prod Supabase project. Both go through the
local `supabase_db_bbterminal` container, so the only host-side dependencies
are PowerShell + Docker.

## Prereqs

- Local Supabase running (`npx supabase start`)
- `PROD_DB_URL` available to the scripts. Three ways, picked in this order
  of precedence — first one set wins:
  1. `-ProdDbUrl '<uri>'` passed on the command line
  2. `$env:PROD_DB_URL` exported in your shell
  3. `scripts/.env.local` (gitignored, auto-loaded) — copy
     `scripts/.env.local.example` to `scripts/.env.local` and edit the
     value once. Easiest for day-to-day use.

  Get the URI from Supabase Dashboard → Project Settings → Database →
  Connection string → **Session pooler** tab. Format:
  ```
  postgresql://postgres.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres
  ```
  **Don't use the "Direct connection" tab** — that hostname
  (`db.<ref>.supabase.co`) is IPv6-only and Docker Desktop on Windows
  doesn't route IPv6, so the scripts fail with "Name or service not
  known" from inside the container. **Don't use the "Transaction pooler"
  tab either** — port 6543 rejects DDL like `CREATE SCHEMA` mid-session.
  Session pooler (port 5432, IPv4) is the one that works for everything
  these scripts do.

  The password is the DB password (separate from your Supabase account
  login). Reset it under the same page if you forgot it or if it's been
  exposed.

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

The script also re-grants Supabase's stock privileges (`anon`,
`authenticated`, `service_role`) on the freshly restored tables. Without
this step the backend's `service_role` key gets `permission denied`
errors on every query, because `pg_restore --no-privileges` strips ACLs
and Supabase's auto-grant event trigger doesn't fire on bulk restores.
Mirror of `supabase/migrations/20260522000000_restore_supabase_default_grants.sql`.

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

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

### `clone-local-to-prod.ps1` — differential sync

Makes prod's `public` schema an exact clone of local by transferring only
what differs — no schema drop, no full data reload. Applies any missing
migrations, upserts + delete-mirrors every table, syncs `metric_data`
per-company by signature (only changed companies cross the wire), and mirrors
the `backtest-results` Storage bucket. Non-destructive to `auth.users` / API
keys. Idempotent — a second run right after transfers nothing.

```powershell
# Preview what would change (read-only):
./scripts/clone-local-to-prod.ps1 -DryRun
# Apply (interactive confirm):
./scripts/clone-local-to-prod.ps1
# Non-interactive:
./scripts/clone-local-to-prod.ps1 -Force
```

Storage sync needs a prod service key — set `PROD_SERVICE_KEY` (and
`PROD_SUPABASE_URL` if it can't be auto-derived) in `scripts/.env.local`; the
DB clone still runs without it (the Storage step warns + skips).

If prod is ever AHEAD of local (extra migrations/columns local lacks), the
clone stops. Reconcile prod by hand (drop the prod-only objects) or restore it
from a Supabase backup, then re-run.

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

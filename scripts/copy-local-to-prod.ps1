<#
.SYNOPSIS
  Destructively replicate the local Supabase 'public' schema (schema + data) to prod.

.DESCRIPTION
  - Wipes prod's 'public' schema (keeps auth.users, Storage buckets, API keys, etc).
  - Dumps local 'public' (schema + data) via pg_dump custom-format.
  - Restores into prod via pg_restore.
  - Aligns prod's supabase_migrations.schema_migrations to match local.

  Intended for early-dev "reset prod from my laptop" workflow. NOT for use after
  the project has real users — every prod row gets nuked.

.PARAMETER ProdDbUrl
  Session-pooler Postgres connection string to prod. Defaults to
  $env:PROD_DB_URL (which is auto-loaded from scripts/.env.local if set there).
  Use the *Session pooler* URI from Supabase Dashboard — the direct-connection
  hostname is IPv6-only and Docker Desktop on Windows can't reach it.
  Format: postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres
  The password is extracted automatically and passed to psql/pg_restore via
  PGPASSWORD so it never appears in `ps`.

.PARAMETER Force
  Skip the interactive confirmation prompt.

.EXAMPLE
  ./scripts/copy-local-to-prod.ps1

.EXAMPLE
  ./scripts/copy-local-to-prod.ps1 -Force
#>
[CmdletBinding()]
param(
    [string]$ProdDbUrl,
    [string]$Container = 'supabase_db_bbterminal',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# Load scripts/.env.local (gitignored) into the process env. Lets you stash
# PROD_DB_URL there once instead of exporting it every shell session. The
# -ProdDbUrl param wins; then $env:PROD_DB_URL (already set in your shell);
# then whatever .env.local provides — same precedence direnv et al. use.
$envFile = Join-Path $PSScriptRoot '.env.local'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
        if ($_ -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.+?)\s*$') {
            $k = $Matches[1]; $v = $Matches[2] -replace '^"(.*)"$','$1' -replace "^'(.*)'$",'$1'
            if (-not (Test-Path "env:$k")) { Set-Item "env:$k" $v }
        }
    }
}
if (-not $ProdDbUrl) { $ProdDbUrl = $env:PROD_DB_URL }

if (-not $ProdDbUrl) {
    Write-Host "ERROR: PROD_DB_URL not set." -ForegroundColor Red
    Write-Host "Either pass -ProdDbUrl, set `$env:PROD_DB_URL, or put it in scripts/.env.local"
    Write-Host "(see scripts/.env.local.example)."
    exit 1
}

# Split the password out of the URL so we can pass it via PGPASSWORD env var
# instead of embedding it in the connection-string argv. With the password in
# argv, anyone with shell access to the container (or anyone reading the
# transcript of a `ps -ef`) can see it. With PGPASSWORD set via docker -e, it
# only lives in the docker exec target process's env.
if ($ProdDbUrl -notmatch '^(?<prefix>postgres(?:ql)?://[^:@/]+):(?<pw>[^@]+)@(?<rest>.+)$') {
    Write-Host "ERROR: PROD_DB_URL doesn't match 'postgres://user:password@host...'." -ForegroundColor Red
    Write-Host "See scripts/.env.local.example for the expected Supabase Session-pooler format."
    exit 1
}
$prodPassword  = $Matches.pw
$prodUrlNoPw   = "$($Matches.prefix)@$($Matches.rest)"
$prodEnv       = @('-e', "PGPASSWORD=$prodPassword")

# Sanity check: container running?
$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: docker container '$Container' is not running. Start local Supabase first ('npx supabase start')." -ForegroundColor Red
    exit 1
}

# Sanity check: prod reachable?
Write-Host "[1/6] Verifying prod connection..."
$probe = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -c "SELECT current_database(), version();" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: could not connect to prod: $probe" -ForegroundColor Red
    exit 1
}
Write-Host "  OK: $probe" -ForegroundColor Green

# Confirmation
if (-not $Force) {
    Write-Host ""
    Write-Host "About to:" -ForegroundColor Yellow
    Write-Host "  1. Dump local 'public' schema (schema + data)"
    Write-Host "  2. DROP SCHEMA public CASCADE on prod (auth/storage/keys preserved)"
    Write-Host "  3. Restore the dump into prod"
    Write-Host "  4. Restore Supabase default GRANTs on public.* (anon/authenticated/service_role)"
    Write-Host "  5. Align prod's schema_migrations to match local"
    Write-Host ""
    $resp = Read-Host "Type 'YES' to proceed"
    if ($resp -ne 'YES') {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
}

# 1. Dump local
Write-Host "[2/6] Dumping local public schema (binary, compressed)..."
docker exec $Container pg_dump -U postgres -d postgres --schema=public --no-owner --no-privileges --format=custom -f /tmp/copy_to_prod_dump.pgdump
if ($LASTEXITCODE -ne 0) { Write-Host "pg_dump failed" -ForegroundColor Red; exit 1 }
$dumpSize = docker exec $Container stat -c %s /tmp/copy_to_prod_dump.pgdump
Write-Host "  OK: $([math]::Round([int64]$dumpSize/1MB,2)) MB" -ForegroundColor Green

# 2. Wipe prod public. DROP only — don't pre-create the schema, because the
#    dump (made with pg_dump --schema=public) includes its own
#    `CREATE SCHEMA public;` and pg_restore would error on conflict otherwise.
#    Schema-level GRANTs come back in step 5 alongside the table grants.
Write-Host "[3/6] Dropping prod public schema..."
docker exec @prodEnv $Container psql $prodUrlNoPw -c "DROP SCHEMA IF EXISTS public CASCADE;"
if ($LASTEXITCODE -ne 0) { Write-Host "drop schema failed" -ForegroundColor Red; exit 1 }
Write-Host "  OK" -ForegroundColor Green

# 3. Restore to prod. --verbose makes pg_restore print one line per object
# (TABLE / INDEX / SEQUENCE / TABLE DATA …) so you can see continuous progress
# instead of a silent multi-minute wait while metric_data COPYs.
Write-Host "[4/6] Restoring dump to prod (verbose; tables + data + indexes)..."
docker exec @prodEnv $Container pg_restore --no-owner --no-privileges --verbose --dbname=$prodUrlNoPw /tmp/copy_to_prod_dump.pgdump
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: pg_restore reported non-zero exit. Inspect output above; pg_restore can warn-and-continue on harmless issues." -ForegroundColor Yellow
}
Write-Host "  OK" -ForegroundColor Green

# 4. Restore Supabase default GRANTs.
#    pg_restore was run with --no-privileges (so we don't fight local's ACLs)
#    AND Supabase's auto-grant event trigger doesn't fire for bulk-restored
#    tables. Without this step, every table ends up granted to 'postgres'
#    only, and the FastAPI backend (service_role) gets 'permission denied
#    for table X' (42501) on every query — even though BYPASSRLS is true.
#    Mirror of supabase/migrations/20260522000000_restore_supabase_default_grants.sql
#    so the script is self-healing whether or not that migration is in sync.
Write-Host "[5/6] Restoring Supabase default GRANTs on public.* ..."
$grantSql = @"
GRANT USAGE  ON SCHEMA public TO anon, authenticated, service_role;
GRANT CREATE ON SCHEMA public TO postgres, service_role;
GRANT ALL ON ALL TABLES    IN SCHEMA public TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO anon, authenticated, service_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES    TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO anon, authenticated, service_role;
NOTIFY pgrst, 'reload schema';
"@
docker exec @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -c $grantSql
if ($LASTEXITCODE -ne 0) { Write-Host "grant step failed" -ForegroundColor Red; exit 1 }
Write-Host "  OK" -ForegroundColor Green

# 5. Align migration tracker
Write-Host "[6/6] Aligning prod's schema_migrations to local..."
# Copy local schema_migrations row(s) into prod verbatim.
$rows = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c "SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version;"
docker exec @prodEnv $Container psql $prodUrlNoPw -c "TRUNCATE supabase_migrations.schema_migrations;"
foreach ($line in $rows -split "`n") {
    $line = $line.Trim()
    if (-not $line) { continue }
    $parts = $line -split '\|', 2
    $ver = $parts[0]
    $nm  = if ($parts.Length -gt 1) { $parts[1] } else { '' }
    docker exec @prodEnv $Container psql $prodUrlNoPw -c "INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES ('$ver', '$nm', ARRAY['-- copied from local on $(Get-Date -Format yyyy-MM-dd)']);"
}
Write-Host "  OK" -ForegroundColor Green

# Cleanup tmp dump
docker exec $Container rm /tmp/copy_to_prod_dump.pgdump | Out-Null

Write-Host ""
Write-Host "DONE. Prod 'public' schema is now a byte-for-byte copy of local." -ForegroundColor Green
Write-Host "Quick verify (run from another shell):"
Write-Host "  docker exec -e PGPASSWORD=`$pw $Container psql '$prodUrlNoPw' -c '\dt public.*'"

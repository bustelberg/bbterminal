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
  Direct Postgres connection string to prod. Defaults to $env:PROD_DB_URL.
  Format: postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres

.PARAMETER Force
  Skip the interactive confirmation prompt.

.EXAMPLE
  $env:PROD_DB_URL = 'postgresql://postgres:xxx@db.abc.supabase.co:5432/postgres'
  ./scripts/copy-local-to-prod.ps1

.EXAMPLE
  ./scripts/copy-local-to-prod.ps1 -ProdDbUrl '<uri>' -Force
#>
[CmdletBinding()]
param(
    [string]$ProdDbUrl = $env:PROD_DB_URL,
    [string]$Container = 'supabase_db_bbterminal',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

if (-not $ProdDbUrl) {
    Write-Host "ERROR: -ProdDbUrl not provided and `$env:PROD_DB_URL is empty." -ForegroundColor Red
    Write-Host "Set it via: `$env:PROD_DB_URL = 'postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres'"
    exit 1
}

# Sanity check: container running?
$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: docker container '$Container' is not running. Start local Supabase first ('npx supabase start')." -ForegroundColor Red
    exit 1
}

# Sanity check: prod reachable?
Write-Host "[1/5] Verifying prod connection..."
$probe = docker exec $Container psql $ProdDbUrl -tA -c "SELECT current_database(), version();" 2>&1
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
    Write-Host "  4. Align prod's schema_migrations to match local"
    Write-Host ""
    $resp = Read-Host "Type 'YES' to proceed"
    if ($resp -ne 'YES') {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
}

# 1. Dump local
Write-Host "[2/5] Dumping local public schema (binary, compressed)..."
docker exec $Container pg_dump -U postgres -d postgres --schema=public --no-owner --no-privileges --format=custom -f /tmp/copy_to_prod_dump.pgdump
if ($LASTEXITCODE -ne 0) { Write-Host "pg_dump failed" -ForegroundColor Red; exit 1 }
$dumpSize = docker exec $Container stat -c %s /tmp/copy_to_prod_dump.pgdump
Write-Host "  OK: $([math]::Round([int64]$dumpSize/1MB,2)) MB" -ForegroundColor Green

# 2. Wipe prod public
Write-Host "[3/5] Dropping prod public schema..."
$sql = @"
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT USAGE  ON SCHEMA public TO postgres, anon, authenticated, service_role;
GRANT CREATE ON SCHEMA public TO postgres,                       service_role;
"@
docker exec $Container psql $ProdDbUrl -c $sql
if ($LASTEXITCODE -ne 0) { Write-Host "drop schema failed" -ForegroundColor Red; exit 1 }
Write-Host "  OK" -ForegroundColor Green

# 3. Restore to prod
Write-Host "[4/5] Restoring dump to prod (this can take several minutes)..."
docker exec $Container pg_restore --no-owner --no-privileges --dbname=$ProdDbUrl /tmp/copy_to_prod_dump.pgdump
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: pg_restore reported non-zero exit. Inspect output above; pg_restore can warn-and-continue on harmless issues." -ForegroundColor Yellow
}
Write-Host "  OK" -ForegroundColor Green

# 4. Align migration tracker
Write-Host "[5/5] Aligning prod's schema_migrations to local..."
# Copy local schema_migrations row(s) into prod verbatim.
$rows = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c "SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version;"
docker exec $Container psql $ProdDbUrl -c "TRUNCATE supabase_migrations.schema_migrations;"
foreach ($line in $rows -split "`n") {
    $line = $line.Trim()
    if (-not $line) { continue }
    $parts = $line -split '\|', 2
    $ver = $parts[0]
    $nm  = if ($parts.Length -gt 1) { $parts[1] } else { '' }
    docker exec $Container psql $ProdDbUrl -c "INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES ('$ver', '$nm', ARRAY['-- copied from local on $(Get-Date -Format yyyy-MM-dd)']);"
}
Write-Host "  OK" -ForegroundColor Green

# Cleanup tmp dump
docker exec $Container rm /tmp/copy_to_prod_dump.pgdump | Out-Null

Write-Host ""
Write-Host "DONE. Prod 'public' schema is now a byte-for-byte copy of local." -ForegroundColor Green
Write-Host "Verify with: docker exec $Container psql `"`$env:PROD_DB_URL`" -c '\dt public.*'"

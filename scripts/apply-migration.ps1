<#
.SYNOPSIS
  Apply a Supabase migration SQL file to local + prod (non-destructive).

.DESCRIPTION
  - Validates the migration file is named '<14-digit-timestamp>_<name>.sql'.
  - Applies it to the local Supabase DB via psql.
  - Applies it to the prod Supabase DB via psql.
  - Records the migration in supabase_migrations.schema_migrations on BOTH.
  - Skips application if the version is already recorded on that side (idempotent).

  Use this for additive migrations (CREATE TABLE / ALTER TABLE / CREATE FUNCTION).
  Migrations are NOT validated to be non-destructive — that's your responsibility.
  The script just routes the SQL through; if your migration drops a table, that's
  on you.

.PARAMETER MigrationFile
  Path to the .sql file under supabase/migrations/. Required.

.PARAMETER ProdDbUrl
  Direct Postgres connection string to prod. Defaults to $env:PROD_DB_URL.

.PARAMETER LocalOnly
  Apply to local only (skip prod). Useful for iterating before pushing.

.EXAMPLE
  # Created via 'npx supabase migration new add_foo_table', edit the file, then:
  ./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/20260601120000_add_foo_table.sql

.EXAMPLE
  # Local-only dry run:
  ./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/... -LocalOnly
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$MigrationFile,
    [string]$ProdDbUrl = $env:PROD_DB_URL,
    [string]$Container = 'supabase_db_bbterminal',
    [switch]$LocalOnly
)

$ErrorActionPreference = 'Stop'

# Resolve + validate file
$full = Resolve-Path $MigrationFile -ErrorAction SilentlyContinue
if (-not $full) {
    Write-Host "ERROR: migration file not found: $MigrationFile" -ForegroundColor Red
    exit 1
}
$fname = (Get-Item $full).Name
if ($fname -notmatch '^(\d{14})_(.+)\.sql$') {
    Write-Host "ERROR: filename '$fname' must match '<14-digit-timestamp>_<name>.sql'" -ForegroundColor Red
    exit 1
}
$version = $matches[1]
$name = $matches[2]

# Check container
$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: docker container '$Container' is not running." -ForegroundColor Red
    exit 1
}

# Check prod creds if not local-only
if (-not $LocalOnly -and -not $ProdDbUrl) {
    Write-Host "ERROR: -ProdDbUrl / `$env:PROD_DB_URL not set. Use -LocalOnly to skip prod." -ForegroundColor Red
    exit 1
}

# Copy migration into container (handles Windows paths cleanly)
$containerPath = "/tmp/migration_$version.sql"
docker cp "$full" "${Container}:$containerPath" | Out-Null

function Apply-To {
    param([string]$Label, [string]$ConnArg)

    # Already recorded?
    $check = docker exec $Container psql $ConnArg -tA -c "SELECT 1 FROM supabase_migrations.schema_migrations WHERE version = '$version';" 2>&1
    if ($check.Trim() -eq '1') {
        Write-Host "  [$Label] already recorded as applied (version=$version) — skipping" -ForegroundColor Yellow
        return
    }

    # Apply SQL
    Write-Host "  [$Label] applying SQL..."
    docker exec $Container psql $ConnArg -v ON_ERROR_STOP=1 -f $containerPath
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [$Label] FAILED — see error above" -ForegroundColor Red
        throw "Apply failed on $Label"
    }

    # Record in schema_migrations
    docker exec $Container psql $ConnArg -c "INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES ('$version', '$name', ARRAY['-- applied via apply-migration.ps1']);" | Out-Null
    Write-Host "  [$Label] OK" -ForegroundColor Green
}

Write-Host "Applying $fname (version=$version):"
Apply-To 'local' '-U postgres -d postgres'
if (-not $LocalOnly) {
    Apply-To 'prod' $ProdDbUrl
}

# Cleanup
docker exec $Container rm $containerPath | Out-Null

Write-Host ""
Write-Host "DONE." -ForegroundColor Green

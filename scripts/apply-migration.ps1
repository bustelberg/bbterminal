<#
.SYNOPSIS
  Apply a Supabase migration SQL file to local + prod (non-destructive).

.DESCRIPTION
  - Validates the migration file is named '<14-digit-timestamp>_<name>.sql'.
  - Applies it to the local Supabase DB via psql.
  - Applies it to the prod Supabase DB via psql.
  - Records the migration in supabase_migrations.schema_migrations on BOTH.
  - Skips application if the version is already recorded on that side
    (idempotent).

  Use this for additive migrations (CREATE TABLE / ALTER TABLE /
  CREATE FUNCTION). Migrations are NOT validated to be non-destructive --
  that is your responsibility.

  NOTE: this file is ASCII-only on purpose. PowerShell 5.1 reads .ps1 files
  without a BOM as cp1252, which mojibakes UTF-8 multi-byte characters
  (em-dash, smart quotes, etc.) and silently breaks string boundaries
  mid-parse. Do not add non-ASCII characters to this file.

.PARAMETER MigrationFile
  Path to the .sql file under supabase/migrations/. Required.

.PARAMETER ProdDbUrl
  Session-pooler connection string to prod. Defaults to $env:PROD_DB_URL,
  which is auto-loaded from scripts/.env.local if set there.

.PARAMETER LocalOnly
  Apply to local only (skip prod). Useful for iterating before pushing.

.EXAMPLE
  ./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/20260601120000_add_foo_table.sql

.EXAMPLE
  ./scripts/apply-migration.ps1 -MigrationFile supabase/migrations/... -LocalOnly
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$MigrationFile,
    [string]$ProdDbUrl,
    [string]$Container = 'supabase_db_bbterminal',
    [switch]$LocalOnly
)

$ErrorActionPreference = 'Stop'

# Load scripts/.env.local (gitignored) -- same precedence as copy-local-to-prod.ps1.
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

# Resolve + validate file. Cast PathInfo to string so Get-Content -Path doesn't
# get a wrapper type on some PS versions.
$resolved = Resolve-Path $MigrationFile -ErrorAction SilentlyContinue
if (-not $resolved) {
    Write-Host "ERROR: migration file not found: $MigrationFile" -ForegroundColor Red
    exit 1
}
$full = $resolved.Path
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
    Write-Host "ERROR: PROD_DB_URL not set. Pass -ProdDbUrl, export `$env:PROD_DB_URL, or put it in scripts/.env.local. Or use -LocalOnly." -ForegroundColor Red
    exit 1
}

function Apply-To {
    # ConnArgs is a string[] so multi-token specs like @('-U','postgres','-d','postgres')
    # reach psql as separate argv entries. Passing one space-separated string
    # makes PowerShell quote the whole thing and psql treats it as one -U value.
    param([string]$Label, [string[]]$ConnArgs)

    # Already recorded? Coerce $check via "$check" so a $null (no rows) does
    # not blow up the .Trim() call -- psql -tA emits nothing when no row matches.
    $check = docker exec $Container psql @ConnArgs -tA -c "SELECT 1 FROM supabase_migrations.schema_migrations WHERE version = '$version';" 2>&1
    if ("$check".Trim() -eq '1') {
        Write-Host "  [$Label] already recorded as applied (version=$version) -- skipping" -ForegroundColor Yellow
        return
    }

    # Apply SQL via stdin. Earlier this script used docker-cp + psql -f /tmp/X.sql,
    # which silently no-opped (exit 0, no SQL ran) -- almost certainly because the
    # container path got translated somewhere between PowerShell and docker.
    # Piping over stdin avoids any file paths entirely.
    Write-Host "  [$Label] applying SQL..."
    Get-Content -Path $full -Raw | docker exec -i $Container psql @ConnArgs -v ON_ERROR_STOP=1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [$Label] FAILED -- see error above" -ForegroundColor Red
        throw "Apply failed on $Label"
    }

    # Record in schema_migrations. ON CONFLICT DO NOTHING handles the case
    # where an earlier broken run inserted the row but the SQL never landed:
    # re-running now applies the SQL (above) and leaves the existing row in place.
    docker exec $Container psql @ConnArgs -c "INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES ('$version', '$name', ARRAY['-- applied via apply-migration.ps1']) ON CONFLICT (version) DO NOTHING;" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "[$Label] FAILED to record in schema_migrations"
    }
    Write-Host "  [$Label] OK" -ForegroundColor Green
}

Write-Host "Applying $fname (version=$version):"
Apply-To 'local' @('-U','postgres','-d','postgres')
if (-not $LocalOnly) {
    Apply-To 'prod' @($ProdDbUrl)
}

Write-Host ""
Write-Host "DONE." -ForegroundColor Green

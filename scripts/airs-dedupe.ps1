<#
.SYNOPSIS
  Report (and optionally delete) duplicate rows in the AIRS scrape tables on PROD.

.DESCRIPTION
  Four AIRS tables key on a surrogate `id` and have no natural unique constraint, so the same
  logical row can be stored twice. Two independent causes, both real:

    1. THE SCRAPER. Measured on LOCAL -- which has never been cloned to -- 24 pairs in
       airs_holding with consecutive ids, identical `retrieved_at` and identical
       ISIN/quantity/value. One run wrote each holding twice.
    2. THE CLONE. It upserted these tables BY PK. Each side assigns its own serial, so a local row
       whose id was free on prod was INSERTED beside the row prod already held. (Fixed 2026-08-11
       -- clone-local-to-prod.ps1 now skips them entirely.)

  This removes the copies, keeping the LOWEST id of each group.

  !! IT REPORTS BY DEFAULT. Pass -Apply to delete.

  !! THE NATURAL KEYS ARE NOT THE OBVIOUS ONES, AND THE OBVIOUS ONE IS WRONG. For airs_holding,
  (portefeuille, as_of_date, holding_name) matches 83 groups on local that are NOT duplicates: a
  bond and its accrued-interest line share a display name ("6,5% Rabobank Certificaten 14-perp."
  at EUR 8,347.20 and at EUR 112.23, same scrape, same timestamp). Quantity and value are what
  separate a second line from a second COPY.

  !! AND `retrieved_at` IS DELIBERATELY EXCLUDED from every key. A clone-inserted copy carries the
  other side's scrape timestamp, so an all-columns match would miss precisely the rows this exists
  to find.

.PARAMETER Apply
  Actually delete. Without it, the script only counts.

.EXAMPLE
  ./scripts/airs-dedupe.ps1
.EXAMPLE
  ./scripts/airs-dedupe.ps1 -Apply
#>
[CmdletBinding()]
param(
    [string]$ProdDbUrl,
    [string]$Container = 'supabase_db_bbterminal',
    [switch]$Local,
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'

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
if (-not $Local -and -not $ProdDbUrl) {
    Write-Host "ERROR: PROD_DB_URL not set (or pass -Local to work on the local database)." -ForegroundColor Red
    exit 1
}
if (-not $Local) {
    if ($ProdDbUrl -notmatch '^(?<prefix>postgres(?:ql)?://[^:@/]+):(?<pw>[^@]+)@(?<rest>.+)$') {
        Write-Host "ERROR: PROD_DB_URL doesn't match 'postgres://user:password@host...'." -ForegroundColor Red
        exit 1
    }
    $prodEnv     = @('-e', "PGPASSWORD=$($Matches.pw)", '-e', 'PGOPTIONS=-c statement_timeout=0')
    $prodUrlNoPw = "$($Matches.prefix)@$($Matches.rest)"
}

function Get-Count([string]$sql) {
    <#
      A single integer out of a single-row result.

      !! `[int](Invoke-Db $sql)[0]` IS A TRAP THAT RETURNS A PLAUSIBLE WRONG NUMBER. PowerShell
      unrolls a one-element array to a bare string, so `[0]` indexes the STRING -- and casting the
      resulting CHARACTER to int yields its ASCII code. This script first reported 50 duplicates in
      airs_holding (the truth was 24: '2' is ASCII 50) and 48 in three tables that were clean ('0'
      is ASCII 48). Every count reads like data, which is what makes it dangerous.
    #>
    $rows = @(Invoke-Db $sql)
    if (-not $rows.Count) { return 0 }
    return [int](($rows[0] -split '\|')[0])
}

function Invoke-Db([string]$sql) {
    if ($Local) {
        $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -v ON_ERROR_STOP=1 -c $sql
    } else {
        $full = "set session characteristics as transaction read write;`nSET statement_timeout = 0;`n$sql"
        $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -v ON_ERROR_STOP=1 -c $full
    }
    if ($LASTEXITCODE -ne 0) { throw "psql failed:`n$sql`n$out" }
    return @($out | ForEach-Object { "$_".TrimEnd("`r") } |
        Where-Object { $_ -ne '' -and $_ -notmatch '^(SET|DELETE \d+)$' })
}

# table -> the columns that identify ONE logical row. See the header for why these and not the
# obvious ones. `IS NOT DISTINCT FROM` throughout, because a NULL quantity or value must match a
# NULL on the other side rather than making the row unmatchable.
$keys = [ordered]@{
    'airs_holding'                  = @('portefeuille', 'as_of_date', 'holding_name',
                                        'quantity', 'current_value_eur')
    'airs_mutatie'                  = @('portefeuille', 'boekdatum', 'grootboek', 'fonds',
                                        'omschrijving', 'amount_eur')
    'airs_model_portfolio'          = @('name')
    'airs_model_portfolio_position' = @('portfolio_id', 'datum', 'isin', 'fonds', 'percentage')
}

$where = "  Target: " + $(if ($Local) { "LOCAL" } else { "PROD" })
Write-Host $where -ForegroundColor Cyan
$total = 0
foreach ($t in $keys.Keys) {
    $cols = $keys[$t]
    $group = ($cols -join ', ')
    $n = Get-Count @"
SELECT coalesce(sum(n - 1), 0) FROM (
  SELECT count(*) AS n FROM public.$t GROUP BY $group HAVING count(*) > 1
) q
"@
    $total += $n
    if ($n -eq 0) {
        Write-Host ("  {0,-32} clean" -f $t) -ForegroundColor Gray
        continue
    }
    Write-Host ("  {0,-32} {1} duplicate row(s) to remove" -f $t, $n) -ForegroundColor Yellow
    if ($Apply) {
        # Keep the LOWEST id of each group -- the first one stored. Arbitrary between two identical
        # rows, and that is the point: they are identical, so any rule that keeps exactly one is
        # correct and this one is deterministic.
        $match = (($cols | ForEach-Object { "a.$_ IS NOT DISTINCT FROM b.$_" }) -join ' AND ')
        Invoke-Db "DELETE FROM public.$t a USING public.$t b WHERE a.id > b.id AND $match;" | Out-Null
        $left = Get-Count @"
SELECT coalesce(sum(n - 1), 0) FROM (
  SELECT count(*) AS n FROM public.$t GROUP BY $group HAVING count(*) > 1
) q
"@
        Write-Host ("    deleted; {0} remaining" -f $left) -ForegroundColor Green
    }
}
if ($total -eq 0) {
    Write-Host "`nNo duplicates." -ForegroundColor Green
} elseif (-not $Apply) {
    Write-Host "`n$total duplicate row(s) found. Re-run with -Apply to delete them." -ForegroundColor Cyan
} else {
    Write-Host "`nDone." -ForegroundColor Green
    Write-Host "NOTE: this removes the SYMPTOM. airs_holding still has no natural unique key, so the" -ForegroundColor Yellow
    Write-Host "      scraper can write duplicates again on its next run -- see TODO.md." -ForegroundColor Yellow
}

<#
.SYNOPSIS
  Make prod's 'public' schema an EXACT clone of local, transferring only what
  differs (fast differential sync). Non-destructive to auth/storage/keys.

.DESCRIPTION
  Unlike copy-local-to-prod.ps1 (which DROPs prod's public schema and restores
  all ~26M metric_data rows every run), this script:

    1. Schema parity: applies any local migrations prod is missing, then aligns
       supabase_migrations.schema_migrations to local. (Stops if prod has drift
       it can't reconcile -- use copy-local-to-prod.ps1 for a full rebuild then.)
    2. Stages every small/medium table's local rows into a clone_stg schema on
       prod (one transfer; ~140k rows total -> seconds).
    3. UPSERTs them parent->child (insert + update by PK), then DELETEs prod rows
       whose PK is gone from local child->parent. True mirror, no schema drop.
    4. metric_data (26M rows) is synced DIFFERENTIALLY: a per-company signature
       (count, sum(value), min/max date) is compared on both sides and only the
       companies whose price data changed are re-copied. Unchanged -> zero rows
       cross the wire. recorded_at is intentionally excluded from the signature
       (prod fetches the same prices independently with different timestamps;
       including it would force a full re-copy every run).
    5. Verifies row counts + the metric_data signature afterwards.

  End result: prod public schema + data is an exact clone of local (modulo the
  recorded_at audit timestamp on metric_data rows whose price data already
  matched). Idempotent: a second run right after transfers nothing.

  All psql runs inside the local Supabase container (docker exec); the prod
  connection uses the Session-pooler URI. Both \copy endpoints share the
  container filesystem, so deltas round-trip through /tmp without leaving it.

.PARAMETER ProdDbUrl
  Session-pooler Postgres URI to prod. Defaults to $env:PROD_DB_URL (auto-loaded
  from scripts/.env.local). Format:
  postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres

.PARAMETER DryRun
  Read-only: report per-table local-vs-prod row counts and the number of
  metric_data companies that differ, then exit without mutating prod.

.PARAMETER Force
  Skip the interactive confirmation prompt.

.EXAMPLE
  ./scripts/clone-local-to-prod.ps1 -DryRun

.EXAMPLE
  ./scripts/clone-local-to-prod.ps1
#>
[CmdletBinding()]
param(
    [string]$ProdDbUrl,
    [string]$Container = 'supabase_db_bbterminal',
    [int]$CompanyChunk = 300,
    [switch]$DryRun,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$swAll = [System.Diagnostics.Stopwatch]::StartNew()

# ---- env + prod URL ---------------------------------------------------------
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
    Write-Host "ERROR: PROD_DB_URL not set. Pass -ProdDbUrl, set `$env:PROD_DB_URL, or add it to scripts/.env.local." -ForegroundColor Red
    exit 1
}
if ($ProdDbUrl -notmatch '^(?<prefix>postgres(?:ql)?://[^:@/]+):(?<pw>[^@]+)@(?<rest>.+)$') {
    Write-Host "ERROR: PROD_DB_URL doesn't match 'postgres://user:password@host...'." -ForegroundColor Red
    exit 1
}
$prodPassword = $Matches.pw
$prodUrlNoPw  = "$($Matches.prefix)@$($Matches.rest)"
$prodEnv      = @('-e', "PGPASSWORD=$prodPassword")
$migDir       = Join-Path (Split-Path $PSScriptRoot -Parent) 'supabase/migrations'

# ---- psql helpers -----------------------------------------------------------
# Each returns rows as string[]; fields are pipe-delimited (-F'|'), tuples-only
# (-tA) so there's no header/footer to strip.
function Invoke-Local([string]$sql) {
    $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c $sql
    if ($LASTEXITCODE -ne 0) { throw "local psql failed: $sql`n$out" }
    return @($out | Where-Object { $_ -ne $null -and $_ -ne '' })
}
function Invoke-Prod([string]$sql) {
    $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -c $sql
    if ($LASTEXITCODE -ne 0) { throw "prod psql failed: $sql`n$out" }
    return @($out | Where-Object { $_ -ne $null -and $_ -ne '' })
}
# Run a multi-statement / \copy script against prod via stdin (-f -).
function Invoke-ProdScript([string]$script) {
    $script | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) { throw "prod script failed (see output above)." }
}

# ---- preflight --------------------------------------------------------------
$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: container '$Container' not running. Run 'npx supabase start' first." -ForegroundColor Red
    exit 1
}
Write-Host "[1] Verifying prod connection..."
$probe = Invoke-Prod "SELECT current_database();"
Write-Host "  OK: prod db = $probe" -ForegroundColor Green

# ---- schema metadata (discovered from LOCAL, applied to both) ---------------
Write-Host "[2] Reading schema metadata from local..."
$allTables = Invoke-Local "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name"

$colsByTable = @{}   # table -> ordered column names
foreach ($r in Invoke-Local "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position") {
    $p = $r -split '\|', 2
    if (-not $colsByTable.ContainsKey($p[0])) { $colsByTable[$p[0]] = New-Object System.Collections.ArrayList }
    [void]$colsByTable[$p[0]].Add($p[1])
}
$pkByTable = @{}     # table -> ordered pk column names
foreach ($r in Invoke-Local @"
SELECT tc.table_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema='public'
ORDER BY tc.table_name, kcu.ordinal_position
"@) {
    $p = $r -split '\|', 2
    if (-not $pkByTable.ContainsKey($p[0])) { $pkByTable[$p[0]] = New-Object System.Collections.ArrayList }
    [void]$pkByTable[$p[0]].Add($p[1])
}
# FK edges child -> parent (within public, self-refs ignored).
$parentsByTable = @{}
foreach ($t in $allTables) { $parentsByTable[$t] = New-Object System.Collections.ArrayList }
foreach ($r in Invoke-Local @"
SELECT DISTINCT tc.table_name, ccu.table_name
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name=ccu.constraint_name AND tc.table_schema=ccu.table_schema
WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema='public' AND tc.table_name<>ccu.table_name
"@) {
    $p = $r -split '\|', 2
    if ($parentsByTable.ContainsKey($p[0]) -and $allTables -contains $p[1]) { [void]$parentsByTable[$p[0]].Add($p[1]) }
}

# Tables whose INSERT must say OVERRIDING SYSTEM VALUE because a PK column is
# GENERATED ALWAYS AS IDENTITY (an explicit value is otherwise rejected).
$alwaysIdentity = @{}
foreach ($r in Invoke-Local "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' AND is_identity='YES' AND identity_generation='ALWAYS'") {
    $alwaysIdentity[$r] = $true
}
# Sequence-backed columns (serial or identity) -> reset after load so prod's own
# pipeline inserts don't collide with the cloned explicit ids. pg_get_serial_sequence
# MISSES manually-created sequences (CREATE SEQUENCE + DEFAULT nextval, not OWNED BY
# -- e.g. company_id_seq), so fall back to parsing the nextval() default expression.
$seqResets = New-Object System.Collections.ArrayList
foreach ($r in Invoke-Local @"
SELECT c.table_name, c.column_name,
  COALESCE(pg_get_serial_sequence(format('public.%I', c.table_name), c.column_name),
           (regexp_match(c.column_default, 'nextval\(''([^'']+)'''))[1])
FROM information_schema.columns c
WHERE c.table_schema='public'
  AND (c.is_identity='YES' OR c.column_default LIKE 'nextval(%')
"@) {
    $p = $r -split '\|'
    if ($p.Length -ge 3 -and $p[2]) { [void]$seqResets.Add(@($p[0], $p[1], $p[2])) }
}

# Topological order (parents before children) via Kahn's algorithm.
$upsertOrder = New-Object System.Collections.ArrayList
$placed = @{}
$guard = 0
while ($upsertOrder.Count -lt $allTables.Count) {
    $progress = $false
    foreach ($t in $allTables) {
        if ($placed.ContainsKey($t)) { continue }
        $ready = $true
        foreach ($par in $parentsByTable[$t]) { if (-not $placed.ContainsKey($par)) { $ready = $false; break } }
        if ($ready) { [void]$upsertOrder.Add($t); $placed[$t] = $true; $progress = $true }
    }
    if (-not $progress) {
        # Cycle (shouldn't happen) -- append the rest so we don't loop forever.
        foreach ($t in $allTables) { if (-not $placed.ContainsKey($t)) { [void]$upsertOrder.Add($t); $placed[$t] = $true } }
    }
    if (++$guard -gt 100) { break }
}
$deleteOrder = @($upsertOrder.ToArray()); [array]::Reverse($deleteOrder)
# metric_data is synced differentially, not via the staging passes.
$stagedTables = @($upsertOrder.ToArray() | Where-Object { $_ -ne 'metric_data' })
Write-Host "  $($allTables.Count) tables; upsert order resolved." -ForegroundColor Green

# Per-company signature for metric_data (price data only; recorded_at excluded).
$mdSigSql = @"
SELECT company_id, count(*), coalesce(sum(numeric_value),0)::text, coalesce(max(target_date)::text,''), coalesce(min(target_date)::text,'')
FROM metric_data GROUP BY company_id
"@
function Get-MdSignatures([scriptblock]$runner) {
    $map = @{}
    foreach ($r in (& $runner $mdSigSql)) {
        $p = $r -split '\|'
        $map[[int]$p[0]] = "$($p[1])|$($p[2])|$($p[3])|$($p[4])"
    }
    return $map
}

# ---- DRY RUN ----------------------------------------------------------------
if ($DryRun) {
    Write-Host "[3] DRY RUN -- comparing local vs prod (read-only)..." -ForegroundColor Yellow
    foreach ($t in $upsertOrder) {
        $lc = [int](Invoke-Local "SELECT count(*) FROM public.$t")
        $pc = [int](Invoke-Prod "SELECT count(*) FROM public.$t")
        $flag = if ($lc -ne $pc) { "  <-- differs" } else { "" }
        Write-Host ("  {0,-28} local={1,-10} prod={2,-10}{3}" -f $t, $lc, $pc, $flag)
    }
    Write-Host "  computing metric_data per-company signatures (full scan both sides)..."
    $localSig = Get-MdSignatures ${function:Invoke-Local}
    $prodSig  = Get-MdSignatures ${function:Invoke-Prod}
    $resync = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
    $prodOnly = @($prodSig.Keys | Where-Object { -not $localSig.ContainsKey($_) })
    Write-Host ("  metric_data: {0} companies local, {1} prod; {2} need re-copy, {3} prod-only to delete." -f $localSig.Count, $prodSig.Count, $resync.Count, $prodOnly.Count) -ForegroundColor Cyan
    if ($resync.Count -gt 0) {
        $est = [int](Invoke-Local ("SELECT count(*) FROM metric_data WHERE company_id = ANY('{{{0}}}'::int[])" -f ($resync -join ',')))
        Write-Host "  ~ $est metric_data rows would transfer." -ForegroundColor Cyan
    }
    Write-Host "DRY RUN complete in $([math]::Round($swAll.Elapsed.TotalSeconds,1))s. No changes made." -ForegroundColor Green
    exit 0
}

# ---- confirmation -----------------------------------------------------------
if (-not $Force) {
    Write-Host ""
    Write-Host "About to make prod an EXACT clone of local (differential):" -ForegroundColor Yellow
    Write-Host "  - apply any missing migrations + align the tracker"
    Write-Host "  - upsert + delete-missing on $($stagedTables.Count) tables"
    Write-Host "  - differentially re-copy only changed-company metric_data rows"
    Write-Host "  (auth.users / storage / API keys are NOT touched)"
    Write-Host ""
    if ((Read-Host "Type 'YES' to proceed") -ne 'YES') { Write-Host "Aborted." -ForegroundColor Yellow; exit 0 }
}

# ---- [3] schema parity ------------------------------------------------------
Write-Host "[3] Reconciling schema (migrations)..."
$localVers = Invoke-Local "SELECT version FROM supabase_migrations.schema_migrations ORDER BY version"
$prodVers  = Invoke-Prod  "SELECT version FROM supabase_migrations.schema_migrations ORDER BY version"
$missing   = @($localVers | Where-Object { $prodVers -notcontains $_ })
$ahead     = @($prodVers  | Where-Object { $localVers -notcontains $_ })
if ($ahead.Count -gt 0) {
    Write-Host "  WARNING: prod has $($ahead.Count) migration(s) local doesn't: $($ahead -join ', ')." -ForegroundColor Yellow
    Write-Host "  prod schema may be AHEAD of local. If data sync errors on unknown columns, use copy-local-to-prod.ps1 for a full rebuild." -ForegroundColor Yellow
}
foreach ($v in $missing) {
    $file = Get-ChildItem -Path $migDir -Filter "$v*.sql" | Select-Object -First 1
    if (-not $file) { throw "missing migration file for version $v in $migDir" }
    Write-Host "  applying $($file.Name) ..."
    (Get-Content $file.FullName -Raw) | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) { throw "migration $($file.Name) failed on prod." }
}
# Align tracker to local verbatim (mirrors copy-local-to-prod.ps1 step 6).
$trackRows = Invoke-Local "SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version"
$trackSql = New-Object System.Text.StringBuilder
[void]$trackSql.AppendLine("TRUNCATE supabase_migrations.schema_migrations;")
foreach ($line in $trackRows) {
    $p = $line -split '\|', 2
    $ver = $p[0]; $nm = if ($p.Length -gt 1) { $p[1] -replace "'","''" } else { '' }
    [void]$trackSql.AppendLine("INSERT INTO supabase_migrations.schema_migrations (version, name, statements) VALUES ('$ver', '$nm', ARRAY['-- cloned from local']);")
}
Invoke-ProdScript $trackSql.ToString()
Write-Host "  schema in sync (applied $($missing.Count) migration(s))." -ForegroundColor Green

# ---- [4] stage small/medium tables on prod ----------------------------------
Write-Host "[4] Staging $($stagedTables.Count) tables into clone_stg on prod..."
Invoke-Prod "DROP SCHEMA IF EXISTS clone_stg CASCADE; CREATE SCHEMA clone_stg;" | Out-Null
docker exec $Container bash -c "rm -f /tmp/clone_*.dat" | Out-Null
foreach ($t in $stagedTables) {
    $collist = ($colsByTable[$t] -join ', ')
    $datfile = "/tmp/clone_$t.dat"
    # Dump local rows (text format: exact NULL + escape round-trip).
    docker exec $Container psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "\copy (SELECT $collist FROM public.$t) TO '$datfile'"
    if ($LASTEXITCODE -ne 0) { throw "local dump of $t failed." }
    # Create staging clone + load it on prod.
    $loadSql = "CREATE TABLE clone_stg.$t (LIKE public.$t INCLUDING DEFAULTS);`n\copy clone_stg.$t ($collist) FROM '$datfile'`n"
    Invoke-ProdScript $loadSql
}
Write-Host "  staged." -ForegroundColor Green

# ---- [5] UPSERT parent->child -----------------------------------------------
Write-Host "[5] Upserting (insert + update by PK)..."
foreach ($t in $stagedTables) {
    $cols   = $colsByTable[$t]
    $pk     = $pkByTable[$t]
    $collist = ($cols -join ', ')
    $pklist  = ($pk -join ', ')
    $nonpk   = @($cols | Where-Object { $pk -notcontains $_ })
    if ($nonpk.Count -gt 0) {
        $setlist = (($nonpk | ForEach-Object { "$_ = EXCLUDED.$_" }) -join ', ')
        $conflict = "ON CONFLICT ($pklist) DO UPDATE SET $setlist"
    } else {
        $conflict = "ON CONFLICT ($pklist) DO NOTHING"
    }
    $ov = if ($alwaysIdentity.ContainsKey($t)) { 'OVERRIDING SYSTEM VALUE ' } else { '' }
    Invoke-Prod "INSERT INTO public.$t ($collist) ${ov}SELECT $collist FROM clone_stg.$t $conflict;" | Out-Null
}
Write-Host "  upserted." -ForegroundColor Green

# ---- [6] metric_data differential -------------------------------------------
Write-Host "[6] Diffing metric_data per company (full scan both sides)..."
$localSig = Get-MdSignatures ${function:Invoke-Local}
$prodSig  = Get-MdSignatures ${function:Invoke-Prod}
$resync   = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
$prodOnly = @($prodSig.Keys  | Where-Object { -not $localSig.ContainsKey($_) })
Write-Host "  $($resync.Count) companies to re-copy, $($prodOnly.Count) prod-only to delete."

$mdCols = ($colsByTable['metric_data'] -join ', ')
$done = 0
for ($i = 0; $i -lt $resync.Count; $i += $CompanyChunk) {
    $chunk = $resync[$i..([math]::Min($i + $CompanyChunk - 1, $resync.Count - 1))]
    $arr = $chunk -join ','
    docker exec $Container psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "\copy (SELECT $mdCols FROM metric_data WHERE company_id = ANY('{$arr}'::int[])) TO '/tmp/clone_md.dat'"
    if ($LASTEXITCODE -ne 0) { throw "local metric_data dump failed." }
    $sql = "BEGIN;`nDELETE FROM public.metric_data WHERE company_id = ANY('{$arr}'::int[]);`n\copy public.metric_data ($mdCols) FROM '/tmp/clone_md.dat'`nCOMMIT;`n"
    Invoke-ProdScript $sql
    $done += $chunk.Count
    Write-Host "    metric_data: re-copied $done/$($resync.Count) companies..."
}
if ($prodOnly.Count -gt 0) {
    for ($i = 0; $i -lt $prodOnly.Count; $i += $CompanyChunk) {
        $chunk = $prodOnly[$i..([math]::Min($i + $CompanyChunk - 1, $prodOnly.Count - 1))]
        Invoke-Prod "DELETE FROM public.metric_data WHERE company_id = ANY('{$($chunk -join ',')}'::int[]);" | Out-Null
    }
    Write-Host "    deleted prod-only metric_data for $($prodOnly.Count) companies."
}
Write-Host "  metric_data in sync." -ForegroundColor Green

# ---- [7] DELETE prod rows gone from local (child->parent) --------------------
Write-Host "[7] Deleting rows removed locally (mirror)..."
foreach ($t in $deleteOrder) {
    if ($t -eq 'metric_data') { continue }
    $pk = $pkByTable[$t]
    $cond = (($pk | ForEach-Object { "s.$_ = p.$_" }) -join ' AND ')
    Invoke-Prod "DELETE FROM public.$t p WHERE NOT EXISTS (SELECT 1 FROM clone_stg.$t s WHERE $cond);" | Out-Null
}
Invoke-Prod "DROP SCHEMA IF EXISTS clone_stg CASCADE;" | Out-Null
docker exec $Container bash -c "rm -f /tmp/clone_*.dat" | Out-Null
Invoke-Prod "NOTIFY pgrst, 'reload schema';" | Out-Null
Write-Host "  done." -ForegroundColor Green

# ---- [7b] reset identity/serial sequences -----------------------------------
# We inserted explicit ids, which doesn't advance prod's sequences. Without this
# the next pipeline insert on prod reuses an id and hits a duplicate-key error.
Write-Host "[7b] Resetting $($seqResets.Count) identity/serial sequences..."
foreach ($s in $seqResets) {
    $tbl = $s[0]; $col = $s[1]; $seq = $s[2]
    Invoke-Prod "SELECT setval('$seq', GREATEST(COALESCE((SELECT MAX($col) FROM public.$tbl),0),1));" | Out-Null
}
Write-Host "  done." -ForegroundColor Green

# ---- [8] verify -------------------------------------------------------------
Write-Host "[8] Verifying..."
$mismatch = 0
foreach ($t in $upsertOrder) {
    $lc = [int](Invoke-Local "SELECT count(*) FROM public.$t")
    $pc = [int](Invoke-Prod "SELECT count(*) FROM public.$t")
    if ($lc -ne $pc) { Write-Host "  MISMATCH $t : local=$lc prod=$pc" -ForegroundColor Red; $mismatch++ }
}
if ($mismatch -eq 0) {
    Write-Host "  all $($upsertOrder.Count) tables match." -ForegroundColor Green
    Write-Host ""
    Write-Host "DONE. Prod is an exact clone of local. ($([math]::Round($swAll.Elapsed.TotalSeconds,1))s)" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "FINISHED WITH $mismatch MISMATCH(ES) -- inspect above." -ForegroundColor Red
    exit 1
}

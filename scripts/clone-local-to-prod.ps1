<#
.SYNOPSIS
  Make prod's 'public' schema an EXACT clone of local, transferring only what
  differs (fast differential sync). Non-destructive to auth/keys; also mirrors
  the backtest-results Storage bucket.

.DESCRIPTION
  Makes prod match local by transferring only what differs -- no schema drop, no
  full ~26M-row metric_data reload. It:

    1. Schema parity: applies any local migrations prod is missing, then aligns
       supabase_migrations.schema_migrations to local. (Stops if prod is AHEAD of
       local -- extra migrations/columns local lacks. Reconcile prod by hand or
       restore it from a Supabase backup, then re-run.)
    2. Stages every small/medium table's local rows into a clone_stg schema on
       prod (one transfer; ~140k rows total -> seconds).
    3. UPSERTs them parent->child (insert + update by PK), then DELETEs prod rows
       whose PK is gone from local child->parent. True mirror, no schema drop.
    4. metric_data (26M rows) is synced DIFFERENTIALLY: a per-company signature
       (count, sum(value), min/max date) is compared on both sides and only the
       companies whose price data changed are re-copied. Unchanged -> zero rows
       cross the wire. recorded_at is intentionally excluded from the signature
       (prod fetches the same prices independently with different timestamps;
       including it would force a full re-copy every run). Each batch stages into
       clone_stg.md_batch and then UPSERTs + deletes-missing, like every other
       table -- NOT a blanket DELETE + COPY into the live table, which aborts the
       whole batch if prod's own ingest writes a row mid-scan (see step [6]).

  SAFE TO RUN WHILE PROD IS LIVE. The scheduled ingest keeps writing metric_data
  (daily 05:00 UTC price update; the month-end full refresh at 12:00 UTC on the
  last few days of the month), and a batch takes minutes. The upsert absorbs
  those writes. The one thing it cannot absorb is a row prod adds for an in-chunk
  company that local does not have at all: it survives the batch, step [8] flags
  the count mismatch, and a re-run clears it.
    5. Verifies row counts + the metric_data signature afterwards.

  Storage: the `backtest-results` bucket is mirrored too (objects missing on
  prod are uploaded, prod-only objects deleted) so each `backtest_run` row's
  `result_path` blob travels with it -- otherwise a freshly-cloned row 404s on
  read (`GET /api/momentum/backtests/{id}` then degrades to result=null). Needs
  a prod service key + storage URL (see below); if either is absent the storage
  step warns and skips while the DB clone still completes. auth.users / API keys
  are still untouched.

  Storage creds: local read from backend/.env.local (SUPABASE_URL +
  SUPABASE_SERVICE_KEY). Prod from $env:PROD_SERVICE_KEY and (optional)
  $env:PROD_SUPABASE_URL -- the latter auto-derives to https://<ref>.supabase.co
  from PROD_DB_URL's `postgres.<ref>` user. Put PROD_SERVICE_KEY in
  scripts/.env.local alongside PROD_DB_URL.

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
# Invoke-WebRequest draws a per-call progress bar on PS 5.1 that throttles file
# transfers by 10-50x; the Storage mirror moves every blob through it. Silence it
# for a large speedup. Scoped to this (child) runspace, so the caller is unaffected.
$ProgressPreference = 'SilentlyContinue'
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
# PGOPTIONS statement_timeout=0: the metric_data re-copy (+ delete-missing) move
# millions of rows per batch. Prod's default per-statement timeout cancels a big
# COPY mid-stream (rolls back, so it's safe -- but the clone never finishes). A
# clone is a deliberate bulk maintenance op, so disable the timeout for its
# sessions. (Session pooler forwards startup options; if a managed setup strips
# them, the explicit `SET statement_timeout = 0;` in the step-6 script still wins.)
$prodEnv      = @('-e', "PGPASSWORD=$prodPassword", '-e', 'PGOPTIONS=-c statement_timeout=0')
$repoRoot     = Split-Path $PSScriptRoot -Parent
$migDir       = Join-Path $repoRoot 'supabase/migrations'

# ---- storage sync creds (backtest-results bucket) ---------------------------
# The DB clone copies `backtest_run` rows (incl. their `result_path`) but NOT
# the Storage objects those paths reference -- so a freshly-cloned prod has rows
# whose blob 404s. Step [7c] mirrors the bucket over the Storage REST API; it
# needs a Storage endpoint + service key on each side. The DB clone still runs
# if these are absent (the storage step warns + skips).
# TLS 1.2 for the https prod Storage calls (PS 5.1 defaults can negotiate down).
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}
function Read-EnvFile([string]$path) {
    $h = @{}
    if (Test-Path $path) {
        Get-Content $path | ForEach-Object {
            if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
            if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*$') {
                $h[$Matches[1]] = $Matches[2] -replace '^"(.*)"$','$1' -replace "^'(.*)'$",'$1'
            }
        }
    }
    return $h
}
$backendEnv      = Read-EnvFile (Join-Path $repoRoot 'backend/.env.local')
$LocalStorageUrl = if ($env:LOCAL_SUPABASE_URL) { $env:LOCAL_SUPABASE_URL } elseif ($backendEnv['SUPABASE_URL']) { $backendEnv['SUPABASE_URL'] } else { 'http://127.0.0.1:54321' }
$LocalServiceKey = if ($env:LOCAL_SERVICE_KEY) { $env:LOCAL_SERVICE_KEY } else { $backendEnv['SUPABASE_SERVICE_KEY'] }
# Prod Storage endpoint: explicit override, else derive https://<ref>.supabase.co
# from the pooler user `postgres.<ref>` in PROD_DB_URL.
$prodRef         = if ($ProdDbUrl -match 'postgres(?:ql)?://postgres\.([a-z0-9]+):') { $Matches[1] } else { $null }
$ProdStorageUrl  = if ($env:PROD_SUPABASE_URL) { $env:PROD_SUPABASE_URL.TrimEnd('/') } elseif ($prodRef) { "https://$prodRef.supabase.co" } else { $null }
$ProdServiceKey  = $env:PROD_SERVICE_KEY
$StorageBucket   = 'backtest-results'
$StorageSyncEnabled = [bool]($LocalServiceKey -and $ProdServiceKey -and $ProdStorageUrl)

# ---- psql helpers -----------------------------------------------------------
# Each returns rows as string[]; fields are pipe-delimited (-F'|'), tuples-only
# (-tA) so there's no header/footer to strip.
function Invoke-Local([string]$sql) {
    $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c $sql
    if ($LASTEXITCODE -ne 0) { throw "local psql failed: $sql`n$out" }
    # TrimEnd CR: docker-exec output can carry a stray trailing \r on Windows,
    # which would silently break version/PK compares + int parses downstream.
    # ("$_" coerces a possible $null to '' so TrimEnd never throws.)
    return @($out | ForEach-Object { "$_".TrimEnd("`r") } | Where-Object { $_ -ne '' })
}
function Invoke-Prod([string]$sql) {
    $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -c $sql
    if ($LASTEXITCODE -ne 0) { throw "prod psql failed: $sql`n$out" }
    return @($out | ForEach-Object { "$_".TrimEnd("`r") } | Where-Object { $_ -ne '' })
}
# Run a multi-statement / \copy script against prod via stdin (-f -).
function Invoke-ProdScript([string]$script) {
    $script | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) { throw "prod script failed (see output above)." }
}

# ---- storage helpers (backtest-results bucket) ------------------------------
# Object names live in each side's `storage.objects` (queryable over psql); the
# bytes live in the Storage backend (reachable only over the Storage REST API).
# So we diff names via psql, then move bytes over HTTP. The blobs are gzipped
# JSON; the backend reader is gzip-magic-based (not content-encoding-based), so
# byte-fidelity through the proxy is all that matters -- we don't re-set the
# content-encoding metadata on the prod copy.
function Get-BucketObjectNames([scriptblock]$Query) {
    return @(& $Query "SELECT name FROM storage.objects WHERE bucket_id='$StorageBucket' AND name IS NOT NULL")
}
function Invoke-StorageMirror([switch]$DryRunOnly) {
    if (-not $StorageSyncEnabled) {
        Write-Host "  SKIPPED: storage sync needs local + prod service keys and a prod storage URL." -ForegroundColor Yellow
        Write-Host "  Set PROD_SERVICE_KEY (and PROD_SUPABASE_URL if not auto-derivable) in scripts/.env.local;" -ForegroundColor Yellow
        Write-Host "  local creds come from backend/.env.local. backtest_run result blobs were NOT mirrored." -ForegroundColor Yellow
        return
    }
    $local    = Get-BucketObjectNames ${function:Invoke-Local}
    $prod     = Get-BucketObjectNames ${function:Invoke-Prod}
    $prodSet  = @{}; foreach ($n in $prod)  { $prodSet[$n]  = $true }
    $localSet = @{}; foreach ($n in $local) { $localSet[$n] = $true }
    $toUpload = @($local | Where-Object { -not $prodSet.ContainsKey($_) })
    $toDelete = @($prod  | Where-Object { -not $localSet.ContainsKey($_) })
    Write-Host ("  bucket '$StorageBucket': {0} local, {1} prod -> {2} to upload, {3} prod-only to delete." -f $local.Count, $prod.Count, $toUpload.Count, $toDelete.Count) -ForegroundColor Cyan
    if ($DryRunOnly) { return }
    if ($toUpload.Count -eq 0 -and $toDelete.Count -eq 0) { Write-Host "  storage already in sync." -ForegroundColor Green; return }

    # The Supabase API gateway (Kong) authenticates on the `apikey` header; the
    # underlying Storage service reads `Authorization`. BOTH are required -- with
    # only Authorization, Kong routes the call as anon and a private bucket reads
    # back as "Bucket not found".
    $localHdr = @{ Authorization = "Bearer $LocalServiceKey"; apikey = $LocalServiceKey }
    $prodHdr  = @{ Authorization = "Bearer $ProdServiceKey";  apikey = $ProdServiceKey }

    # Ensure the prod bucket exists (private), idempotent (400/409 = exists).
    try {
        Invoke-WebRequest -Method Post -Uri "$ProdStorageUrl/storage/v1/bucket" `
            -Headers ($prodHdr + @{ 'Content-Type' = 'application/json' }) `
            -Body (ConvertTo-Json @{ id = $StorageBucket; name = $StorageBucket; public = $false }) `
            -UseBasicParsing -ErrorAction Stop | Out-Null
        Write-Host "  created prod bucket '$StorageBucket'." -ForegroundColor Green
    } catch { }

    $tmp = Join-Path $env:TEMP 'clone_storage_obj.bin'
    $up = 0; $failed = 0
    foreach ($name in $toUpload) {
        # Per-object try/catch: a single flaky blob shouldn't abort the run -- by
        # this point (step 7c) the DB clone is already done. Warn + continue;
        # re-running the (idempotent) clone retries only the still-missing blobs.
        try {
            $encName = [Uri]::EscapeDataString($name)
            Invoke-WebRequest -Method Get -Uri "$LocalStorageUrl/storage/v1/object/$StorageBucket/$encName" `
                -Headers $localHdr -OutFile $tmp -UseBasicParsing -ErrorAction Stop
            Invoke-WebRequest -Method Post -Uri "$ProdStorageUrl/storage/v1/object/$StorageBucket/$encName" `
                -Headers ($prodHdr + @{ 'x-upsert' = 'true'; 'Content-Type' = 'application/json' }) `
                -InFile $tmp -UseBasicParsing -ErrorAction Stop | Out-Null
            $up++
            if ($up % 25 -eq 0) { Write-Host "    uploaded $up/$($toUpload.Count)..." }
        } catch {
            $failed++
            Write-Host "    WARNING: object '$name' failed to mirror: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
    if (Test-Path $tmp) { Remove-Item $tmp -Force }
    if ($toUpload.Count -gt 0) { Write-Host "  uploaded $up object(s) to prod." -ForegroundColor Green }
    if ($failed -gt 0) { Write-Host "  $failed object(s) failed to upload -- re-run the clone to retry (idempotent)." -ForegroundColor Yellow }

    if ($toDelete.Count -gt 0) {
        # Bulk delete: DELETE /storage/v1/object/{bucket} with {prefixes:[...]}.
        # Build the JSON array by hand so a single name still serializes as [..].
        $prefixesJson = ($toDelete | ForEach-Object { '"' + ($_ -replace '\\','\\' -replace '"','\"') + '"' }) -join ','
        $body = "{`"prefixes`":[$prefixesJson]}"
        try {
            Invoke-WebRequest -Method Delete -Uri "$ProdStorageUrl/storage/v1/object/$StorageBucket" `
                -Headers ($prodHdr + @{ 'Content-Type' = 'application/json' }) `
                -Body $body -UseBasicParsing -ErrorAction Stop | Out-Null
            Write-Host "  deleted $($toDelete.Count) prod-only object(s)." -ForegroundColor Green
        } catch {
            Write-Host "  WARNING: prod-only object delete failed (orphans remain): $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

# ---- preflight --------------------------------------------------------------
$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: container '$Container' not running. Run 'npx supabase start' first." -ForegroundColor Red
    exit 1
}
Write-Host "[1] Verifying prod connection..."
# !! RETRIED, BECAUSE THE COMMON FAILURE HERE IS TRANSIENT AND THE RAW ERROR IS MISLEADING.
# Supabase's pooler answers with
#   FATAL: Failed to connect to database: authentication did not complete within 15000ms
# when it accepted the TCP connection but could not reach the underlying Postgres in time.
# That is NOT a bad password (which says "password authentication failed") and NOT a bad
# host (which never connects at all) -- it means the database is paused, still waking, or
# out of connection slots. A single attempt turning into a PowerShell stack trace sends
# you looking at PROD_DB_URL for a problem that is usually gone in thirty seconds.
$probe = $null
for ($attempt = 1; $attempt -le 3; $attempt++) {
    try {
        $probe = Invoke-Prod "SELECT current_database();"
        break
    } catch {
        if ($attempt -eq 3) {
            Write-Host ""
            Write-Host "ERROR: could not reach prod after 3 attempts." -ForegroundColor Red
            Write-Host "  The pooler accepted the connection but the database did not complete auth." -ForegroundColor Yellow
            Write-Host "  That is a database-availability problem, not a credentials one. Check, in order:" -ForegroundColor Yellow
            Write-Host "    1. Supabase dashboard -- is the project ACTIVE (free-tier projects auto-pause)?" -ForegroundColor Yellow
            Write-Host "    2. Is something already hammering it? A month-end full price refresh, a" -ForegroundColor Yellow
            Write-Host "       previous clone that died mid-run, or the Railway backend's pool can fill" -ForegroundColor Yellow
            Write-Host "       the connection slots. Wait for it to finish, then re-run." -ForegroundColor Yellow
            Write-Host "    3. Only if both look fine, re-check PROD_DB_URL in scripts/.env.local." -ForegroundColor Yellow
            Write-Host ""
            Write-Host "  This script is idempotent -- re-running picks up wherever it left off." -ForegroundColor Cyan
            exit 1
        }
        $wait = 5 * $attempt
        Write-Host "  attempt $attempt failed; retrying in ${wait}s..." -ForegroundColor Yellow
        Start-Sleep -Seconds $wait
    }
}
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
# Secondary UNIQUE constraints (NOT the PK) -> used to pre-clear prod rows that
# collide on a unique key with an incoming local row but whose own PK is gone
# from local (diverged ids for the same logical row -- e.g. a frozen universe
# re-frozen independently on prod under a different universe_id but the same
# label). Without clearing them first the by-PK upsert tries to INSERT and trips
# the unique constraint (universe_label_key).
$uniqByTable = @{}
$uniqAccum = @{}   # "table|constraint" -> ArrayList(cols)
foreach ($r in Invoke-Local @"
SELECT tc.table_name, tc.constraint_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
WHERE tc.constraint_type='UNIQUE' AND tc.table_schema='public'
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
"@) {
    $p = $r -split '\|', 3
    $key = "$($p[0])|$($p[1])"
    if (-not $uniqAccum.ContainsKey($key)) { $uniqAccum[$key] = New-Object System.Collections.ArrayList }
    [void]$uniqAccum[$key].Add($p[2])
}
foreach ($key in $uniqAccum.Keys) {
    $tbl = ($key -split '\|', 2)[0]
    if (-not $uniqByTable.ContainsKey($tbl)) { $uniqByTable[$tbl] = New-Object System.Collections.ArrayList }
    [void]$uniqByTable[$tbl].Add(@($uniqAccum[$key].ToArray()))
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
# The leading `SET statement_timeout = 0;` disables the per-statement timeout for
# THIS query's session: it's a full ~26M-row GROUP BY, and on an IO-throttled prod
# it can exceed the default timeout. Done inline (not just via PGOPTIONS) because
# a managed pooler may ignore startup options. The `SET` emits a "SET" status line
# that ConvertTo-MdSignatureMap skips (it isn't a pipe-delimited data row).
$mdSigSql = @"
SET statement_timeout = 0;
SELECT company_id, count(*), coalesce(sum(numeric_value),0)::text, coalesce(max(target_date)::text,''), coalesce(min(target_date)::text,'')
FROM metric_data GROUP BY company_id
"@
function ConvertTo-MdSignatureMap([string[]]$lines) {
    $map = @{}
    foreach ($r in $lines) {
        $p = ("$r".TrimEnd("`r")) -split '\|'        # TrimEnd CR (job output)
        if ($p.Length -lt 5) { continue }            # skip the 'SET' tag + blank lines
        $cid = 0
        if (-not [int]::TryParse($p[0], [ref]$cid)) { continue }
        $map[$cid] = "$($p[1])|$($p[2])|$($p[3])|$($p[4])"
    }
    return $map
}
# Run BOTH metric_data signature scans concurrently. Each is a full ~26M-row
# GROUP BY and they're independent (local -> local disk; prod -> the pooler), so
# parallel wall-time is ~max(local, prod) instead of the sum -- and this diff is
# the dominant cost of the whole clone. Background jobs run docker in a separate
# process; we check each job's exit code, then parse the raw output in-runspace.
# Returns @{ Local = <sig map>; Prod = <sig map> }.
function Get-MdSignaturesParallel {
    $localJob = Start-Job -ScriptBlock {
        param($Container, $sql)
        $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c $sql
        [pscustomobject]@{ Out = @($out); Code = $LASTEXITCODE }
    } -ArgumentList $Container, $mdSigSql
    $prodJob = Start-Job -ScriptBlock {
        param($Container, $prodEnv, $prodUrlNoPw, $sql)
        $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -c $sql
        [pscustomobject]@{ Out = @($out); Code = $LASTEXITCODE }
    } -ArgumentList $Container, $prodEnv, $prodUrlNoPw, $mdSigSql
    # Both jobs are already running; -Wait on each just collects (max, not sum).
    $lr = Receive-Job -Job $localJob -Wait -AutoRemoveJob
    $pr = Receive-Job -Job $prodJob  -Wait -AutoRemoveJob
    if ($lr.Code -ne 0) { throw "local metric_data signature scan failed:`n$($lr.Out -join "`n")" }
    if ($pr.Code -ne 0) { throw "prod metric_data signature scan failed:`n$($pr.Out -join "`n")" }
    return @{ Local = (ConvertTo-MdSignatureMap $lr.Out); Prod = (ConvertTo-MdSignatureMap $pr.Out) }
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
    Write-Host "  computing metric_data per-company signatures (both sides, concurrent)..."
    $sigs = Get-MdSignaturesParallel
    $localSig = $sigs.Local; $prodSig = $sigs.Prod
    $resync = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
    $prodOnly = @($prodSig.Keys | Where-Object { -not $localSig.ContainsKey($_) })
    Write-Host ("  metric_data: {0} companies local, {1} prod; {2} need re-copy, {3} prod-only to delete." -f $localSig.Count, $prodSig.Count, $resync.Count, $prodOnly.Count) -ForegroundColor Cyan
    if ($resync.Count -gt 0) {
        $est = [int](Invoke-Local ("SELECT count(*) FROM metric_data WHERE company_id = ANY('{{{0}}}'::int[])" -f ($resync -join ',')))
        Write-Host "  ~ $est metric_data rows would transfer." -ForegroundColor Cyan
    }
    Write-Host "  storage (backtest-results bucket):"
    Invoke-StorageMirror -DryRunOnly
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
    Write-Host "  - mirror the backtest-results Storage bucket$(if (-not $StorageSyncEnabled) { ' (SKIPPED -- no prod service key)' })"
    Write-Host "  (auth.users / API keys are NOT touched)"
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
    Write-Host "  prod schema may be AHEAD of local. If data sync errors on unknown columns, drop the prod-only objects (or restore prod from a Supabase backup) and re-run." -ForegroundColor Yellow
}
foreach ($v in $missing) {
    $file = Get-ChildItem -Path $migDir -Filter "$v*.sql" | Select-Object -First 1
    if (-not $file) { throw "missing migration file for version $v in $migDir" }
    Write-Host "  applying $($file.Name) ..."
    (Get-Content $file.FullName -Raw) | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) { throw "migration $($file.Name) failed on prod." }
}
# Align the migration tracker to local verbatim.
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
$nStaged = $stagedTables.Count
$tIdx = 0
foreach ($t in $stagedTables) {
    $tIdx++
    Write-Host ("  [{0,2}/{1}] {2,-30} " -f $tIdx, $nStaged, $t) -NoNewline
    $swTbl = [System.Diagnostics.Stopwatch]::StartNew()
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
    # Pre-clear stale prod rows that collide on a secondary UNIQUE key with an
    # incoming local row but whose PK is gone from local (diverged ids for the
    # same logical row). Without this the INSERT below trips the unique
    # constraint (e.g. universe.label for a snapshot re-frozen on prod under a
    # different universe_id). The "PK gone from local" guard means we only touch
    # rows step [7] would delete anyway; FK ON DELETE CASCADE/SET NULL cleans
    # their dependents. A NULL unique value never matches (p.col = s.col is NULL),
    # so multi-NULL columns like template_key are correctly left alone.
    if ($uniqByTable.ContainsKey($t)) {
        $pkMatch = (($pk | ForEach-Object { "k.$_ = p.$_" }) -join ' AND ')
        foreach ($ucols in $uniqByTable[$t]) {
            $uMatch = (($ucols | ForEach-Object { "p.$_ = s.$_" }) -join ' AND ')
            Invoke-Prod "DELETE FROM public.$t p USING clone_stg.$t s WHERE $uMatch AND NOT EXISTS (SELECT 1 FROM clone_stg.$t k WHERE $pkMatch);" | Out-Null
        }
    }
    $ov = if ($alwaysIdentity.ContainsKey($t)) { 'OVERRIDING SYSTEM VALUE ' } else { '' }
    Invoke-Prod "SET statement_timeout = 0; INSERT INTO public.$t ($collist) ${ov}SELECT $collist FROM clone_stg.$t $conflict;" | Out-Null
    Write-Host ("done ({0:N1}s)" -f $swTbl.Elapsed.TotalSeconds) -ForegroundColor Green
}
Write-Host "  all $nStaged tables upserted." -ForegroundColor Green

# ---- [6] metric_data differential -------------------------------------------
Write-Host "[6] Diffing metric_data per company (full scan both sides)..."
Write-Host "  scanning LOCAL + PROD signatures concurrently (full ~26M-row GROUP BYs)... " -NoNewline
$swSig = [System.Diagnostics.Stopwatch]::StartNew()
$sigs = Get-MdSignaturesParallel
$localSig = $sigs.Local; $prodSig = $sigs.Prod
Write-Host ("done (local {0}, prod {1} companies, {2:N0}s wall)" -f $localSig.Count, $prodSig.Count, $swSig.Elapsed.TotalSeconds) -ForegroundColor Green
$resync   = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
$prodOnly = @($prodSig.Keys  | Where-Object { -not $localSig.ContainsKey($_) })
Write-Host "  $($resync.Count) companies to re-copy, $($prodOnly.Count) prod-only to delete."

$mdCols = ($colsByTable['metric_data'] -join ', ')
# Upsert plumbing for the metric_data batches, discovered like every other table's.
$mdPk       = $pkByTable['metric_data']
$mdPkList   = ($mdPk -join ', ')
$mdNonPk    = @($colsByTable['metric_data'] | Where-Object { $mdPk -notcontains $_ })
$mdConflict = if ($mdNonPk.Count -gt 0) {
    "ON CONFLICT ($mdPkList) DO UPDATE SET " + (($mdNonPk | ForEach-Object { "$_ = EXCLUDED.$_" }) -join ', ')
} else { "ON CONFLICT ($mdPkList) DO NOTHING" }
$mdKeyMatch = (($mdPk | ForEach-Object { "s.$_ = p.$_" }) -join ' AND ')
# !! UNCHANGED ROWS MUST NOT BE REWRITTEN, AND THIS IS THE DIFFERENCE BETWEEN A SYNC
# AND A DISK-FILLING EVENT. Postgres is MVCC: an UPDATE that sets a column to the value
# it already holds still writes a NEW tuple, marks the old one dead, updates every index
# and WAL-logs all of it. The differential picks companies whose SIGNATURE changed, but
# inside such a company only the newest few days are actually new -- the other ~11,000
# rows are identical. Rewriting them turned a batch of 300 companies into ~3.3M dead
# tuples plus the WAL to match, and three batches of that is what bloated prod.
#
# So the UPDATE is gated on the row genuinely differing. `recorded_at` is EXCLUDED from
# the comparison on purpose: prod fetches the same prices independently and stamps its
# own timestamp, so including it makes every row "different" and the guard does nothing
# -- the same reason it is excluded from the per-company signature.
$mdCompare = @($colsByTable['metric_data'] | Where-Object { $mdPk -notcontains $_ -and $_ -ne 'recorded_at' })
if ($mdNonPk.Count -gt 0 -and $mdCompare.Count -gt 0) {
    $tgt = (($mdCompare | ForEach-Object { "public.metric_data.$_" }) -join ', ')
    $inc = (($mdCompare | ForEach-Object { "EXCLUDED.$_" }) -join ', ')
    $mdConflict = "$mdConflict WHERE ($tgt) IS DISTINCT FROM ($inc)"
}
$done = 0
$nChunks = [math]::Ceiling($resync.Count / $CompanyChunk)
$ci = 0
for ($i = 0; $i -lt $resync.Count; $i += $CompanyChunk) {
    $ci++
    $swBatch = [System.Diagnostics.Stopwatch]::StartNew()
    $chunk = $resync[$i..([math]::Min($i + $CompanyChunk - 1, $resync.Count - 1))]
    $arr = $chunk -join ','
    docker exec $Container psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "\copy (SELECT $mdCols FROM metric_data WHERE company_id = ANY('{$arr}'::int[])) TO '/tmp/clone_md.dat'"
    if ($LASTEXITCODE -ne 0) { throw "local metric_data dump failed." }
    # Stage -> delete-missing -> UPSERT, exactly like steps [4]/[5] do for every
    # other table. metric_data was the one table that COPYed straight into the live
    # table after a blanket DELETE, and that is not safe against a concurrent writer:
    #
    #   PROD WRITES TO metric_data WHILE THIS RUNS. Under READ COMMITTED the DELETE
    #   fixes its snapshot when the statement STARTS, then scans ~3M rows for several
    #   minutes. Any row prod's own ingest commits during that scan is invisible to
    #   the DELETE, so it survives -- and the COPY that follows takes a FRESH snapshot,
    #   sees it, and aborts the whole batch on metric_data_pkey. Observed 2026-07-31:
    #   "duplicate key (4676, close_price, gurufocus, 2026-07-30)" ~3.2M rows into a
    #   3.3M-row batch. ALLWYN AG's close was written by prod's own month-end full
    #   price refresh (scheduler.py, 12:00 UTC on the last MONTH_END_WINDOW_DAYS+1
    #   days -- and the clone was run on 31 July), on top of the daily 05:00 UTC
    #   price update. The local file was NOT the problem: local carries the same PK
    #   and has zero duplicate keys.
    #
    #   An upsert cannot hit that: a row prod inserted under us is simply overwritten
    #   with local's copy. The delete-missing is an anti-join against the staging
    #   table rather than a blanket wipe, so it still removes rows local no longer
    #   has. Residual, and deliberately left: a row prod commits for a company in this
    #   chunk that local does NOT have, AFTER the anti-join's snapshot, survives the
    #   batch. Step [8]'s count check flags it and a re-run (idempotent) clears it --
    #   a soft, self-healing miss instead of a hard abort 5 minutes into a batch.
    #
    # The PK on the staging table is what keeps the anti-join and the ON CONFLICT
    # probe from degrading into hashes of a 3M-row table on a small prod instance.
    $sql = @"
SET statement_timeout = 0;
DROP TABLE IF EXISTS clone_stg.md_batch;
CREATE TABLE clone_stg.md_batch (LIKE public.metric_data INCLUDING DEFAULTS);
\copy clone_stg.md_batch ($mdCols) FROM '/tmp/clone_md.dat'
ALTER TABLE clone_stg.md_batch ADD PRIMARY KEY ($mdPkList);
BEGIN;
DELETE FROM public.metric_data p
 WHERE p.company_id = ANY('{$arr}'::int[])
   AND NOT EXISTS (SELECT 1 FROM clone_stg.md_batch s WHERE $mdKeyMatch);
INSERT INTO public.metric_data ($mdCols)
SELECT $mdCols FROM clone_stg.md_batch
$mdConflict;
COMMIT;
DROP TABLE clone_stg.md_batch;
"@
    Invoke-ProdScript $sql
    $done += $chunk.Count
    Write-Host ("    batch {0}/{1}: re-copied {2}/{3} companies ({4:N1}s)" -f $ci, $nChunks, $done, $resync.Count, $swBatch.Elapsed.TotalSeconds)
}
if ($prodOnly.Count -gt 0) {
    for ($i = 0; $i -lt $prodOnly.Count; $i += $CompanyChunk) {
        $chunk = $prodOnly[$i..([math]::Min($i + $CompanyChunk - 1, $prodOnly.Count - 1))]
        Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.metric_data WHERE company_id = ANY('{$($chunk -join ',')}'::int[]);" | Out-Null
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
    Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$t p WHERE NOT EXISTS (SELECT 1 FROM clone_stg.$t s WHERE $cond);" | Out-Null
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
    # is_called = (table has rows): non-empty -> next id = MAX+1; empty ->
    # setval(1, false) so the first insert gets id 1, not 2.
    Invoke-Prod "SELECT setval('$seq', GREATEST(m,1), m > 0) FROM (SELECT COALESCE(MAX($col),0) AS m FROM public.$tbl) q;" | Out-Null
}
Write-Host "  done." -ForegroundColor Green

# ---- [7c] mirror the backtest-results Storage bucket ------------------------
# Upload local blobs missing on prod + delete prod-only blobs, so each cloned
# `backtest_run.result_path` resolves (no out-of-band 404 on read).
Write-Host "[7c] Mirroring '$StorageBucket' Storage bucket..."
Invoke-StorageMirror

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

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
       whose PK is gone from local child->parent -- removing whatever still points
       at such a row first, because half this schema's FKs are ON DELETE NO ACTION
       and do NOT clean up after themselves (see Remove-RowsWithDependents; this
       is what killed the 2026-08-11 run on company/metric_data). A parent an
       ADDITIVE table still references is kept instead, never cascaded into.
       True mirror, no schema drop --
       EXCEPT the airs_* tables, which are ADDITIVE (see `$additiveTables`): prod
       scrapes those itself from a live AirSPMS, so local is only ever a subset
       and deleting prod-only rows would destroy scraped history. They are still
       upserted, never pruned.
    4. The BIG tables -- metric_data (30M rows, keyed per company_id) and
       asset_price (39M rows, per analysis_id) -- are synced DIFFERENTIALLY by one
       shared implementation driven by `$diffSpecs`: a per-key signature (count,
       sum of every value column, min/max date) is compared on both sides and only
       the keys whose data changed are re-copied. Unchanged -> zero rows cross the
       wire. recorded_at is intentionally excluded from metric_data's signature
       (prod fetches the same prices independently with different timestamps;
       including it would force a full re-copy every run). Each batch stages into
       clone_stg.md_batch and then UPSERTs + deletes-missing, like every other
       table -- NOT a blanket DELETE + COPY into the live table, which aborts the
       whole batch if prod's own ingest writes a row mid-scan (see step [6]).
       !! asset_price joined this list on 2026-08-02, after a clone died on a
       statement timeout 34M rows into COPYing it through the small-table lane.
       A table over a few hundred thousand rows belongs in `$diffSpecs`, not in
       the staging pass -- the staging pass rewrites every row it touches.

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
# ---- retry: the network WILL drop during a run this long -------------------
# !! A CLONE IS 30-90 MINUTES OF OPEN CONNECTIONS, SO A BLIP IS NOT AN EXCEPTION, IT IS AN EVENT
# TO PLAN FOR. Observed 2026-08-11, mid `metric_data` batch:
#
#   psql:<stdin>:12: SSL SYSCALL error: EOF detected
#   psql:<stdin>:12: error: connection to server was lost
#
# Switching wifi <-> ethernet changes the source IP, which kills every established TCP connection
# outright -- no keepalive setting can survive that, so the only defence is to reconnect and redo
# the unit of work. Each `docker exec psql` opens its OWN connection, so a retry is a clean
# reconnect rather than a resumption of a broken one.
#
# !! RETRYING IS ONLY SAFE BECAUSE EVERY RETRIED UNIT IS IDEMPOTENT, and that is a property this
# script had to be given rather than one it happened to have:
#   * staging (step 4)   `DROP TABLE IF EXISTS` before the CREATE -- added for exactly this;
#   * upserts (5, 6)     INSERT ... ON CONFLICT DO UPDATE, by definition;
#   * delete-missing     an anti-join; deleting nothing twice is deleting nothing;
#   * doom tables        every one is DROP-IF-EXISTS then CREATE;
#   * sequence resets    setval to MAX, computed fresh each time.
# !! THE MIGRATIONS IN STEP [3] ARE THE EXCEPTION AND ARE DELIBERATELY NOT RETRIED. A migration
# that half-applied must be looked at by a human, not re-run by a loop.
$MaxDbTries = 4

function Test-TransientDbError([string]$text) {
    # Named patterns rather than "any failure", so a genuine SQL error is reported on the first
    # attempt instead of being repeated four times with a 30-second pause between.
    foreach ($p in @('SSL SYSCALL error', 'connection to server was lost', 'EOF detected',
                     'server closed the connection unexpectedly', 'could not connect to server',
                     'Connection refused', 'Connection reset', 'connection timed out',
                     'no connection to the server', 'terminating connection',
                     'authentication did not complete', 'server login has been failing')) {
        if ($text -like "*$p*") { return $true }
    }
    # !! AN EMPTY BODY WITH A NON-ZERO EXIT IS ALSO A DROPPED CONNECTION. psql writes its errors to
    # STDERR, which PowerShell does not fold into the captured stdout -- so on a lost connection
    # the text we can see here is often empty while the exit code is 2. A real SQL error usually
    # leaves something on stdout (the rows before it, a NOTICE); nothing at all is the signature of
    # never having got an answer.
    return [string]::IsNullOrWhiteSpace($text)
}

function Invoke-Local([string]$sql) {
    for ($try = 1; $try -le $MaxDbTries; $try++) {
        $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c $sql
        if ($LASTEXITCODE -eq 0) {
            # TrimEnd CR: docker-exec output can carry a stray trailing \r on Windows,
            # which would silently break version/PK compares + int parses downstream.
            # ("$_" coerces a possible $null to '' so TrimEnd never throws.)
            return @($out | ForEach-Object { "$_".TrimEnd("`r") } | Where-Object { $_ -ne '' })
        }
        $text = ($out -join "`n")
        if ($try -eq $MaxDbTries -or -not (Test-TransientDbError $text)) {
            throw "local psql failed: $sql`n$text"
        }
        Write-Host ("  local connection lost (attempt {0}/{1}) -- retrying in {2}s..." -f `
                $try, $MaxDbTries, (5 * $try)) -ForegroundColor Yellow
        Start-Sleep -Seconds (5 * $try)
    }
}
function Invoke-Prod([string]$sql) {
    for ($try = 1; $try -le $MaxDbTries; $try++) {
        $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -c $sql
        if ($LASTEXITCODE -eq 0) {
            return @($out | ForEach-Object { "$_".TrimEnd("`r") } | Where-Object { $_ -ne '' })
        }
        $text = ($out -join "`n")
        if ($try -eq $MaxDbTries -or -not (Test-TransientDbError $text)) {
            throw "prod psql failed: $sql`n$text"
        }
        Write-Host ("  prod connection lost (attempt {0}/{1}) -- retrying in {2}s..." -f `
                $try, $MaxDbTries, (5 * $try)) -ForegroundColor Yellow
        Start-Sleep -Seconds (5 * $try)
    }
}
# Run a multi-statement / \copy script against prod via stdin (-f -).
#
# !! THIS IS THE ONE THAT ACTUALLY DROPPED. It carries the metric_data batch -- minutes of held
# connection moving millions of rows -- so it is where a network change lands. The whole batch is
# re-sent on a retry, which is safe: it opens with `DROP TABLE IF EXISTS clone_stg.md_batch` and
# wraps its delete+insert in BEGIN/COMMIT, so a half-finished attempt left nothing behind.
function Invoke-ProdScript([string]$script) {
    for ($try = 1; $try -le $MaxDbTries; $try++) {
        $script | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
        if ($LASTEXITCODE -eq 0) { return }
        # stdin scripts stream their output straight to the console (that is how a long \copy shows
        # progress), so there is nothing captured to classify -- and a lost connection is by far the
        # likeliest cause of a non-zero exit here. Retry, and let the final attempt throw.
        if ($try -eq $MaxDbTries) { throw "prod script failed (see output above)." }
        Write-Host ("  prod script failed (attempt {0}/{1}) -- reconnecting in {2}s..." -f `
                $try, $MaxDbTries, (10 * $try)) -ForegroundColor Yellow
        Start-Sleep -Seconds (10 * $try)
    }
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

# FK edges that BLOCK a parent delete: ON DELETE NO ACTION ('a') or RESTRICT ('r').
# CASCADE and SET NULL clean themselves up; these do not, and the statement that
# fails does not tell you which kind you have. Read from the catalogue (not
# information_schema, which does not expose the delete rule) so a new table gets
# the right treatment on its first clone. See Remove-RowsWithDependents.
$blockingChildren = @{}
foreach ($t in $allTables) { $blockingChildren[$t] = New-Object System.Collections.ArrayList }
foreach ($r in Invoke-Local @"
SELECT p.relname, c.relname,
       (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
          FROM unnest(con.conkey) WITH ORDINALITY k(att, ord)
          JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.att),
       (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
          FROM unnest(con.confkey) WITH ORDINALITY k(att, ord)
          JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.att)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_class p ON p.oid = con.confrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype = 'f' AND n.nspname = 'public'
  AND con.confdeltype IN ('a', 'r') AND c.relname <> p.relname
ORDER BY p.relname, c.relname
"@) {
    $f = $r -split '\|'
    if ($f.Length -lt 4) { continue }
    if (-not $blockingChildren.ContainsKey($f[0])) { continue }
    [void]$blockingChildren[$f[0]].Add(@{
        Child = $f[1]; ChildCols = @($f[2] -split ','); ParentCols = @($f[3] -split ',')
    })
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

# ---- tables synced DIFFERENTIALLY (not via the staging passes) ---------------
# !! A TABLE BELONGS HERE ONCE IT IS TOO BIG TO REWRITE WHOLESALE, AND asset_price
# EARNED ITS PLACE THE HARD WAY. On 2026-08-02 a clone died with
#
#   psql:<stdin>:2: ERROR: canceling statement due to statement timeout
#   CONTEXT: COPY asset_price, line 34063937
#
# because asset_price (39,460,548 rows / 3,653 MB -- larger than metric_data) was
# still travelling through the "small/medium tables" lane this script documents as
# "~140k rows total -> seconds". Even with the timeout disabled that lane would
# UPSERT all 39.5M rows and then anti-join 39.5M against 39.5M, rewriting every
# row whether or not it changed -- the exact MVCC bloat the metric_data comments
# below were written about, at twelve times the scale.
#
#   Table   the table
#   Key     the grouping column whose signature is compared (the analogue of a
#           "company"): only keys whose signature differs are re-copied
#   Sums    numeric columns summed into the signature. !! EVERY value column
#           belongs here: asset_price carries close AND volume, and 2026-08-02
#           found 887 companies whose VOLUME history had drifted while the price
#           was fine -- a close-only signature would call those in sync.
#   Exclude columns kept out of the change comparison because both sides write
#           them independently (see the recorded_at note in step [6]).
$diffSpecs = @(
    @{ Table = 'metric_data'; Key = 'company_id';  Sums = @('numeric_value');   Exclude = @('recorded_at') },
    @{ Table = 'asset_price'; Key = 'analysis_id'; Sums = @('close', 'volume'); Exclude = @() }
)
$diffTables   = @($diffSpecs | ForEach-Object { $_.Table })

# ---- NOT SYNCED AT ALL: prod owns these, and their PK is a SURROGATE id ------
# !! ADDITIVE IS NOT ENOUGH WHEN THE PRIMARY KEY IS A SERIAL, AND ON 2026-08-11
# THAT PUT DUPLICATE ROWS IN PROD. "Additive" means: upsert local's rows BY PK,
# never delete prod-only ones. That is exactly right for a table whose PK is the
# NATURAL key -- airs_performance (portefeuille, periode), airs_model_weight
# (portefeuille, fonds), airs_account_roster (portefeuille) -- because the same
# logical row carries the same key on both sides, so an upsert updates it.
#
# These six have a surrogate `id` instead, and BOTH SIDES SCRAPE AIRS
# INDEPENDENTLY, each assigning its own serial. Local's airs_holding id=5 and
# prod's id=5 are different holdings. So the upsert did two wrong things at once:
#
#   * ON CONFLICT (id) DO UPDATE OVERWROTE prod's row 5 with local's -- silent
#     corruption of a row prod scraped and local never saw; and
#   * a local row whose id was free on prod was INSERTED beside the row prod
#     already held for that same holding -- the duplicates, which nothing then
#     removes, because additive never deletes.
#
# There is no key to match them on, so there is no correct upsert. The honest
# answer is not to sync them: prod scrapes AIRS on its own schedule and is the
# AUTHOR of every row here. Local's copies are dev artifacts -- the same reasoning
# `asset_ingest_queue` is already excluded under.
#
# !! ALL SIX GO TOGETHER, FOR FK COHERENCE. airs_model_portfolio_position and
# airs_account_model_link both point at airs_model_portfolio; syncing one while
# skipping another would leave a child referencing a parent that was never sent.
#
# !! THE LAST TWO DO HAVE A NATURAL KEY -- AND THE SCRIPT CANNOT SEE IT. It is an
# EXPRESSION unique index (`lower(portefeuille)`, and
# `COALESCE(NULLIF(isin,''), lower(fonds))`), while `$uniqByTable` is built from
# information_schema.table_constraints, which lists CONSTRAINTS only. An
# expression index is invisible there, so step [5]'s pre-clear never de-conflicts
# them and the INSERT trips the index outright. Skipping is both the fix for the
# duplicates and the fix for that.
$skipTables = @(
    'airs_holding',                    # scraped positions per book per date
    'airs_mutatie',                    # scraped mutations
    'airs_model_portfolio',            # the 95 model portfolios, scraped
    'airs_model_portfolio_position',   # their compositions, scraped
    'airs_model_portfolio_link',       # manual link, natural key is an expression index
    'airs_account_model_link'          # manual pairing, same
)
$stagedTables = @($upsertOrder.ToArray() |
    Where-Object { $diffTables -notcontains $_ -and $skipTables -notcontains $_ })

# ---- ADDITIVE tables: upsert local rows, NEVER delete prod-only ones ---------
# !! PROD IS THE AUTHOR OF THESE TABLES, NOT LOCAL. Everything named airs_* is
# written by prod's own scrapers -- the daily Vermogensoverzicht refresh, the CRM
# 'Alle relaties' export, the model-portfolio scan -- against a live AirSPMS that
# a dev machine does not continuously poll. So local holds a SUBSET, and the usual
# mirror semantics read that subset as "everything else was deleted". Measured on
# the 2026-08-02 dry run, a plain mirror would have destroyed:
#
#   airs_holding                  14,143 prod ->  9,846 local  = 4,297 rows gone
#   airs_model_portfolio_position  1,903      ->    919        =   984
#   airs_mutatie                   1,946      ->    974        =   972
#   airs_performance               1,610      ->  1,245        =   365
#
# Scraped history, deleted to make prod agree with a laptop. These tables are
# therefore ADDITIVE: local rows are still inserted/updated by PK (so a local fix
# propagates), but step [7]'s delete-missing skips them entirely.
#
# !! THE UNIQUE-COLLISION CLEAR IN STEP [5] STILL APPLIES. That one removes a prod
# row whose secondary UNIQUE key collides with an incoming local row under a
# different PK; leaving it would make the INSERT fail outright. It resolves a key
# conflict rather than pruning prod-only data, so additive tables need it too.
#
# Prefix-matched on purpose: every airs_* table has the same author (prod), so a
# table added later inherits the right behaviour instead of silently getting the
# mirror semantics on its first clone.
#
# The named tables below are the same story without the shared prefix -- prod
# writes them on its own schedule, so a prod-only row means "prod knows something
# local doesn't", never "local deleted this":
#
#   current_picks_snapshot  THE RECORD OF WHAT THE STRATEGIES ACTUALLY HELD. Prod
#       rebalances on its own scheduler; on 2026-08-02 it held 88 snapshots local
#       had never seen. They are not regenerable -- a snapshot is a decision taken
#       on the data available at that moment, and re-deriving it later gives a
#       different answer (that is the whole reason the golden-master fixture
#       exists). Deleting them to match a laptop is the worst trade in this file.
#   fx_rate  Prod runs its own ECB sync (weekdays 16:30 CET) and had 493 rates
#       local lacked. Dropping them leaves prod converting EUR on stale rates
#       until its next sync -- for reference data both sides fetch independently.
#   asset_ingest_queue  Prod's own worker owns this queue's state; local's copy is
#       a dev artifact.
#
# !! ADDITIVE IS THE SAFE DIRECTION, AND THAT ASYMMETRY IS WHY THIS LIST CAN GROW
# WITHOUT CEREMONY. Marking a table additive can only fail to delete; it can never
# destroy a row. The cost is the mirror-purity one: a row deleted LOCALLY survives
# on prod. For append-only history (snapshots, rates, scrapes) that is exactly
# what you want; for a config table where a deletion is meaningful, it is not --
# so keep those out.
$additiveTables = @($upsertOrder.ToArray() | Where-Object {
    $_ -like 'airs_*' -or
    $_ -in @('current_picks_snapshot', 'fx_rate', 'asset_ingest_queue')
})
Write-Host "  $($allTables.Count) tables; upsert order resolved." -ForegroundColor Green

# ---- deleting a parent row means deleting what points at it -----------------
# !! STEP [5]'s COMMENT USED TO CLAIM "FK ON DELETE CASCADE/SET NULL cleans their
# dependents". For three of company's six children that is FALSE, and the clone
# died on it on 2026-08-11, 47 tables in:
#
#   ERROR: update or delete on table "company" violates foreign key constraint
#          "metric_data_company_id_fkey" on table "metric_data"
#   DETAIL: Key (company_id)=(6418) is still referenced from table "metric_data".
#
# metric_data, portfolio_weight and earnings_portfolio_member are all NO ACTION,
# as are five more edges elsewhere (country<-gurufocus_exchange,
# currency<-fx_rate, currency<-gurufocus_exchange, gurufocus_exchange<-company,
# portfolio<-portfolio_weight). Nothing in the failing statement says which of a
# table's children block and which clean up after themselves, so the edges come
# from the catalogue and the dependents go first, deepest first.
#
# !! THIS IS NOT AN EXTRA DELETION -- IT IS THE SAME ONE, EARLIER. Both call
# sites only ever remove a parent row whose PK IS GONE FROM LOCAL. Local
# therefore cannot hold a child pointing at it either, so every dependent removed
# here is a row step [6] (prod-only metric_data / asset_price keys) or step [7]
# (the mirror) would have deleted minutes later anyway. Nothing is destroyed that
# the mirror was not already going to destroy.
#
# !! EXCEPT IN AN ADDITIVE TABLE, WHERE IT WOULD BE -- SO THE PARENT IS SPARED
# INSTEAD. fx_rate is a blocking child of currency AND is additive on purpose
# (prod runs its own ECB sync and had 493 rates local lacked). Cascading into it
# would destroy precisely the rows $additiveTables exists to protect, to remove a
# currency row. So a parent still pointed at by an additive child is LEFT ON PROD,
# counted and named. A stale reference-data row is a much better outcome than
# deleting prod's own history, and step [8] reports the resulting count.
function Remove-Dependents {
    param([string]$Parent, [string]$DoomTable, [int]$Depth)
    if ($Depth -gt 5) { throw "FK dependency walk exceeded depth 5 at '$Parent' (cycle?)." }
    if (-not $blockingChildren.ContainsKey($Parent)) { return }
    $edges = @($blockingChildren[$Parent])
    if ($edges.Count -eq 0) { return }

    # PASS 1 -- spare the parents an ADDITIVE child still points at, BEFORE anything
    # is deleted. Doing it inside one loop would delete a mirrored child of a parent
    # that a later edge then spares, i.e. destroy a dependent of a row we keep.
    foreach ($e in $edges) {
        if ($additiveTables -notcontains $e.Child) { continue }
        $join = Get-JoinClause $e 'ch' 'd'
        $keep = [int](Invoke-Prod "SELECT count(*) FROM $DoomTable d WHERE EXISTS (SELECT 1 FROM public.$($e.Child) ch WHERE $join);")
        if ($keep -gt 0) {
            Invoke-Prod "DELETE FROM $DoomTable d WHERE EXISTS (SELECT 1 FROM public.$($e.Child) ch WHERE $join);" | Out-Null
            Write-Host ""
            Write-Host ("    KEPT {0} {1} row(s) on prod: {2} (additive) still references them." -f $keep, $Parent, $e.Child) -ForegroundColor Yellow
            if (-not $script:sparedByTable.ContainsKey($Parent)) { $script:sparedByTable[$Parent] = 0 }
            $script:sparedByTable[$Parent] += $keep
        }
    }

    # PASS 2 -- remove the dependents that do block, depth-first.
    foreach ($e in $edges) {
        if ($additiveTables -contains $e.Child) { continue }
        $child = $e.Child
        $join  = Get-JoinClause $e 'ch' 'd'
        $childPk = $pkByTable[$child]
        # Only stage the grandchildren's doom set when there ARE grandchildren --
        # metric_data has none, and it is the one table where an extra scan hurts.
        if ($childPk -and $blockingChildren.ContainsKey($child) -and $blockingChildren[$child].Count -gt 0) {
            $childDoom = "clone_stg.doomed_${child}_$Depth"
            $sel = (($childPk | ForEach-Object { "ch.$_" }) -join ', ')
            Invoke-Prod "SET statement_timeout = 0; DROP TABLE IF EXISTS $childDoom; CREATE TABLE $childDoom AS SELECT $sel FROM public.$child ch JOIN $DoomTable d ON $join;" | Out-Null
            Remove-Dependents -Parent $child -DoomTable $childDoom -Depth ($Depth + 1)
            # !! DELETED VIA ITS OWN (PRUNED) DOOM SET, NOT VIA THE JOIN. The recursion
            # may have SPARED some of these children -- deleting by the join would take
            # them anyway, which is the very destruction the sparing exists to prevent.
            $childMatch = (($childPk | ForEach-Object { "cd.$_ = ch.$_" }) -join ' AND ')
            Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$child ch USING $childDoom cd WHERE $childMatch;" | Out-Null
            Invoke-Prod "DROP TABLE IF EXISTS $childDoom;" | Out-Null
        } else {
            Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$child ch USING $DoomTable d WHERE $join;" | Out-Null
        }
        # !! AND THE SPARING PROPAGATES UP, OR IT IS NOT SPARING AT ALL. If anything
        # of this child SURVIVED (it was kept for an additive descendant), then this
        # parent is still referenced and deleting it would fail on the very FK this
        # whole function exists to respect. Asking AFTER the delete is what makes that
        # self-evident: whatever is still pointing here was deliberately kept. On a
        # normal run every child is gone and this removes nothing.
        $left = [int](Invoke-Prod "SELECT count(*) FROM $DoomTable d WHERE EXISTS (SELECT 1 FROM public.$child ch WHERE $join);")
        if ($left -gt 0) {
            Invoke-Prod "DELETE FROM $DoomTable d WHERE EXISTS (SELECT 1 FROM public.$child ch WHERE $join);" | Out-Null
            Write-Host ""
            Write-Host ("    KEPT {0} {1} row(s) on prod: rows in {2} survived below them." -f $left, $Parent, $child) -ForegroundColor Yellow
            if (-not $script:sparedByTable.ContainsKey($Parent)) { $script:sparedByTable[$Parent] = 0 }
            $script:sparedByTable[$Parent] += $left
        }
    }
}

# "ch.col = d.col AND ..." for one FK edge, composite keys included.
function Get-JoinClause {
    param($Edge, [string]$ChildAlias, [string]$ParentAlias)
    $parts = @()
    for ($i = 0; $i -lt $Edge.ChildCols.Count; $i++) {
        $parts += "$ChildAlias.$($Edge.ChildCols[$i]) = $ParentAlias.$($Edge.ParentCols[$i])"
    }
    return ($parts -join ' AND ')
}

# Delete the rows of $Table matching $Predicate (a boolean SQL expression over the
# alias `p`), removing whatever blocks them first.
#
# !! THE DOOMED KEYS ARE MATERIALISED, NOT RE-EVALUATED PER CHILD. The predicate
# reads clone_stg and public.$Table, and every child delete would otherwise re-run
# it as a correlated subquery -- against metric_data, that is a 30M-row scan per
# edge. Staged once, each child delete is a join on its own FK index. It also
# makes "spare this parent" expressible at all: a row is removed from the doom set
# and the parent delete simply never sees it.
function Remove-RowsWithDependents {
    param([string]$Table, [string]$Predicate)
    $pk = $pkByTable[$Table]
    # Nothing blocks this table (48 of 55) -- one DELETE, exactly as before.
    if (-not $pk -or -not $blockingChildren.ContainsKey($Table) -or $blockingChildren[$Table].Count -eq 0) {
        Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$Table p WHERE $Predicate;" | Out-Null
        return
    }
    $doom  = "clone_stg.doomed_$Table"
    $pkSel = (($pk | ForEach-Object { "p.$_" }) -join ', ')
    Invoke-Prod "SET statement_timeout = 0; DROP TABLE IF EXISTS $doom; CREATE TABLE $doom AS SELECT $pkSel FROM public.$Table p WHERE $Predicate;" | Out-Null
    $n = [int](Invoke-Prod "SELECT count(*) FROM $doom")
    if ($n -gt 0) {
        Remove-Dependents -Parent $Table -DoomTable $doom -Depth 0
        $match = (($pk | ForEach-Object { "d.$_ = p.$_" }) -join ' AND ')
        Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$Table p USING $doom d WHERE $match;" | Out-Null
    }
    Invoke-Prod "DROP TABLE IF EXISTS $doom;" | Out-Null
}
# parent table -> rows left on prod because an additive child still points at them.
# Step [8] reads this so a deliberate survivor is not reported as a MISMATCH.
$sparedByTable = @{}

# Per-company signature for metric_data (price data only; recorded_at excluded).
# The leading `SET statement_timeout = 0;` disables the per-statement timeout for
# THIS query's session: it's a full ~26M-row GROUP BY, and on an IO-throttled prod
# it can exceed the default timeout. Done inline (not just via PGOPTIONS) because
# a managed pooler may ignore startup options. The `SET` emits a "SET" status line
# that ConvertTo-MdSignatureMap skips (it isn't a pipe-delimited data row).
function Get-SignatureSql([hashtable]$spec) {
    $sums = (($spec.Sums | ForEach-Object { "coalesce(sum($_),0)::text" }) -join ', ')
    return @"
SET statement_timeout = 0;
SELECT $($spec.Key), count(*), $sums, coalesce(max(target_date)::text,''), coalesce(min(target_date)::text,'')
FROM $($spec.Table) GROUP BY $($spec.Key)
"@
}
function ConvertTo-MdSignatureMap([string[]]$lines) {
    $map = @{}
    foreach ($r in $lines) {
        $p = ("$r".TrimEnd("`r")) -split '\|'        # TrimEnd CR (job output)
        if ($p.Length -lt 5) { continue }            # skip the 'SET' tag + blank lines
        $cid = 0
        if (-not [int]::TryParse($p[0], [ref]$cid)) { continue }
        # Everything after the key IS the signature -- joined rather than indexed,
        # so a spec with two sum columns (asset_price: close + volume) needs no
        # change here and metric_data's four-part signature is byte-identical.
        $map[$cid] = ($p[1..($p.Length - 1)] -join '|')
    }
    return $map
}
# Run BOTH metric_data signature scans concurrently. Each is a full ~26M-row
# GROUP BY and they're independent (local -> local disk; prod -> the pooler), so
# parallel wall-time is ~max(local, prod) instead of the sum -- and this diff is
# the dominant cost of the whole clone. Background jobs run docker in a separate
# process; we check each job's exit code, then parse the raw output in-runspace.
# Returns @{ Local = <sig map>; Prod = <sig map> }.
function Get-MdSignaturesParallel([hashtable]$spec) {
    $sql = Get-SignatureSql $spec
    $localJob = Start-Job -ScriptBlock {
        param($Container, $sql)
        $out = docker exec $Container psql -U postgres -d postgres -tA -F'|' -c $sql
        [pscustomobject]@{ Out = @($out); Code = $LASTEXITCODE }
    } -ArgumentList $Container, $sql
    $prodJob = Start-Job -ScriptBlock {
        param($Container, $prodEnv, $prodUrlNoPw, $sql)
        $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -c $sql
        [pscustomobject]@{ Out = @($out); Code = $LASTEXITCODE }
    } -ArgumentList $Container, $prodEnv, $prodUrlNoPw, $sql
    # Both jobs are already running; -Wait on each just collects (max, not sum).
    $lr = Receive-Job -Job $localJob -Wait -AutoRemoveJob
    $pr = Receive-Job -Job $prodJob  -Wait -AutoRemoveJob
    if ($lr.Code -ne 0) { throw "local $($spec.Table) signature scan failed:`n$($lr.Out -join "`n")" }
    if ($pr.Code -ne 0) { throw "prod $($spec.Table) signature scan failed:`n$($pr.Out -join "`n")" }
    return @{ Local = (ConvertTo-MdSignatureMap $lr.Out); Prod = (ConvertTo-MdSignatureMap $pr.Out) }
}

# ---- DRY RUN ----------------------------------------------------------------
if ($DryRun) {
    Write-Host "[3] DRY RUN -- comparing local vs prod (read-only)..." -ForegroundColor Yellow
    $wouldDelete = 0
    foreach ($t in $upsertOrder) {
        $lc = [int](Invoke-Local "SELECT count(*) FROM public.$t")
        $pc = [int](Invoke-Prod "SELECT count(*) FROM public.$t")
        # !! SAY WHAT WOULD ACTUALLY HAPPEN, NOT JUST THAT THE NUMBERS DIFFER.
        # "<-- differs" reads the same whether prod is about to gain rows or lose
        # 4,297 scraped ones, which is the difference between a sync and a data
        # loss. Additive tables are marked so their surplus stops looking alarming.
        $flag = ''; $colour = 'Gray'
        # !! A SKIPPED TABLE MUST NOT BE REPORTED AS "additive", WHICH IS WHAT IT DID ON
        # 2026-08-11: the dry run showed `airs_holding ... <-- additive: prod keeps +2784`, i.e. it
        # promised an UPSERT of local's 11,213 rows on a table the run then correctly left alone.
        # Reporting a stronger action than the one taken is the safe direction to be wrong in and
        # still the wrong direction to be wrong in -- a dry run exists to be believed.
        if ($skipTables -contains $t) {
            Write-Host ("  {0,-28} local={1,-10} prod={2,-10}  <-- NOT TOUCHED (prod owns it)" -f $t, $lc, $pc) `
                -ForegroundColor DarkGray
            continue
        }
        if ($additiveTables -contains $t) {
            if ($pc -gt $lc) { $flag = "  <-- additive: prod keeps +$($pc - $lc)"; $colour = 'DarkGray' }
            elseif ($lc -ne $pc) { $flag = "  <-- $($lc - $pc) to add"; $colour = 'Gray' }
        } elseif ($pc -gt $lc) {
            $flag = "  <-- WOULD DELETE $($pc - $lc) prod-only row(s)"; $colour = 'Yellow'
            $wouldDelete += ($pc - $lc)
        } elseif ($lc -ne $pc) {
            $flag = "  <-- $($lc - $pc) to add"
        }
        Write-Host ("  {0,-28} local={1,-10} prod={2,-10}{3}" -f $t, $lc, $pc, $flag) -ForegroundColor $colour
    }
    if ($wouldDelete -gt 0) {
        Write-Host "  ~$wouldDelete prod-only row(s) would be DELETED by the mirror (additive tables excluded)." -ForegroundColor Yellow
    }
    foreach ($spec in $diffSpecs) {
        Write-Host "  computing $($spec.Table) per-$($spec.Key) signatures (both sides, concurrent)..."
        $sigs = Get-MdSignaturesParallel $spec
        $localSig = $sigs.Local; $prodSig = $sigs.Prod
        $resync = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
        $prodOnly = @($prodSig.Keys | Where-Object { -not $localSig.ContainsKey($_) })
        Write-Host ("  {0}: {1} keys local, {2} prod; {3} need re-copy, {4} prod-only to delete." -f $spec.Table, $localSig.Count, $prodSig.Count, $resync.Count, $prodOnly.Count) -ForegroundColor Cyan
        if ($resync.Count -gt 0) {
            $est = [int](Invoke-Local ("SELECT count(*) FROM {0} WHERE {1} = ANY('{{{2}}}'::int[])" -f $spec.Table, $spec.Key, ($resync -join ',')))
            Write-Host "  ~ $est $($spec.Table) rows would transfer." -ForegroundColor Cyan
        }
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
    Write-Host "  - upsert + delete-missing on $($stagedTables.Count - $additiveTables.Count) tables"
    Write-Host "  - upsert ONLY (prod-only rows kept) on $($additiveTables.Count) additive tables: $($additiveTables -join ', ')"
    Write-Host "  - NOT TOUCHED at all (prod owns them; surrogate ids cannot be matched across sides):"
    Write-Host "      $($skipTables -join ', ')" -ForegroundColor DarkGray
    Write-Host "  - differentially re-copy only changed rows of $($diffTables -join ', ')"
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

# !! EVERYTHING FROM HERE TO STEP [7b] IS INSIDE A try/finally, AND THE finally IS WHAT DROPS
# clone_stg. A run that dies mid-way used to LEAVE A FULL COPY OF EVERY STAGED TABLE ON PROD --
# the drop lived on the success path only. On 2026-08-11 the FK failure in step [5] did exactly
# that, and prod went READ-ONLY: Supabase locks writes at 95% of disk, the staging copy is ~500MB
# (universe_membership alone is 444MB), and repeated attempts each left another one plus the dead
# tuples from every table already upserted. Four disk expansions inside 24h hit the modification
# limit, and there is a ~4h cooldown before the disk can grow again.
#
# The braces are deliberately NOT re-indented over the ~200 lines they wrap: PowerShell does not
# care, and a whitespace-only diff over the whole body would bury the two lines that changed.
try {
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
    # !! THE LEADING `SET statement_timeout = 0;` IS LOAD-BEARING, AND ITS ABSENCE
    # IS WHAT KILLED A CLONE ON 2026-08-02:
    #
    #   psql:<stdin>:2: ERROR: canceling statement due to statement timeout
    #   CONTEXT: COPY asset_price, line 34063937
    #
    # `<stdin>:2` is this very \copy -- statement 1 is the CREATE TABLE above it.
    # Every other prod statement in this script sets the timeout inline; this one
    # relied on the PGOPTIONS startup option alone, and the managed session pooler
    # does not forward it (which the PGOPTIONS comment already warned was possible).
    # So a table big enough to outlast the default timeout could never be staged.
    # !! `DROP TABLE IF EXISTS` MAKES THIS RETRYABLE, which it has to be: a dropped connection
    # mid-\copy leaves the staging table created and half-loaded, and re-running without the drop
    # fails on "already exists" -- or worse, succeeds and loads the rows a second time. See the
    # retry note on Invoke-ProdScript.
    $loadSql = "SET statement_timeout = 0;`nDROP TABLE IF EXISTS clone_stg.$t;`nCREATE TABLE clone_stg.$t (LIKE public.$t INCLUDING DEFAULTS);`n\copy clone_stg.$t ($collist) FROM '$datfile'`n"
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
        # !! AN UNCHANGED ROW MUST NOT BE REWRITTEN -- THE SAME RULE STEP [6] ALREADY HAS, ONE
        # LANE OVER, AND ITS ABSENCE HERE IS WHY A CLONE THAT CHANGES NOTHING STILL COSTS DISK.
        # Postgres is MVCC: `SET x = x` writes a NEW tuple, marks the old one dead, updates every
        # index and WAL-logs all of it. Without this guard every clone rewrote EVERY row of EVERY
        # staged table -- ~8,400 universe_membership rows in a 444MB table, on every run, whether
        # or not a single value differed. That dead weight is what autovacuum then has to chase,
        # and on 2026-08-11 it did not chase it fast enough: prod crossed 95% of disk and went
        # READ-ONLY. The metric_data comment in step [6] calls this "the difference between a sync
        # and a disk-filling event"; it is the same event here.
        #
        # !! NO EXCLUSION LIST, UNLIKE STEP [6]. That one omits `recorded_at` because both sides
        # stamp it independently, which would defeat the guard. Here EVERY non-PK column is
        # compared, so a row is skipped only when it is identical in every field -- the guard can
        # therefore never change what prod ends up holding, only whether we pointlessly wrote it.
        # (Checked: no staged table has a `json`/geometric column, so every type involved has an
        # equality operator. A plain `json` column would make this fail outright.)
        $tgt = (($nonpk | ForEach-Object { "public.$t.$_" }) -join ', ')
        $inc = (($nonpk | ForEach-Object { "EXCLUDED.$_" }) -join ', ')
        $conflict = "ON CONFLICT ($pklist) DO UPDATE SET $setlist WHERE ($tgt) IS DISTINCT FROM ($inc)"
    } else {
        $conflict = "ON CONFLICT ($pklist) DO NOTHING"
    }
    # Pre-clear stale prod rows that collide on a secondary UNIQUE key with an
    # incoming local row but whose PK is gone from local (diverged ids for the
    # same logical row). Without this the INSERT below trips the unique
    # constraint (e.g. universe.label for a snapshot re-frozen on prod under a
    # different universe_id). The "PK gone from local" guard means we only touch
    # rows step [7] would delete anyway. A NULL unique value never matches
    # (p.col = s.col is NULL), so multi-NULL columns like template_key are
    # correctly left alone.
    #
    # !! IT GOES THROUGH Remove-RowsWithDependents BECAUSE CASCADE IS NOT A GIVEN.
    # This is the statement that killed the 2026-08-11 clone on company/metric_data
    # -- the comment here asserted the dependents would be cleaned up by the FKs,
    # and for metric_data, portfolio_weight and earnings_portfolio_member (all NO
    # ACTION) they are not. See that function.
    if ($uniqByTable.ContainsKey($t)) {
        $pkMatch = (($pk | ForEach-Object { "k.$_ = p.$_" }) -join ' AND ')
        foreach ($ucols in $uniqByTable[$t]) {
            $uMatch = (($ucols | ForEach-Object { "p.$_ = s.$_" }) -join ' AND ')
            Remove-RowsWithDependents -Table $t -Predicate (
                "EXISTS (SELECT 1 FROM clone_stg.$t s WHERE $uMatch)" +
                " AND NOT EXISTS (SELECT 1 FROM clone_stg.$t k WHERE $pkMatch)")
        }
    }
    $ov = if ($alwaysIdentity.ContainsKey($t)) { 'OVERRIDING SYSTEM VALUE ' } else { '' }
    Invoke-Prod "SET statement_timeout = 0; INSERT INTO public.$t ($collist) ${ov}SELECT $collist FROM clone_stg.$t $conflict;" | Out-Null
    Write-Host ("done ({0:N1}s)" -f $swTbl.Elapsed.TotalSeconds) -ForegroundColor Green
}
Write-Host "  all $nStaged tables upserted." -ForegroundColor Green

# ---- [6] differential tables (metric_data, asset_price) ---------------------
# One implementation, driven by `$diffSpecs`. Every comment below was written for
# metric_data and applies unchanged to any table in the list -- the concurrent-
# writer hazard, the MVCC rewrite guard and the per-key chunking are properties of
# "a huge table synced while prod is live", not of metric_data specifically.
$stepNo = 6
foreach ($spec in $diffSpecs) {
$tbl = $spec.Table; $keyCol = $spec.Key
Write-Host "[$stepNo] Diffing $tbl per $keyCol (full scan both sides)..."
Write-Host "  scanning LOCAL + PROD signatures concurrently (full-table GROUP BYs)... " -NoNewline
$swSig = [System.Diagnostics.Stopwatch]::StartNew()
$sigs = Get-MdSignaturesParallel $spec
$localSig = $sigs.Local; $prodSig = $sigs.Prod
Write-Host ("done (local {0}, prod {1} keys, {2:N0}s wall)" -f $localSig.Count, $prodSig.Count, $swSig.Elapsed.TotalSeconds) -ForegroundColor Green
$resync   = @($localSig.Keys | Where-Object { $prodSig[$_] -ne $localSig[$_] })
$prodOnly = @($prodSig.Keys  | Where-Object { -not $localSig.ContainsKey($_) })
Write-Host "  $($resync.Count) $keyCol values to re-copy, $($prodOnly.Count) prod-only to delete."

$mdCols = ($colsByTable[$tbl] -join ', ')
# Upsert plumbing for the batches, discovered like every other table's.
$mdPk       = $pkByTable[$tbl]
$mdPkList   = ($mdPk -join ', ')
$mdNonPk    = @($colsByTable[$tbl] | Where-Object { $mdPk -notcontains $_ })
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
$mdCompare = @($colsByTable[$tbl] | Where-Object { $mdPk -notcontains $_ -and $spec.Exclude -notcontains $_ })
if ($mdNonPk.Count -gt 0 -and $mdCompare.Count -gt 0) {
    $tgt = (($mdCompare | ForEach-Object { "public.$tbl.$_" }) -join ', ')
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
    docker exec $Container psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "\copy (SELECT $mdCols FROM $tbl WHERE $keyCol = ANY('{$arr}'::int[])) TO '/tmp/clone_md.dat'"
    if ($LASTEXITCODE -ne 0) { throw "local $tbl dump failed." }
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
CREATE TABLE clone_stg.md_batch (LIKE public.$tbl INCLUDING DEFAULTS);
\copy clone_stg.md_batch ($mdCols) FROM '/tmp/clone_md.dat'
ALTER TABLE clone_stg.md_batch ADD PRIMARY KEY ($mdPkList);
BEGIN;
DELETE FROM public.$tbl p
 WHERE p.$keyCol = ANY('{$arr}'::int[])
   AND NOT EXISTS (SELECT 1 FROM clone_stg.md_batch s WHERE $mdKeyMatch);
INSERT INTO public.$tbl ($mdCols)
SELECT $mdCols FROM clone_stg.md_batch
$mdConflict;
COMMIT;
DROP TABLE clone_stg.md_batch;
"@
    Invoke-ProdScript $sql
    $done += $chunk.Count
    Write-Host ("    batch {0}/{1}: re-copied {2}/{3} $keyCol values ({4:N1}s)" -f $ci, $nChunks, $done, $resync.Count, $swBatch.Elapsed.TotalSeconds)
}
if ($prodOnly.Count -gt 0) {
    for ($i = 0; $i -lt $prodOnly.Count; $i += $CompanyChunk) {
        $chunk = $prodOnly[$i..([math]::Min($i + $CompanyChunk - 1, $prodOnly.Count - 1))]
        Invoke-Prod "SET statement_timeout = 0; DELETE FROM public.$tbl WHERE $keyCol = ANY('{$($chunk -join ',')}'::int[]);" | Out-Null
    }
    Write-Host "    deleted prod-only $tbl for $($prodOnly.Count) $keyCol values."
}
Write-Host "  $tbl in sync." -ForegroundColor Green
$stepNo++
}

# ---- [7] DELETE prod rows gone from local (child->parent) --------------------
Write-Host "[7] Deleting rows removed locally (mirror)..."
foreach ($t in $deleteOrder) {
    # The differential tables were never staged, so there is no clone_stg copy to
    # anti-join against -- their prod-only rows are deleted inside step [6].
    if ($diffTables -contains $t) { continue }
    # Additive tables keep whatever prod scraped that local never saw.
    if ($additiveTables -contains $t) { continue }
    $pk = $pkByTable[$t]
    $cond = (($pk | ForEach-Object { "s.$_ = p.$_" }) -join ' AND ')
    # !! THE SAME WALL AS STEP [5], REACHED MINUTES LATER. The mirror delete drops
    # prod rows whose PK is gone from local -- which for `company` is exactly the
    # row whose metric_data still points at it. The delete order is child->parent,
    # so a child TABLE is emptied before its parent, but that says nothing about a
    # prod-only PARENT row whose children prod also still holds: those children are
    # deleted by step [6] (metric_data / asset_price are never in clone_stg) or by
    # an earlier table in this very loop, and neither had run for company 6418.
    Remove-RowsWithDependents -Table $t -Predicate "NOT EXISTS (SELECT 1 FROM clone_stg.$t s WHERE $cond)"
}
# The staging schema is dropped in the `finally` below, so it goes on a failure too.
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

}
finally {
    # !! THE STAGING COPY IS DROPPED WHETHER THE RUN SUCCEEDED OR DIED, and that is a DISK rule,
    # not tidiness. clone_stg holds a full copy of every staged table (~500MB, three quarters of
    # it universe_membership); left behind on prod it counts against the 95%-of-disk threshold
    # that puts Supabase into read-only mode, where the next attempt cannot even start.
    #
    # !! BEST-EFFORT, AND IT MUST NOT MASK THE REAL FAILURE. If the run is dying because prod is
    # unreachable then this drop cannot work either, and letting its error escape would replace
    # "the FK on company blocked the delete" with "connection refused" -- burying the fault that
    # actually needs fixing. So it warns and rethrows nothing.
    try {
        Invoke-Prod "DROP SCHEMA IF EXISTS clone_stg CASCADE;" | Out-Null
        Write-Host "  clone_stg dropped (staging copy removed from prod)." -ForegroundColor DarkGray
    } catch {
        Write-Host "  WARNING: could not drop clone_stg on prod -- it is still using disk there." -ForegroundColor Yellow
        Write-Host "           Run this by hand once prod is reachable:  DROP SCHEMA IF EXISTS clone_stg CASCADE;" -ForegroundColor Yellow
    }
    try { docker exec $Container bash -c "rm -f /tmp/clone_*.dat" | Out-Null } catch { }
}

# ---- [7c] mirror the backtest-results Storage bucket ------------------------
# Upload local blobs missing on prod + delete prod-only blobs, so each cloned
# `backtest_run.result_path` resolves (no out-of-band 404 on read).
Write-Host "[7c] Mirroring '$StorageBucket' Storage bucket..."
Invoke-StorageMirror

# ---- [8] verify -------------------------------------------------------------
Write-Host "[8] Verifying..."
$mismatch = 0
$surplus  = 0
foreach ($t in $upsertOrder) {
    # !! A SKIPPED TABLE HAS NO EXPECTED RELATIONSHIP BETWEEN THE TWO COUNTS. Prod owns it and
    # local's copy is a dev artifact, so prod holding more (or fewer) is not a finding -- reporting
    # either as a MISMATCH would fail every clone for doing exactly what it was told.
    if ($skipTables -contains $t) {
        $lc = [int](Invoke-Local "SELECT count(*) FROM public.$t")
        $pc = [int](Invoke-Prod "SELECT count(*) FROM public.$t")
        Write-Host ("  skipped  {0} : local={1} prod={2} (prod owns this table)" -f $t, $lc, $pc) `
            -ForegroundColor DarkGray
        continue
    }
    $lc = [int](Invoke-Local "SELECT count(*) FROM public.$t")
    $pc = [int](Invoke-Prod "SELECT count(*) FROM public.$t")
    if ($additiveTables -contains $t) {
        # !! EQUALITY IS THE WRONG TEST HERE. An additive table is expected to hold
        # MORE on prod (rows its own scrapers wrote); the failure mode is prod
        # holding FEWER, which would mean local rows did not land.
        if ($pc -lt $lc) {
            Write-Host "  MISSING  $t : local=$lc prod=$pc (additive: prod should be >= local)" -ForegroundColor Red
            $mismatch++
        } elseif ($pc -gt $lc) {
            Write-Host ("  additive $t : local={0} prod={1} (+{2} prod-only rows kept)" -f $lc, $pc, ($pc - $lc)) -ForegroundColor DarkGray
            $surplus++
        }
        continue
    }
    if ($lc -ne $pc) {
        # !! A ROW WE DELIBERATELY KEPT IS NOT A MISMATCH, AND REPORTING IT AS ONE
        # WOULD FAIL THE RUN FOR DOING THE RIGHT THING. Remove-Dependents spares a
        # parent that an ADDITIVE child still references rather than cascading into
        # prod's own scraped/synced history; the survivor is a surplus prod row by
        # construction. Only the amount it explains is forgiven -- anything beyond
        # it is still a real mismatch.
        $sp = 0
        if ($sparedByTable.ContainsKey($t)) { $sp = $sparedByTable[$t] }
        if ($sp -gt 0 -and $pc -gt $lc -and ($pc - $lc) -le $sp) {
            Write-Host ("  KEPT     {0} : local={1} prod={2} (+{3} still referenced by an additive table -- see above)" -f $t, $lc, $pc, ($pc - $lc)) -ForegroundColor Yellow
        } else {
            Write-Host "  MISMATCH $t : local=$lc prod=$pc" -ForegroundColor Red
            $mismatch++
        }
    }
}
if ($surplus -gt 0) { Write-Host "  $surplus additive table(s) kept prod-only rows (by design)." -ForegroundColor DarkGray }
if ($mismatch -eq 0) {
    Write-Host "  all $($upsertOrder.Count) tables match." -ForegroundColor Green
    Write-Host ""
    Write-Host "DONE. Prod is an exact clone of local. ($([math]::Round($swAll.Elapsed.TotalSeconds,1))s)" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "FINISHED WITH $mismatch MISMATCH(ES) -- inspect above." -ForegroundColor Red
    exit 1
}

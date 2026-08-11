<#
.SYNOPSIS
  Read-only "is prod OK right now, and if not, what is eating it?" - one screen.

.DESCRIPTION
  Written during the 2026-08-11 incident, where three different symptoms
  (401 Unauthorized, httpcore.ReadTimeout, a 500 on /analysis) all had ONE
  cause: the instance was I/O-saturated, so every query took 1-60s at random.

  !! THE POINT OF THIS SCRIPT IS THAT LATENCY IS THE SYMPTOM, NOT THE CAUSE.
  A single timing is useless here because the spread IS the signal: measured
  on the same 1,815-row read, six consecutive calls gave
  60s, 60s, 11.4s, 12.0s, 1.0s, 1.4s. One sample would have told you the
  system was fine, or fatally broken, depending purely on when you looked.
  So it samples repeatedly and prints min/median/max.

  Everything here is a plain SELECT. It starts nothing, cancels nothing and
  writes nothing.

.PARAMETER Samples
  How many latency probes to take (default 5).

.PARAMETER Watch
  Loop until interrupted, re-reporting every -Every seconds. Use this to know
  when a vacuum/reindex has finished instead of reloading the app to find out
  (reloading adds load to the thing you are waiting on).

.PARAMETER Every
  Seconds between passes in -Watch mode (default 60).
#>
[CmdletBinding()]
param(
    [int]$Samples = 5,
    [switch]$Watch,
    [int]$Every = 60,
    [string]$Container = 'supabase_db_bbterminal'
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
$url = $env:PROD_DB_URL
if (-not $url) { Write-Host "ERROR: PROD_DB_URL not set (scripts/.env.local)." -ForegroundColor Red; exit 1 }
if ($url -notmatch '^(?<prefix>postgres(?:ql)?://[^:@/]+):(?<pw>[^@]+)@(?<rest>.+)$') {
    Write-Host "ERROR: PROD_DB_URL is not postgres://user:pass@host/db" -ForegroundColor Red; exit 1
}
$pw = $Matches.pw
$noPw = "$($Matches.prefix)@$($Matches.rest)"
$dockerEnv = @('-e', "PGPASSWORD=$pw")

function Invoke-Q([string]$sql) {
    # -q so psql's command tags never land in a parsed result; the SET stamps the
    # session so this probe is distinguishable from application traffic.
    $out = docker exec @dockerEnv $Container psql $noPw -q -tA -F'|' -c "SET application_name='prod-health'; $sql"
    if ($LASTEXITCODE -ne 0) { return @() }
    return @($out | ForEach-Object { "$_".TrimEnd("`r") } | Where-Object { $_ -ne '' })
}
function Get-Scalar([string]$sql) {
    # !! THE @() HERE IS THE WHOLE FUNCTION, AND OMITTING IT BIT THIS SCRIPT ON ITS
    # FIRST RUN. PowerShell unrolls a one-element array when a function RETURNS it,
    # so even though Invoke-Q ends in `return @(...)`, a single-row result arrives
    # as a bare string -- and `$rows[0]` then indexes the STRING. The first run
    # printed "db 2 + WAL 1 ... read_only=o" and "dead tuples: 4": those are the
    # first CHARACTERS of "27 GB", "1840 MB", "off" and "4,027,178". Every one of
    # them reads like a plausible value, which is what makes this trap expensive
    # (it has now cost this codebase four separate incidents -- see
    # prod-reclaim-disk.ps1's Invoke-ProdScalar and airs-dedupe.ps1's Get-Count).
    # Re-wrapping at the CALL SITE is what forces it back to an array.
    $rows = @(Invoke-Q $sql)
    if (-not $rows.Count) { return '' }
    return ($rows[0] -split '\|')[0]
}

function Show-Pass {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkGray
    Write-Host ("  prod health  " + (Get-Date -Format 'HH:mm:ss')) -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor DarkGray

    # ---- 1. latency spread ---------------------------------------------------
    $times = @()
    foreach ($i in 1..$Samples) {
        $sw = [Diagnostics.Stopwatch]::StartNew()
        Invoke-Q "SELECT count(*) FROM airs_performance;" | Out-Null
        $times += $sw.ElapsedMilliseconds
    }
    # NOTE: `$times += ...` in a loop is genuinely an array; the unroll trap that hits
    # Get-Scalar applies to values RETURNED FROM A FUNCTION, not to locals built here.
    $sorted = $times | Sort-Object
    $min = $sorted[0]; $max = $sorted[-1]; $med = $sorted[[int]($sorted.Count / 2)]
    $colour = if ($max -lt 1000) { 'Green' } elseif ($max -lt 5000) { 'Yellow' } else { 'Red' }
    Write-Host ("  latency  min {0} ms / median {1} ms / max {2} ms   [{3}]" -f `
            $min, $med, $max, ($times -join ', ')) -ForegroundColor $colour
    if ($max -ge 5000) {
        Write-Host "    ^ the app WILL time out intermittently at this spread." -ForegroundColor Red
    }

    # ---- 2. what is consuming the disk --------------------------------------
    # !! @(...) ON EVERY ONE OF THESE. A function returning a one-element array
    # hands back a bare string, and then `$vac + $idx` CONCATENATES TWO STRINGS
    # instead of merging two lists -- printing one run-together line and hiding
    # that two things are running at once. Same unroll as Get-Scalar.
    $vac = @(Invoke-Q @"
-- !! `index_vacuum_count` IS NOT A PROGRESS BAR AND READING IT AS ONE WASTES AN HOUR.
-- It counts COMPLETED PASSES OVER ALL INDEXES, so it sits at 0 for the entire run of a
-- normal vacuum and then jumps straight to 1 at the end. During the 2026-08-11 cleanup it
-- read `idx_pass=0` for 25 minutes while the vacuum was working perfectly, which looks
-- exactly like a hung process. `heap_blks_scanned` is no better once the heap phase is
-- done: it pins at total and stops moving for the whole index phase.
-- `indexes_processed`/`indexes_total` (PG17+) is the one that actually advances.
SELECT 'VACUUM  '||v.relid::regclass||'  phase='||v.phase
       ||'  indexes '||coalesce(v.indexes_processed::text,'?')||'/'||coalesce(v.indexes_total::text,'?')
       ||'  heap '||v.heap_blks_scanned||'/'||v.heap_blks_total
       ||'  running '||to_char(now()-a.query_start,'HH24:MI:SS')
       ||'  wait='||coalesce(a.wait_event_type,'RUNNING')
FROM pg_stat_progress_vacuum v JOIN pg_stat_activity a ON a.pid=v.pid
"@)
    $idx = @(Invoke-Q @"
SELECT 'CREATE INDEX  '||p.index_relid::regclass||'  phase='||p.phase
       ||'  blocks '||p.blocks_done||'/'||p.blocks_total
       ||'  running '||to_char(now()-a.query_start,'HH24:MI:SS')
FROM pg_stat_progress_create_index p JOIN pg_stat_activity a ON a.pid=p.pid
"@)
    if ($vac.Count -or $idx.Count) {
        Write-Host "  heavy maintenance in progress:" -ForegroundColor Yellow
        ($vac + $idx) | ForEach-Object { Write-Host "    $_" -ForegroundColor Yellow }
        # Only blame it when it is actually hurting. Heavy maintenance running while the app
        # is responsive is NORMAL and expected -- saying "this is the cause of the latency"
        # under a healthy 320ms teaches you to skip the line on the day it is true.
        if ($max -ge 2000) {
            Write-Host "    -> this is what is slowing the app down." -ForegroundColor Yellow
        } else {
            Write-Host "    -> running, but the app is responsive. Let it finish." -ForegroundColor Gray
        }
        Write-Host "    -> do NOT start another one until this clears." -ForegroundColor Yellow
    } else {
        Write-Host "  no vacuum/reindex running." -ForegroundColor Green
    }

    # ---- 3. long queries + genuine lock waits -------------------------------
    # A blocked pid is a DIFFERENT problem from a slow one and has a different
    # fix, so they are never merged into one "slow" bucket.
    $long = @(Invoke-Q @"
SELECT '    pid '||pid||'  '||to_char(now()-query_start,'HH24:MI:SS')||'  '
       ||coalesce(wait_event_type,'RUNNING')||'  '||left(regexp_replace(query,'\s+',' ','g'),46)
FROM pg_stat_activity
WHERE state='active' AND datname=current_database() AND now()-query_start > interval '5 seconds'
  AND pid <> pg_backend_pid() AND query NOT LIKE 'autovacuum:%'
ORDER BY query_start
"@)
    if ($long.Count) {
        Write-Host "  queries running > 5s:" -ForegroundColor Yellow
        $long | ForEach-Object { Write-Host $_ -ForegroundColor Yellow }
    }
    $blocked = @(Invoke-Q "SELECT '    pid '||pid||' blocked by '||array_to_string(pg_blocking_pids(pid),',') FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid))>0;")
    if ($blocked.Count) {
        Write-Host "  BLOCKED ON LOCKS (not just slow -- find the blocker):" -ForegroundColor Red
        $blocked | ForEach-Object { Write-Host $_ -ForegroundColor Red }
    }

    # ---- 4. capacity ---------------------------------------------------------
    $ro   = Get-Scalar "SELECT current_setting('default_transaction_read_only');"
    $db   = Get-Scalar "SELECT pg_size_pretty(pg_database_size(current_database()));"
    $wal  = Get-Scalar "SELECT pg_size_pretty(coalesce(sum(size),0)) FROM pg_ls_waldir();"
    $conn = Get-Scalar "SELECT count(*)::text FROM pg_stat_activity WHERE datname=current_database();"
    $maxc = Get-Scalar "SELECT current_setting('max_connections');"
    $dead = Get-Scalar "SELECT to_char(n_dead_tup,'FM999,999,999') FROM pg_stat_user_tables WHERE relname='metric_data';"
    Write-Host ("  db {0}  + WAL {1}   connections {2}/{3}   read_only={4}" -f $db, $wal, $conn, $maxc, $ro) `
            -ForegroundColor $(if ($ro -eq 'on') { 'Red' } else { 'Gray' })
    if ($ro -eq 'on') { Write-Host "    !! READ-ONLY: out of disk. Reclaim space before anything else." -ForegroundColor Red }
    Write-Host ("  metric_data dead tuples: {0}" -f $dead) -ForegroundColor Gray

    # Index bloat is the standing cost behind all of this: a bulk load builds
    # indexes by insertion, and the bloated result is what the next vacuum and
    # every query then has to walk.
    Invoke-Q @"
SELECT '    '||c.relname||'  '||pg_size_pretty(pg_relation_size(c.oid))
FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
WHERE i.indrelid='metric_data'::regclass ORDER BY pg_relation_size(c.oid) DESC
"@ | ForEach-Object { Write-Host $_ -ForegroundColor DarkGray }

    $invalid = @(Invoke-Q "SELECT '    INVALID: '||c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT i.indisvalid AND n.nspname='public';")
    if ($invalid.Count) {
        Write-Host "  invalid indexes (left by a cancelled REINDEX CONCURRENTLY -- drop them):" -ForegroundColor Yellow
        $invalid | ForEach-Object { Write-Host $_ -ForegroundColor Yellow }
    }
}

if ($Watch) {
    Write-Host "Watching prod every ${Every}s. Ctrl-C to stop." -ForegroundColor Cyan
    while ($true) { Show-Pass; Start-Sleep -Seconds $Every }
} else {
    Show-Pass
    Write-Host ""
    Write-Host "  Tip: -Watch to follow a vacuum/reindex to completion without reloading the app." -ForegroundColor DarkGray
}

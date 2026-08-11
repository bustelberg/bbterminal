<#
.SYNOPSIS
  Report and reclaim disk on PROD, for when Supabase has put the database into read-only mode.

.DESCRIPTION
  Supabase blocks writes at 95% of disk ("cannot execute INSERT in a read-only transaction") and
  limits disk EXPANSIONS to 4 per rolling 24h -- so when both trip at once the only way out is to
  make the database smaller. That happened on 2026-08-11 after a failed clone.

  IT REPORTS BY DEFAULT AND CHANGES NOTHING. Every mutating step is opt-in by switch, and they are
  offered in the order that makes each one POSSIBLE: a REINDEX needs free space to build the new
  index, so anything that frees space with no headroom must run first.

    (report)          sizes per table and per index, each index's SHARE of its table's scans,
                      replication slots, DB total -- and it nominates drop candidates
    -DropIndex <name> drop that index (you name it)         -> frees space, needs NO headroom
    -Reindex          rebuild the big indexes, smallest first -> frees bloat, NEEDS headroom
    -Vacuum           VACUUM (ANALYZE) the big tables       -> marks space reusable, fixes stats

  !! WHY A SCRIPT AND NOT THE SQL EDITOR. The dashboard editor has its own statement timeout, and
  a VACUUM or REINDEX of a multi-GB table outlives it -- which is exactly how "the vacuum one took
  too long" happens. Every statement here runs through psql with `statement_timeout = 0`, the same
  transport (and the same PROD_DB_URL out of scripts/.env.local) the clone script uses.

  !! IT SETS `transaction read write` PER SESSION. In read-only mode a maintenance command is
  itself a write and would be refused; this is the override Supabase's own documentation names.
  It does NOT flip `default_transaction_read_only` for the database -- once usage falls back under
  95% Supabase restores read-write on its own, and forcing it while still over the line only hides
  the condition.

.PARAMETER DropIndex
  Name(s) of index(es) to drop. The script NEVER chooses -- the report nominates candidates (under
  0.01% of their table's index scans) and prints the exact command; you decide. A primary key or a
  UNIQUE constraint's index is refused outright.

.PARAMETER Apply
  Required alongside any of -DropIndex / -Reindex / -Vacuum. Without it those are planned and
  printed, not run.

.EXAMPLE
  ./scripts/prod-reclaim-disk.ps1
.EXAMPLE
  ./scripts/prod-reclaim-disk.ps1 -DropIndex idx_metric_data_source_date -Apply
.EXAMPLE
  ./scripts/prod-reclaim-disk.ps1 -Reindex -Apply
#>
[CmdletBinding()]
param(
    [string]$ProdDbUrl,
    [string]$Container = 'supabase_db_bbterminal',
    [string[]]$DropIndex,
    [switch]$RebuildDropped,
    [switch]$Reindex,
    [switch]$Vacuum,
    [switch]$Apply
)

$ErrorActionPreference = 'Stop'

# ---- env + prod URL (identical resolution to clone-local-to-prod.ps1) -------
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
    Write-Host "ERROR: PROD_DB_URL not set (scripts/.env.local)." -ForegroundColor Red
    exit 1
}
if ($ProdDbUrl -notmatch '^(?<prefix>postgres(?:ql)?://[^:@/]+):(?<pw>[^@]+)@(?<rest>.+)$') {
    Write-Host "ERROR: PROD_DB_URL doesn't match 'postgres://user:password@host...'." -ForegroundColor Red
    exit 1
}
$prodEnv     = @('-e', "PGPASSWORD=$($Matches.pw)", '-e', 'PGOPTIONS=-c statement_timeout=0')
$prodUrlNoPw = "$($Matches.prefix)@$($Matches.rest)"

function Invoke-Prod([string]$sql) {
    # One READ. `-c` is fine here: a SELECT is happy inside the implicit transaction psql wraps a
    # multi-statement `-c` in. The read-write prefix is there because in read-only mode even a
    # read runs inside a read-only transaction.
    #
    # !! NEVER A `"""..."""` BLOCK HERE. PowerShell has no docstring: that is a STRING EXPRESSION,
    # and an expression whose value is not consumed is EMITTED. Written as one, it came back as
    # the first "row" of every query -- it printed as the database size, it printed as the
    # stats_reset timestamp, and (the dangerous part) it was parsed as an index row with zero
    # scans, so `-DropUnused -Apply` would have tried to drop it. Comments only.
    #
    # !! AND psql's COMMAND TAGS ARE NOT DATA. The two SET statements each emit a literal `SET`
    # line before the rows; unfiltered they became phantom index rows the same way. Dropped here,
    # in the one place every caller comes through, rather than in each parser.
    $full = "set session characteristics as transaction read write;`nSET statement_timeout = 0;`n$sql"
    $out = docker exec @prodEnv $Container psql $prodUrlNoPw -tA -F'|' -v ON_ERROR_STOP=1 -c $full
    if ($LASTEXITCODE -ne 0) { throw "prod psql failed:`n$sql`n$out" }
    return @($out | ForEach-Object { "$_".TrimEnd("`r") } |
        Where-Object { $_ -ne '' -and $_ -notmatch '^(SET|SET SESSION CHARACTERISTICS)$' })
}

function Invoke-ProdScalar([string]$sql) {
    <#
      The FIRST FIELD of the FIRST ROW, as a string.

      !! THIS EXISTS BECAUSE `(Invoke-Prod $sql)[0]` IS A TRAP, AND IT BIT. PowerShell unrolls a
      one-element array to a bare scalar, so a single-row result comes back as the STRING "20 GB"
      and `[0]` indexes the string -- yielding "2". The database total printed as `2`, the reset
      timestamp as `(`, and "Database: 2 -> 1" looked like the disk had shrunk by half.

      !! AND IT WAS NOT ONLY COSMETIC. The drop guard read `$m = $meta[0] -split '|'` on that same
      unrolled string, so `$m` was a single CHARACTER: the primary-key check passed only by luck
      (indisprimary is the first field, so its flag is the first character), and the UNIQUE check
      -- `$m[1]` -- was always null. A UNIQUE index enforcing a constraint could have been dropped.
      Every single-value read goes through here now.
    #>
    $rows = @(Invoke-Prod $sql)
    if (-not $rows.Count) { return '' }
    return (($rows[0] -split '\|')[0])
}

function Invoke-ProdMaintenance([string]$sql) {
    <#
      One MAINTENANCE command -- and it must go over stdin, not `-c`.

      !! `psql -c "a; b; c"` RUNS THE WHOLE STRING IN ONE IMPLICIT TRANSACTION, and every command
      this script actually needs refuses to run inside one:

          ERROR:  VACUUM cannot run inside a transaction block
          ERROR:  REINDEX CONCURRENTLY cannot run inside a transaction block
          ERROR:  DROP INDEX CONCURRENTLY cannot run inside a transaction block

      Reading from stdin (`-f -`) makes psql execute each statement separately, so the two SET
      lines still apply to the session and the maintenance command runs outside a transaction.
      The clone script uses the same shape for the same reason.
    #>
    $script = "set session characteristics as transaction read write;`nSET statement_timeout = 0;`n$sql`n"
    $script | docker exec -i @prodEnv $Container psql $prodUrlNoPw -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) { throw "prod maintenance failed (see output above):`n$sql" }
}

$running = docker ps --filter "name=$Container" --format '{{.Names}}'
if (-not $running) {
    Write-Host "ERROR: container '$Container' not running (it is only the psql client here)." -ForegroundColor Red
    exit 1
}

# ---- report -----------------------------------------------------------------
Write-Host "[1] Database total" -ForegroundColor Cyan
$dbSize = Invoke-ProdScalar "SELECT pg_size_pretty(pg_database_size(current_database()));"
Write-Host "  $dbSize"

Write-Host "`n[2] Largest relations" -ForegroundColor Cyan
foreach ($r in Invoke-Prod @"
SELECT n.nspname||'.'||c.relname, pg_size_pretty(pg_total_relation_size(c.oid)),
       pg_size_pretty(pg_relation_size(c.oid)), pg_size_pretty(pg_indexes_size(c.oid))
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','m') AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 10
"@) {
    $f = $r -split '\|'
    Write-Host ("  {0,-34} total {1,-10} heap {2,-10} indexes {3}" -f $f[0], $f[1], $f[2], $f[3])
}

Write-Host "`n[3] Indexes, with how often the planner has actually used them" -ForegroundColor Cyan
# !! `idx_scan` COUNTS SINCE THE LAST STATS RESET, AND A DISK-LIMITED INSTANCE RESTARTS. A zero
# here is "not used since the counters were cleared", not "never used in its life" -- which is why
# the reset time is printed beside it and why dropping requires the operator to name the index.
$idxRows = Invoke-Prod @"
SELECT s.relname, s.indexrelname, s.idx_scan,
       pg_relation_size(s.indexrelid), pg_size_pretty(pg_relation_size(s.indexrelid)),
       i.indisunique, i.indisprimary
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE pg_relation_size(s.indexrelid) > 50*1024*1024
ORDER BY pg_relation_size(s.indexrelid) DESC
"@
# !! THE SCAN COUNT IS ONLY MEANINGFUL AGAINST ITS OWN TABLE'S TRAFFIC. "30 scans" reads as
# "barely used" and "189,364 scans" as "busy" -- but both are meaningless until you know the table
# saw 38 MILLION index scans in the same window. The share is what separates a dead index from a
# rare-but-critical one, and it is the number this decision turns on.
$byTable = @{}
foreach ($r in $idxRows) {
    $f = $r -split '\|'
    if (-not $byTable.ContainsKey($f[0])) { $byTable[$f[0]] = 0.0 }
    $byTable[$f[0]] += [double]$f[2]
}
$candidates = New-Object System.Collections.ArrayList
foreach ($r in $idxRows) {
    $f = $r -split '\|'
    $isPk = ($f[6] -eq 't'); $isUniq = ($f[5] -eq 't'); $scans = [double]$f[2]
    $total = [Math]::Max($byTable[$f[0]], 1)
    $share = $scans / $total * 100.0
    $tag = ''; $colour = 'Gray'
    if ($isPk) { $tag = '  [PRIMARY KEY - never drop]' }
    elseif ($isUniq) { $tag = '  [UNIQUE constraint - never drop]' }
    elseif ($share -lt 0.01) {
        # Under one scan in ten thousand for its table. NOT dropped automatically -- see the
        # `-DropIndex` block: this only nominates, the operator names.
        $tag = '  <-- CANDIDATE: under 0.01% of this table''s index scans'
        $colour = 'Yellow'
        [void]$candidates.Add(@($f[0], $f[1], $f[4]))
    }
    Write-Host ("  {0,-14} {1,-44} {2,12} scans ({3,6:N3}%)  {4,9}{5}" -f `
            $f[0], $f[1], $f[2], $share, $f[4], $tag) -ForegroundColor $colour
}
$reset = Invoke-ProdScalar "SELECT COALESCE(stats_reset::text,'(never)') FROM pg_stat_database WHERE datname=current_database();"
Write-Host "  (usage counted since stats_reset = $reset)" -ForegroundColor DarkGray

Write-Host "`n[4] Replication slots (retained WAL counts against the disk)" -ForegroundColor Cyan
$slots = Invoke-Prod @"
SELECT slot_name, active::text,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots
"@
if (-not $slots) { Write-Host "  none." }
foreach ($r in $slots) {
    $f = $r -split '\|'
    Write-Host ("  {0,-34} active={1,-6} retained {2}" -f $f[0], $f[1], $f[2]) `
        -ForegroundColor $(if ($f[1] -eq 'f') { 'Yellow' } else { 'Gray' })
}

# ---- the three reclaim steps, each opt-in ----------------------------------
if (-not ($DropIndex -or $RebuildDropped -or $Reindex -or $Vacuum)) {
    if ($candidates.Count) {
        Write-Host "`nDrop candidates (frees space needing NO headroom -- do these before -Reindex):" -ForegroundColor Cyan
        foreach ($c in $candidates) {
            Write-Host "  ./scripts/prod-reclaim-disk.ps1 -DropIndex $($c[1]) -Apply   # $($c[2]) on $($c[0])"
        }
    }
    Write-Host "`nReport only. Add -DropIndex <name> / -Reindex / -Vacuum (with -Apply) to reclaim." -ForegroundColor Cyan
    exit 0
}

# (a) DROP A NAMED INDEX -- frees space needing NO headroom, so it goes first.
#
# !! THE OPERATOR NAMES IT; THE SCRIPT NEVER PICKS. An earlier version dropped everything it
# scored as unused, and a parsing bug fed that list two junk rows -- it would have issued a DROP
# built from a fragment of its own source code. Beyond the bug, "zero scans" is a claim about a
# stats window that a restart silently resets, and this database restarts when the disk fills.
# Nominating in the report and requiring the name here means a wrong number can waste your time
# but cannot drop an index by itself.
if ($DropIndex) {
    Write-Host "`n[5] Dropping named index(es)" -ForegroundColor Cyan
    foreach ($name in $DropIndex) {
        $safe = $name -replace '[^A-Za-z0-9_]', ''
        if ($safe -ne $name) { throw "Refusing '$name': an index name is [A-Za-z0-9_] only." }
        # !! `@(...)` IS LOAD-BEARING -- see Invoke-ProdScalar. Without it a one-row result unrolls
        # to a string and `$meta[0]` is its first CHARACTER, which silently reduced this guard to
        # a single flag.
        $meta = @(Invoke-Prod @"
SELECT i.indisprimary::text, i.indisunique::text, s.idx_scan,
       pg_size_pretty(pg_relation_size(s.indexrelid)), s.relname
FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.indexrelname = '$safe'
"@)
        if (-not $meta.Count) { Write-Host "  '$safe' does not exist -- nothing to do." -ForegroundColor Yellow; continue }
        $m = $meta[0] -split '\|'
        if ($m.Count -lt 5) { throw "Refusing '$safe': could not read its catalogue row ($($meta[0]))." }
        if ($m[0] -eq 't') { throw "'$safe' is a PRIMARY KEY. Refusing." }
        if ($m[1] -eq 't') { throw "'$safe' is UNIQUE and enforces a constraint. Refusing." }
        Write-Host "  DROP INDEX public.$safe   ($($m[3]) on $($m[4]), $($m[2]) scans)" -ForegroundColor Yellow
        # !! THE DEFINITION IS CAPTURED BEFORE THE DROP, AND THAT IS WHAT MAKES THIS REVERSIBLE.
        # Dropping a big index is the only lever left when the disk is too full to REINDEX -- it
        # frees space needing NO headroom. But `pg_get_indexdef` only exists while the index does:
        # drop it without recording the definition and putting it back becomes an archaeology
        # exercise against whichever migration created it. Saved to a file AND printed, because a
        # file nobody looked at is not a record.
        $def = Invoke-ProdScalar "SELECT pg_get_indexdef('public.$safe'::regclass);"
        $log = Join-Path $PSScriptRoot 'dropped-indexes.sql'
        if ($def) {
            $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm')
            Add-Content -Path $log -Encoding ascii -Value @(
                "-- dropped $stamp from prod to free $($m[3]) ($($m[2]) lifetime scans)",
                "-- rebuild with CONCURRENTLY so it does not lock the table; needs free space",
                "-- roughly the size above, so grow the disk FIRST.",
                # CONCURRENTLY so the rebuild does not lock the table, IF NOT EXISTS so running
                # the file twice is a no-op rather than an error -- a recovery step you are
                # afraid to re-run is one you put off.
                ($def -replace '^CREATE INDEX ', 'CREATE INDEX CONCURRENTLY IF NOT EXISTS ') + ';',
                ""
            )
            Write-Host "    definition saved to scripts/dropped-indexes.sql:" -ForegroundColor DarkGray
            Write-Host "      $def" -ForegroundColor DarkGray
        } else {
            throw "Refusing '$safe': could not read its definition, so the drop would not be reversible."
        }
        if ($Apply) {
            # CONCURRENTLY: a plain DROP INDEX takes an ACCESS EXCLUSIVE lock on the TABLE, which
            # on metric_data stalls the ingest behind it. The concurrent form takes a weaker lock
            # and cannot run inside a transaction block -- hence Invoke-ProdMaintenance.
            Invoke-ProdMaintenance "DROP INDEX CONCURRENTLY IF EXISTS public.$safe;"
            Write-Host "    dropped." -ForegroundColor Green
        }
    }
    if (-not $Apply) { Write-Host "  (dry run -- pass -Apply to drop)" -ForegroundColor DarkGray }
}

# (a2) PUT BACK what -DropIndex took out, from the definitions it saved.
#
# !! THIS IS THE SECOND HALF OF THE ONLY REBUILD A FULL DISK ALLOWS. `REINDEX` needs the old and
# new copies to coexist; DROP-then-CREATE never does, so on a disk with no headroom it is the ONLY
# route to a compact index -- and it comes back smaller than it went, because the bloat is not
# copied. Measured shape on prod: the covering index sits at 2.20x its table's heap where local
# runs the same definition at 1.34x, so ~6.0 GB should return as ~3.7 GB.
#
# !! THE REBUILD IS THE STEP PEOPLE SKIP, WHICH IS WHY IT IS A SWITCH AND NOT A COMMENT. A dropped
# index that never comes back is a permanent, silent performance regression -- the queries still
# work, so nothing ever fails loudly enough to remind you.
if ($RebuildDropped) {
    $log = Join-Path $PSScriptRoot 'dropped-indexes.sql'
    Write-Host "`n[5b] Rebuilding indexes recorded in scripts/dropped-indexes.sql" -ForegroundColor Cyan
    if (-not (Test-Path $log)) {
        Write-Host "  no such file -- nothing was dropped by this script." -ForegroundColor Yellow
    } else {
        $stmts = @(Get-Content $log | Where-Object { $_ -match '^\s*CREATE INDEX' })
        if (-not $stmts.Count) { Write-Host "  file has no CREATE INDEX lines." -ForegroundColor Yellow }
        foreach ($s in $stmts) {
            $name = if ($s -match 'IF NOT EXISTS\s+(\S+)\s+ON') { $Matches[1] } else { '?' }
            Write-Host "  $($s.Trim())" -ForegroundColor Yellow
            if ($Apply) {
                $sw = [System.Diagnostics.Stopwatch]::StartNew()
                Invoke-ProdMaintenance $s
                $size = Invoke-ProdScalar "SELECT pg_size_pretty(pg_relation_size('public.$name'::regclass));"
                Write-Host ("    built: {0}  ({1:N0}s)" -f $size, $sw.Elapsed.TotalSeconds) -ForegroundColor Green
            }
        }
        if (-not $Apply) { Write-Host "  (dry run -- pass -Apply to build)" -ForegroundColor DarkGray }
    }
}

# (b) REINDEX -- reclaims index bloat, but BUILDS A NEW INDEX FIRST, so it needs headroom.
if ($Reindex) {
    Write-Host "`n[6] Rebuilding indexes, SMALLEST FIRST" -ForegroundColor Cyan
    # !! SMALLEST FIRST IS NOT A PREFERENCE. Each REINDEX needs free space for the new copy of
    # THAT index; starting with the biggest is how you fail at 95% full and gain nothing. Each
    # small one that finishes frees its own bloat and makes the next one possible.
    $targets = @()
    foreach ($r in $idxRows) {
        $f = $r -split '\|'
        if ($DropIndex -contains $f[1]) { continue }   # dropped above; do not rebuild it
        $targets += ,@([int64]$f[3], $f[1], $f[4], $f[0])
    }
    foreach ($t in ($targets | Sort-Object { $_[0] })) {
        Write-Host "  REINDEX public.$($t[1])  (currently $($t[2]) on $($t[3]))" -ForegroundColor Yellow
        if ($Apply) {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            # CONCURRENTLY keeps the table readable/writable while it rebuilds. It costs MORE peak
            # space (old + new + a transient) -- the trade this script accepts because prod serves
            # traffic. If space is too tight for it, the plain form is `REINDEX INDEX name`.
            Invoke-ProdMaintenance "REINDEX INDEX CONCURRENTLY public.$($t[1]);"
            $now = Invoke-ProdScalar "SELECT pg_size_pretty(pg_relation_size('public.$($t[1])'::regclass));"
            Write-Host ("    {0} -> {1}  ({2:N0}s)" -f $t[2], $now, $sw.Elapsed.TotalSeconds) -ForegroundColor Green
        }
    }
    if (-not $Apply) { Write-Host "  (dry run -- pass -Apply to rebuild)" -ForegroundColor DarkGray }
}

# (c) VACUUM -- does not usually return space to the OS, but stops the growth and fixes the stats
#     autovacuum reads. ANALYZE is the load-bearing half here: prod reported n_live_tup 25,413 for
#     asset_price, which is off by orders of magnitude, and autovacuum sizes its thresholds off
#     exactly that number -- so a wrong estimate is self-perpetuating.
if ($Vacuum) {
    Write-Host "`n[7] VACUUM (ANALYZE) on the big tables" -ForegroundColor Cyan
    foreach ($t in @('public.metric_data', 'public.asset_price')) {
        Write-Host "  VACUUM (ANALYZE) $t" -ForegroundColor Yellow
        if ($Apply) {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            Invoke-ProdMaintenance "VACUUM (ANALYZE) $t;"
            Write-Host ("    done ({0:N0}s)" -f $sw.Elapsed.TotalSeconds) -ForegroundColor Green
        }
    }
    if (-not $Apply) { Write-Host "  (dry run -- pass -Apply to run)" -ForegroundColor DarkGray }
}

$after = Invoke-ProdScalar "SELECT pg_size_pretty(pg_database_size(current_database()));"
Write-Host "`nDatabase: $dbSize -> $after" -ForegroundColor Cyan
Write-Host "Supabase restores read-write on its own once usage is back under 95% of the disk." -ForegroundColor Cyan

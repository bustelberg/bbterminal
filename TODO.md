# Open follow-ups — resume here

Running list of unfinished / offered-but-not-built work, newest context first.
Last updated **2026-08-19**. Delete items as they're done.

---

## ⏱ Analyse modal: the FIRST open is still the full load (2026-08-19)

**Done that day**: the cross-portfolio leg cache (`_analysis_cache.leg`) + routing
`compute_portfolio_performance` through `_airs_ref`. A *second* book now costs 27 round trips and
12 COPYs where it cost 35 and 24; the basket path went 396ms → 113ms. Full write-up, numbers and
the two scripts (`profile_analysis_modal.py`, `verify_analysis_cache.py`) in
[`docs/airs-portfolios.md`](docs/airs-portfolios.md).

**Not done**: a cold server still pays the whole ~7s (production) on the first open, and the
fingerprint moves whenever any watched table is written — so the first open after each daily price
refresh is cold again for everybody. Two ways to remove that, neither built:

* **Warm the cache after the AIRS refresh.** `airs_vermogen_refresh` (09:30 Amsterdam) and the
  05:00/06:00 price ticks are what move the fingerprint. Recomputing the 26 paired books afterwards
  (~26 × 2s with the legs warming each other) would make the first human open of the day a HIT.
  ⚠ Must run AFTER the price writes settle, or it warms against a fingerprint that is about to
  change; ⚠ and must not overlap a pipeline — this is a free-tier box and the modal computes in a
  worker thread, so background warming competes with real requests for the GIL.
* **Prefetch on hover of the Analyse button.** Cheapest perceived win — the request is in flight
  before the click. ⚠ **Needs single-flight in `_analysis_cache` first**: hover-then-click within
  the compute window would otherwise run the same ~7s computation twice, and a fast mouse down the
  table would fire several. Bound it (one outstanding prefetch, ~150ms dwell) or it makes the real
  click slower rather than faster.

**Also measured and NOT taken** — overlapping the benchmark side with the portfolio side in a
thread. It is the only lever left that attacks the *first* open (~2.6s of the production load is
serialized network wait), but `common/pg.py` states in capitals that a psycopg connection is not
thread-safe and each worker needs its OWN `copy_connection_scope`, while `read_cache`'s ContextVar
does not propagate to a bare `threading.Thread`. Getting either wrong shares a live connection
across threads. Worth doing deliberately, not as a footnote to a caching change.

**Remaining round trips, if someone wants the cold path**: `airs_holding` is read through **6
distinct query shapes** (3 per portefeuille × the wrapped book) inside `resolve_account_isins` /
`_wrapped_book_marks` — the exact fragmentation `_airs_ref` exists to fix, worth ~4 round trips
(~0.24s production). The paged tables each spend one extra request proving the page was the last
one, which is the load-bearing "break on an empty page" rule and must stay.

---

## 🔁 "Refresh all from AIRS" is the LAST button not going through `refresh_portfolio_fully` (2026-08-18)

**Done that day**: a portfolio is a PAIR (Fixed model + Dynamic book) and each refresh button used
to refresh ONE half — /management-dashboard's row the book, /portfolios' row the model, the Analyse
modal whichever panel opened it. `routers/_airs_full_refresh.py::refresh_portfolio_fully` now does
both from either handle, and **both per-portfolio endpoints call it**
(`POST /api/airs/portfolios/{portefeuille}/refresh[/job]`, `GET /api/airs/model-portfolios/{id}/refresh`).
`refresh_many` is the tested concurrent fan-out over it — a thread pool and nothing else.

**Not done**: `airs_vermogen.run_airs_vermogen_refresh_sync` (the "Refresh all" button + the
scheduler tick) still calls `scan_one` directly, so a fleet pass refreshes **books only** — 64 of
102 model compositions and every model's prices/FX are untouched by it.

**⚠ THE BLOCKER IS ITS LOCK DISCIPLINE, NOT THE CALL SITE.** The fleet takes `_LOCK` **once for the
whole run**; `refresh_portfolio_fully` takes it **per AIRS leg** (which is what lets the Yahoo/FX/
price legs overlap). Calling the new function from inside the fleet's hold self-deadlocks on a
non-reentrant lock. Converting it means the fleet stops holding the session globally, and that
lands on three documented invariants at once:

* **`_STATUS` / `_PROGRESS` are single-writer globals** (`_PROGRESS` is explicitly "one run at a
  time, guarded by `_LOCK`"). A pool needs them thread-safe.
* **"the counter runs over the ROSTER, not the worklist"** — a long, deliberate design (it reads
  n/44 and names each skip) that assumes sequential progress. Under a fan-out it needs re-thinking,
  not just re-wiring.
* **Run duration changes shape.** Adding the model half for the ~28 paired accounts means
  composition + OpenFIGI + FX backfill + a price fetch per holding, ×28. Concurrency wins some of
  that back; it is still a different job from today's book-only sweep. **Decide whether "Refresh
  all" should do this at all, or stay the fast book sweep with a separate "rebuild every model".**

---

## ⚡ /management-dashboard speed — MEASURED 2026-08-11. The frontend is already clean; the cost is inside the endpoints.

**⚠ THE FRONTEND IS NOT THE PROBLEM AND DOES NOT NEED CHANGING.** Audited: the page fires
**exactly ONE request on mount** (`GET /api/airs/portfolios/overview`), the other two tabs are lazy
and stay mounted after first open, and **nothing polls**. The "no unneeded API calls" goal is
already met at the page level — every remaining call is a user action. The waste is entirely in how
many database round trips ONE endpoint makes.

**⚠ COUNT ROUND TRIPS, NOT MILLISECONDS.** Prod is in **eu-west-3 (Paris)** and Railway is
elsewhere, so each PostgREST call costs ~40-80ms of pure network. A local profile understates this
by an order of magnitude. Measure with the instrumentation pattern that produced the numbers below:
wrap **`deps.supabase.postgrest.session._c.request`** (the inner httpx client), NOT
`session.request` — the memo sits between them, so wrapping the outer one counts memo HITS as round
trips and reports no improvement from a change that worked. (I made exactly that mistake and nearly
filed the memo as broken.)

### Measured baseline (local, memo active, as production runs it)

| | round trips | rows |
|---|---|---|
| `GET /portfolios/overview` (page load) | **13 → 12** | 3,760 → 2,778 |
| `GET /model-portfolios/{id}/analysis` (Analyse) | **92** | 22,416 |

The modal's memo already absorbs **103 of 195** logical reads. The 92 that remain are the target;
at 60ms that is ~5.5s of latency before a single query runs.

### ✅ Done — the dead read (free)

`_airs_overview.list_overview` built a `positions` dict from a **982-row
`airs_model_portfolio_position`** read that **nothing consumed** — the `isins` column had moved to
the account's own count and only the *use* was deleted. ⚠ **A read whose result is discarded cannot
produce a wrong answer, only a slow one**, which is why it survived. Verified: 13 → 12 round trips,
payload byte-identical (45 rows, 26 keys).

### ✅ 1 & 2 DONE (2026-08-11) — modal 92 → 71 HTTP, rows 22,416 → 9,570

Verified by deep-comparing the whole Analyse payload before/after: **structure exact, largest
numeric difference `3.55e-15`** (pre-existing float-ordering noise; a hash is useless here because
the payload is not byte-stable between two runs of *identical* code — only `timings_ms` and 1e-15
float noise move).

**1. `common/fx_load.py` — one FX loader, COPY first.** `_benchmark_index._fx_to_eur` and
`_airs_portfolio_perf._fx` each called the other its twin *in the docstring* and had drifted: only
one had the COPY fast path, so the modal paid **17 HTTP round trips / 13,617 rows** on the AIRS
side for what the benchmark side did in 4 COPYs. Now one definition; `fx_rate` is **gone from HTTP
entirely** (6 COPYs). Proven identical: 14 currencies, 12,589 rates, COPY vs paged byte-for-byte.
⚠ Removing the old copy also deleted `_JUMP_LO/_JUMP_HI/_SPLIT_*` by accident (a line-range edit) —
restored; the split whitelist is load-bearing (see `project_prices_not_split_adjusted`).

**2. `routers/_airs_ref.py` — the small tables read once.** `airs_model_portfolio` (**102 rows**)
was read 8× with **8 different `select=` lists**, so the memo could not collapse a single one —
eleven correct modules each asking for exactly what they needed, and *that precision* defeated it.
Now one canonical superset query per table, filtered in Python, so the EXISTING memo dedupes them
for free (no new cache, and the memo's fresh-dicts-per-caller safety is preserved — a hand-rolled
row cache would have quietly shared 982 mutable dicts).

⚠⚠ **AND THAT WORK SURFACED A LIVE PRODUCTION HAZARD, UNRELATED TO SPEED:**
`airs_model_portfolio_position` holds **982 rows against PostgREST's 1,000-row cloud cap — 18 rows
of headroom** — and several existing readers use an unpaged `.limit(20000)`, which does nothing
(the server's cap binds, not ours). Local's cap is 10,000, so the first symptom would have been
**production silently losing positions while every local check passed.** `_airs_ref` pages, ⚠ **on
the primary key `id`, NOT `(portfolio_id, isin)` — that pair is NOT unique: measured, the table
contains exactly one duplicate** (a model listing the same instrument at two weights, the
CapitaLand case in CLAUDE.md), and paging on a non-unique key serves a row twice or never.
Pinned by `tests/test_airs_ref_paging.py` (caps down to 1 row/response, duplicate preserved).
⚠ **The other unpaged readers of this table are still out there** — `_airs_account_links` pages but
on the non-unique pair; the rest of the `.limit()` call sites should be swept onto `_airs_ref`.

**Still open from the original list:** `asset_grid` 11×, `airs_holding` 10×, `asset_execution` 6×,
`airs_mutatie` 6× — same whole-table treatment, plus items 3 and 4 below.
⚠ **Check `SUPABASE_DB_URL` is set on Railway.** With it unset the modal does not merely slow down,
it **500s** (`asset_price` has no PostgREST fallback), so it must already be set in prod — but the
COPY win above depends on it, and nothing warns if it silently goes missing.

### Where the Analyse modal's time goes now (measured 2026-08-11, after 1 & 2)

Local total **3,934 ms**, and ⚠ **the ranking is DIFFERENT in production, which is why a laptop
profile keeps pointing at the wrong thing**:

| | local | production estimate |
|---|---|---|
| 71 HTTP round trips | ~355 ms (5 ms each) | **~3,550 ms** (50 ms each) |
| 16 COPY calls | 1,480 ms | ~1,500 ms |
| pandas / Python compute | ~2,450 ms | ~2,450 ms |
| **total** | **~3.9 s** | **~7 s** |

Phases (sequential and dependent — not trivially parallelisable):
`returns_and_benchmark` 2,030 ms · `book_holdings` 922 ms · `axes` 801 ms ·
`composition_and_benchmark` 777 ms. Flat-ish, i.e. **there is no single hot query left to fix**.

⚠ **The frontend is NOT a factor and needs no work**: the modal fires exactly ONE request, the page
one on mount, row expansion two in parallel. Everything below is backend.

### ✅ A & B DONE (2026-08-11) — Analyse re-open **6,173 ms → 1 ms**

**A. `routers/_analysis_cache.py` — cached on a DATA FINGERPRINT, not a clock.** Steady state
measured: first open 6,173 ms, every re-open 1 ms, fingerprint stable; switching to another
portfolio correctly recomputes (2,217 ms) and switching back is instant. In production add the
fingerprint round trip (~34 ms local) on a re-open after >2 s, so realistically **~7 s → ~50-100 ms**.

⚠ **A TTL WAS THE WRONG INSTRUMENT and the obvious one.** This page's discipline is "current or
absent"; a time-based cache would show yesterday's book after a refresh with nothing saying so.
The key contains a fingerprint of the data, so stale is unreachable rather than unlikely.

⚠ **THE OBVIOUS FINGERPRINT WAS 1,150 ms WARM / 14.9 s COLD** (`count(*)` + `max(updated_at)` per
table — `count(*)` is a full scan and `asset_price` holds 39.5M rows). That would have replaced a
7-second recompute with a 1.2-second floor and *looked like a win*. The one used instead is
`SELECT relname, n_tup_ins+n_tup_upd+n_tup_del FROM pg_stat_user_tables` — **3.5-20 ms**, a catalog
read that never scans, and it sees writes made by **another replica or the scheduler**, which no
in-process invalidation can. Verified end to end: a real write to `asset_bucket_override` moves the
fingerprint and forces a recompute.
⚠ It folds in `pg_postmaster_start_time()` + `stats_reset` because those counters **reset to zero**
on restart — without them the fingerprint could go BACKWARD and match newer data.
⚠ `_WATCHED` must list every table the endpoint reads (derived by instrumenting a real call, not by
reading code); a table missing from it is a table whose changes are invisible.

**B. Whole-table sweep — and ⚠ MY OWN PLAN WAS WRONG FOR MOST OF IT.** The pattern only wins when
the table is small AND read for several books per request; a whole-table read costs
`ceil(rows/1000)+1` round trips. Measured before applying:

    airs_mutatie         999 rows,  6x -> 2   ✅ done      airs_holding    9,817 rows, 10x -> 11  ❌
    airs_model_weight    734 rows,  4x -> 2   ✅ done      asset_analysis  8,376 rows,  3x ->  9  ❌
                                                          asset_execution 16,150 rows, 6x -> 17  ❌ 3x WORSE

Applying it to `asset_execution` "because it looked like the others" would have nearly tripled its
round trips. ⚠ Also caught: `airs_model_weight` has **no `id` column** — its PK is the composite
`(portefeuille, fonds)`, so `_paged` takes the key as a tuple. Payload verified unchanged
(largest numeric difference **0**).

⚠ **A measurement trap worth keeping**: instrumenting `pg._run_copy_uncached` made the fingerprint
silently return `None`, so the cache never hit and the run *looked* like the cache was broken.
Wrap the HTTP client only; leave the COPY path alone when measuring this.

### ✅ Benchmark COPYs DONE — and ❌ `asset_grid` MEASURED AND DELIBERATELY NOT DONE (2026-08-11)

**Benchmark: 2 COPYs → 1 (922 ms → 439 ms).** `index_returns`' docstring said "ONE price load" but
it ran `window_marks` **once per anchor** — two full COPYs over the identical 500 ids and the
identical date window, differing only in which anchor picked the opening mark. `window_marks_multi`
does the per-anchor selection inside one query. ⚠ **The jump set stays per-anchor**: jumps are
those at or after that anchor's own opening mark, because a split BEFORE the mark is already
absorbed into it and re-applying it rescales a price that was never on the old basis — a shared
jump set would be wrong for whichever anchor is later. Verified field-for-field against the
per-anchor loader (**IDENTICAL**, jumps included: 3 at one anchor, 0 at the other) and end to end
(largest numeric difference **0**). ⚠ The single-anchor `window_marks` is KEPT — it is the
reference the multi version is checked against, and `index_rows` has one window.

**❌ `asset_grid`: my own plan item was wrong, and the measurement says leave it alone.** The
proposal was "unify the `select=` lists so the memo dedupes". Measured on a real call:

    isin calls          8 calls, 703 ids requested, 538 DISTINCT  -> only 165 redundant (23%)
    analysis_id calls   3 calls, 502 ids requested, 502 DISTINCT  -> ZERO redundancy

⚠ **There are 3 distinct column sets but ELEVEN distinct FILTERS**, so unifying columns dedupes
nothing — the memo keys on the whole request. And the three `analysis_id` calls are not duplication
at all: they are ONE logical read of 502 ids chunked at `IN_CHUNK_SIZE=200` because of URI length.
The most an accumulating per-request loader could win is ~8 → ~5 calls ≈ **0.4 s off the COLD open
only** (~6%), against touching the reads that feed sector/region classification. With the payload
cache in place the cold path is paid once. Not worth the risk; revisit only if the cold open itself
becomes the complaint, and then via COPY (no URI limit ⇒ 502 ids in one round trip), not via
column-unification.

**Current state of the Analyse modal:** cold **65 HTTP round trips / ~5.1 s**, re-open
**0 round trips / 35 ms**.

### ✅ D — PROFILED (2026-08-11). ⚠ "~2,450 ms of pandas" WAS WRONG.

`cProfile` sorted by **tottime** (self time — cumulative just re-reports the outer frames and the
time spent *waiting* on the database). Total **7,244 ms → 5,402 ms** after two fixes:

| self time | what it actually is |
|---|---|
| **1.19 s** | `select.select` + `socket.recv` — I/O **WAIT**, not compute. I had counted this as pandas. |
| **1.16 s** | pydantic `validate_json` + `validate_python` — postgrest deserialisation |
| 0.26 s → **0** | pandas Arrow `__iter__`, 102,348 calls — **fixed** |
| 0.23 s | `_rate`, 103,835 calls |

**Fixed: `_closes` was zipping three ARROW-BACKED pandas Series element by element** — one boxed
scalar per element, 102,348 iterator steps. `.tolist()` does each column in one C pass.
Cumulative **1.453 s → 0.899 s**. ⚠ Nulls arrive as `None` from an Arrow column and `nan` from a
numpy one, and `pd.notna` is gone, so BOTH are checked — dropping either turns a missing close into
a real price of `nan`.

⚠⚠ **A "FREE WIN" THAT WAS 2.3x SLOWER, MEASURED.** `_rate`'s fallback builds a list of every
earlier date then takes `max()`. Replacing it with `max(d for d in tbl ...)` — same O(n) scan, no
list allocated — is *obviously* better and is **wrong**: the genexpr profiled at **2,038,758 calls
/ 0.295 s** with `max` rising 0.048 → 0.283 s. **Since PEP 709 (Python 3.12) a list comprehension
is INLINED into its enclosing frame, while a generator expression still builds and resumes a frame
per item.** Reverted, with the measurement in the code so nobody "optimises" it again.

### ✅ The pydantic item — DONE. Cold Analyse **7,244 ms → 3,870 ms** (−47%).

`common/parse_cache.py` reuses the PARSE of a response `read_cache` served from its memo, instead
of letting postgrest re-run the full pydantic parse on every hit.

    validate_json    196 calls / 0.821 s  ->  102 calls / 0.142 s
    validate_python  197 calls / 0.338 s  ->  gone from the top 10

⚠ **THE ORIGINAL DOCSTRING WAS HALF RIGHT, AND THE MEASUREMENT SETTLED WHICH HALF.** It refused to
cache parsed rows because a deep copy is "not obviously cheaper than the query it replaces".
Measured on the real payloads: **`deepcopy` IS worse than re-parsing** (8.86 ms vs 2.89 ms on
`airs_performance`) — that instinct was correct. What it got wrong was concluding no copy works:
a **shallow** copy is 0.23 ms, ~25x cheaper than the full parse.

⚠ **A SHALLOW COPY IS ONLY SAFE WHEN EVERY VALUE IS A SCALAR — CHECKED PER PAYLOAD, NOT ASSUMED.**
`dict(row)` shares nested values, and the schema has `jsonb` + array columns
(`asset_universe.params`, `airs_model_portfolio.positions_dates`). `_is_flat` scans once at parse
time; non-flat payloads deep-copy only their non-scalar values (still 2-3x cheaper than a re-parse).
None of the six largest payloads on this path has a single nested value.

⚠ **CALLERS DO MUTATE THEIR ROWS** — `_benchmark_index._members` runs `r["currency"] = ...` in
place. So the pristine parse is kept as a master that is never handed out, and **the first caller
gets a copy too**. Pinned by `tests/test_parse_cache.py`, including that a mutation of the first
parse cannot leak into the second.

⚠ **`model_construct`, NOT `APIResponse(...)`, ON A HIT.** The normal constructor re-validates the
whole payload — that was the entire remaining `validate_python` cost (0.338 s) and would have eaten
most of what skipping the JSON parse just saved. These rows came out of a validated parse and were
copied, not built.

⚠ **The cache lives on the `httpx.Response` object itself**, not in an `id()`-keyed map: `read_cache`
returns the same instance on a hit, so the attribute is exactly as long-lived as the response and
cannot alias another one after a GC.

**Superseded note, kept for the reasoning:** the original write-up of this item said —

**The real remaining compute item is pydantic, at 1.16 s (21%)** — 196 `validate_json` calls for 65
HTTP requests, because `read_cache` caches the HTTP RESPONSE and postgrest re-parses it into fresh
dicts on every memo hit (~103 of those parses are re-parses). That is a deliberate safety property
(callers cannot corrupt each other's rows), and its docstring judged a deep copy "not obviously
cheaper". ⚠ Now measurable: a re-parse is ~4.8 ms; `[dict(r) for r in rows]` on the same payload is
~1 ms. Worth revisiting — but it is a change to a shared safety-critical module, so measure the
copy cost on the LARGEST payload (`airs_performance`, 150 KB) before touching it.

⚠ The `_rate` scan itself (0.23 s) should NOT be fixed with a bisect over cached sorted keys — the
cache would key on the `fx` dict's identity and `id()` is reused after GC, so a stale hit converts
a price at ANOTHER currency's rate. The structural fix is to stop calling `_rate` per date:
`_eur_series` and the FX table are both date-ordered and could merge-walk once, O(n+m).

### ✅ 2026-08-11 — one Postgres connection per REQUEST, not per COPY. **~3.3 s off production.**

⚠⚠ **THE COST A LOCAL PROFILE STRUCTURALLY CANNOT SEE, WHICH IS WHY IT SURVIVED FOUR ROUNDS OF
PROFILING.** `common/pg._run_copy_uncached` opened a fresh `psycopg.connect()` for every COPY.
Measured (connect + `SET statement_timeout` + `SELECT 1`):

    local (127.0.0.1)         24.0 ms          production (eu-west-3, via Supavisor)   220.7 ms

The Analyse modal issues 17 COPYs ⇒ **~3.75 s in production spent purely opening connections**,
against 0.41 s locally. Every profile I took reported it as ~4% of the time.

Now scoped to the request: `copy_connection_scope()` in `common/pg.py`, entered by `read_cache`
(already exactly "one request"), so **every existing caller gets it with no call-site change**.
Measured: **16 connections → 1**, local cold 5,801 → 2,711 ms, payload identical (largest numeric
difference **0**).

⚠ **KEYED PER THREAD, AND THAT IS CORRECTNESS, NOT TIDINESS.** A ContextVar is COPIED into a
worker by `asyncio.to_thread`, so several workers share one scope — and **a psycopg connection is
not thread-safe**. Two COPY streams interleaved on one socket do not raise, they return the wrong
bytes. Pinned by `tests/test_copy_connection_scope.py`.
⚠ **Any COPY failure drops the connection** so the next one reconnects: a poisoned session would
otherwise fail every remaining COPY in the request instead of just that one.
⚠ **Nesting must not close the outer connection**, and `SET statement_timeout` runs once per
connection rather than once per COPY.

### ✅ `asset_grid` via COPY (2026-08-11) — 11 HTTP round trips → **0**

⚠ **THIS REVERSES THE EARLIER "NOT WORTH IT" VERDICT, AND THE REASON IS THAT THE ECONOMICS
CHANGED.** Rejecting it was right at the time: the proposal then was *select-list unification*,
which the measurement showed wins nothing (11 distinct filters, only 23% id overlap, and the three
`analysis_id` calls are one logical read chunked at 200 for URL length — zero redundancy). The
lever was always COPY, and COPY only became cheap once the connection stopped being re-opened per
call.

`common/pg.load_rows_via_copy(table, columns, key_col, values)` — no URL, so the whole id list
goes in one `= ANY()`. Wired into `_grid`, `_asset_benchmark` and `_airs_holding_isin`, each with
the chunked PostgREST loop kept as the fallback. Verified against PostgREST **field for field,
types included, on all three column sets** (after deduping ids — `asset_grid` is one row per
EXECUTION, so a duplicated id in two chunks makes PostgREST return the row twice; the real callers
use `sorted(set(...))`). End-to-end payload unchanged (largest numeric difference 3.55e-15, the
pre-existing float-ordering noise).

⚠⚠ **THE ROWS ARE SHIPPED AS JSON, NOT CSV COLUMNS — AND THAT IS NOT A STYLE CHOICE.** Every other
COPY loader here parses with `line.split(",")`, which is safe only because those queries select
numbers and dates. These select `name` / `gf_company_name` / `openfigi_name` / `leonteq_name`, and
**1,948 rows in `asset_grid` have a comma in `name`** ("Alphabet, Inc."). A comma-split would shift
every field after the name by one — a sector, currency and market cap attributed to the WRONG
instrument, parsing cleanly, raising nothing. `row_to_json` also keeps types and distinguishes NULL
from `""`, which bare CSV cannot. Pinned by `tests/test_load_rows_via_copy.py`.

**Net: 65 → 54 HTTP, 17 → 23 COPY (all on the one pooled connection), local cold 2,711 → 2,361 ms.**
⚠ Honest accounting: that is ~5 fewer round trips overall, not 11 — the 11 HTTP calls became 6
COPYs. The win is real but smaller than "11 → 0" suggests, and it is worth more in production
(~50 ms per HTTP hop) than the local clock shows.

### ✅ `asset_execution` + `airs_holding` + the last `_airs_ref` stragglers (2026-08-11)

**`_executions` → COPY.** 6 chunked round trips → 1. ⚠ The `r["isin"] not in out` guard reads like
a transport-order-dependent "first listing wins" — it is not: **`asset_execution.isin` carries a
UNIQUE constraint** (16,613 rows / 16,613 distinct). Checked rather than assumed, because if a
duplicate were possible the two transports could pick different winners and only under COPY.

**`airs_holding` snapshot → COPY.** Removes the paging *and* its empty probe page (a 24-row
snapshot issued a second request at `offset=24` purely to come back empty). 10 → 6.
⚠⚠ **THE FIRST ATTEMPT WAS A SILENT 18.8x OVER-FETCH.** `load_rows_via_copy` took one key, so I
filtered on `portefeuille` and re-applied `as_of_date` in Python — **788 rows instead of 42**,
because the table keeps 28 historical snapshots per book. Right answer, far more bytes, and
nothing would have reported it. The loader now takes a `where=` for extra equality predicates.

**Three readers of `airs_model_portfolio_position` were still bypassing `_airs_ref`** — visible
only by dumping the actual query params: `order=id.asc` (correct), `order=portfolio_id.asc,isin.asc`
(`_airs_account_links`), and two unpaged reads (`_airs_portfolio_links`). ⚠ The second was **paging
on a NON-UNIQUE key** — the CapitaLand duplicate — so it also had a latent
serve-twice-or-skip bug at a page boundary. All migrated: 6 → 3, and `airs_model_portfolio` 5 → 3.

⚠⚠ **AND THAT MIGRATION NEARLY CHANGED 30 PORTFOLIOS' NUMBERS.** `_airs_account_links` counted
EVERY position row; `_airs_ref.position_counts()` counts only ISIN-bearing ones (the grid counts
*instruments*, and a cash line is not one). **31 position rows have no ISIN, across 30 portfolios**,
so swapping in the shared helper would have quietly altered more than half the list's `positions`.
The count is now computed inline from the shared read, preserving the original semantics exactly.
Verified: overview + account-links payload **IDENTICAL**.

### Where /management-dashboard stands now (local, cold vs re-open)

| | round trips | pg connections | time |
|---|---|---|---|
| Analyse — first open | 65 HTTP | **1** | **2,711 ms** (was ~7,200) |
| Analyse — re-open | **0** | 1 | **25 ms** |

⚠ **Production is still dominated by the 65 HTTP round trips (~50 ms each ⇒ ~3.3 s).** That is now
the single biggest remaining item and the local clock will keep under-reporting it. The tables are
`asset_grid` 11 · `airs_holding` 10 · `asset_execution` 6 · `airs_mutatie`/`airs_model_weight` — and
the lever is **COPY** (no URI chunking limit ⇒ 502 ids in one round trip), not the
select-list unification measured and rejected above.

### Next, in value order

**A. CACHE THE ANALYSE PAYLOAD — by far the biggest win, and the only one that changes the feel.**
Re-opening the same portfolio, toggling benchmark and back, or switching `weight_by` recomputes all
~7s from scratch. Key on `(portfolio_id, benchmark, weight_by, source)` + a **data-version stamp**
so it can never serve stale numbers: `max(airs_model_portfolio.positions_scanned_at)`,
`max(airs_holding.as_of_date)`, `max(asset_price.date)` — three tiny reads against a 7-second
recompute. ⚠ Must be a *version* key, not a TTL: this page's whole discipline is that a number is
either current or absent, and a 5-minute TTL silently serves yesterday's book right after a scan.

**B. Finish the whole-table sweep (~37 round trips → ~10, ≈1.3 s of prod latency).** Same proven
`_airs_ref` pattern, now for `asset_grid` 11× · `airs_holding` 10× · `asset_execution` 6× ·
`airs_mutatie` 6× · `airs_model_weight` 4×. ⚠ Check each table's row count against the 1,000-row
cloud cap FIRST and page on a unique key — `airs_holding` is the one to check, it is per-book and
grows.

**C. The three 500-id benchmark COPYs cost 913 ms** (177 + 371 + 365). Two share an identical id
set AND date range, differing only in an anchor parameter; the third is a sub-window of them.
Loading the widest window once looks like 913 → ~371 ms. Verify before building.

**D. ~2,450 ms is Python/pandas.** Do NOT guess at this — profile it (`cProfile` on one call) before
touching anything. It is now the largest local cost and the second largest in prod.

**E. Page load: `_year_perf` → a view/RPC** (3 round trips + 1,473 rows → 1 + ~45). Small next to
the above, but it is on every page load rather than every modal open.

⚠ **A hypothesis I tested and DISPROVED, recorded so nobody re-derives it**: that COPY calls miss
the memo because their id lists are unsorted. Measured: sorting would collapse **zero** calls —
the repeated-looking COPYs genuinely differ.

### 1. FX through COPY — the single biggest win (17 round trips → 1)

`fx_rate` is **17 of the modal's 92 round trips and 13,617 of its 22,416 rows**, and ⚠ **14 of
those are the SAME query differing only by `offset`** — PostgREST's 1,000-row pager, walking 14
pages for one logical load. `common/pg.py::_run_copy` exists for exactly this and CLAUDE.md already
measures the shape: **~1,080ms paged vs ~89ms through one COPY.**

Targets: `_airs_portfolio_perf._fx` and `_benchmark_index._fx_to_eur`.
⚠ **Keep the window semantics exactly** — an FX read that loses a currency does not blank a cell,
it makes a fully-priced holding *silently leave the portfolio* (`project_postgrest_max_rows_trap`:
TWC cut to 20 rows dropped Taiwan Semiconductor from its own book, 49 of 56 models changed). A COPY
has no row cap, so it is strictly safer than paging — but only if the currency set and date floor
are computed the same way.

### 2. A per-request whole-table loader for the small reference tables (~25 → ~3)

The dominant pattern, and the memo cannot fix it: **tiny tables read many times, each caller asking
for a different `select=` list**, so no two URLs match.

| table | rows in table | round trips | distinct select-lists |
|---|---|---|---|
| `airs_model_portfolio` | **102** | 8 | 8 |
| `airs_model_portfolio_position` | 982 | 7 | 5 |
| `airs_holding` | ~200 for a book | 10 | 4 |

⚠ **At 60ms a round trip, ONE unfiltered read of a 102-row table beats eight filtered ones by an
order of magnitude** — the filter is saving bytes that were never the cost. Add
`routers/_airs_ref.py` exposing `models()`, `positions()`, `holdings(portefeuille)` that each read
the **superset of columns once**, memoized in the existing `read_cache`, and filter in Python. No
new caching semantics: it dies with the request like everything else.

### 3. Unify the `asset_grid` select-lists (11 → ~3)

Three distinct column lists over chunked `isin=in.(...)` batches. Asking every caller for one
superset list makes identical chunks collapse in the memo.

### 4. `_year_perf` server-side (page load: 3 → 1, −1,428 rows)

`airs_performance` is 3 round trips and 1,473 rows to produce **45 numbers**. The
freshest-row-per-month dedupe + year aggregation could be a view/RPC returning 45 rows in one call.
⚠ **The dedupe rule must move into SQL exactly** — a period is identified by its opening capital,
and the daily refresh writes a new row per month per run (BUS_Offensief_Dyn holds 20 rows for 7
months). Summing them counts June seven times. That trap is documented at length in `_year_perf`'s
docstring; port it verbatim or not at all.

### Expected result

Page load **12 → ~8**; Analyse modal **92 → ~30**. At 60ms/round trip the modal's network cost goes
~5.5s → ~1.8s. ⚠ None of this touches the frontend, and none of it changes a number — every item is
the same data fetched fewer times. Re-measure after each with the inner-client instrumentation.

---

## 🚂 Railway migration — plan written, nothing started

Full plan: [`docs/railway-migration.md`](docs/railway-migration.md). The headline, because it is
the thing that gets assumed wrong: **"move to Railway" is not "move Postgres"** — 169 backend files
speak **PostgREST**, 3 buckets speak **Storage**, and frontend+backend speak **GoTrue**. Moving
only Postgres means rewriting all three. The plan runs Supabase's OSS images on Railway instead, so
`SUPABASE_URL` repoints and application code does not change.

Verified feasible against the repo: **zero `CREATE EXTENSION` across 109 migrations**, no
pg_cron/pg_net/vault, no Realtime, no Edge Functions, and the **frontend uses supabase-js for auth
only** (26 calls, all `auth.*`, zero `.from()`/`.rpc()`/`.storage`).

Three things that are genuinely new work, not config: a **path-routing gateway** (supabase-py/js
derive `/rest/v1` `/auth/v1` `/storage/v1` from ONE base URL, Railway routes by domain — Caddy,
~20 lines); **new JWT keys** (current anon/service keys are signed with the hosted project's
secret and cannot be carried over — everyone gets logged out once, passwords are unaffected); and
**Storage needs a home** (Railway has no object store — R2/B2, or a volume you then must back up).

⚠ **Backups become your job**, and that is the item most likely to be deferred until it matters.
⚠ **Today's outage was NOT Supabase's fault** — an aborted 60M-row transfer behaves the same
anywhere — so migrating *for reliability* would be solving the wrong problem. The real wins are
co-location (~40 ms per round trip today) and staging. Price Supabase Pro + a second project as
staging before committing; it buys most of both in days rather than weeks.

---

## 🚑 2026-08-11 PROD OUTAGE — recovered. Two maintenance passes still owed.

An aborted clone left a server-side `INSERT` running for 46 minutes; it filled the volume and
exhausted the connection pool, and every `/api/*` returned 500 `ECHECKOUTTIMEOUT`. **The database
was never damaged — it was busy on a statement nobody was waiting for.** Recovery was
`pg_cancel_backend(1094454)` → `DROP SCHEMA IF EXISTS clone_stg CASCADE;` → let autovacuum run.

State at the end of the session — **prod is serving**: 26.8 GB + 1.9 GB WAL of 40 GB (72%),
`default_transaction_read_only = off`, 9–11 connections, `clone_stg` gone, autovacuum working
through 4,027,178 dead tuples in `metric_data`. The script fixes are in
[clone-local-to-prod](#-clone-local-to-prod--fk-cascade-fixed-2026-08-11-the-dry-run-still-cannot-see-it).

### ⚠ THE AFTERMATH LOOKS LIKE FOUR DIFFERENT BUGS. IT IS ONE. (measured 2026-08-11)

The aborted clone left **4,027,178 dead tuples** and re-bloated `metric_data`'s indexes from the
3.7 GB the morning's reindex achieved back to **14.8 GB** (`idx_metric_data_metric_source_company_date`
7,638 MB + `metric_data_pkey` 6,723 MB + 454 MB). autovacuum must walk all of it, which saturates
the instance's disk. **Latency then becomes a coin flip, and that is the whole difficulty**: the
*same* 1,815-row read, six times in a row, measured

    60,000ms (timeout) · 60,000ms (timeout) · 11,400ms · 12,047ms · 1,041ms · 1,364ms

so one probe "proves" whatever you already believed. Every symptom below is that one cause:

- **401 Unauthorized** — GoTrue's own DB lookup timed out and `verify_token` swallowed it (fixed:
  now a 503, see `AuthBackendUnavailable`).
- **`httpcore.ReadTimeout` in `_year_perf`** — `deps._CachingSession` retries twice; both attempts
  exceeded the timeout, so the raw error escaped. The retry is not broken, it was *exhausted*.
- **500 after 13,671 ms** on `/analysis` — same contention, different endpoint.
- **`OPTIONS … 200 OK` beside it** — a red herring. OPTIONS is the CORS preflight; it never reaches
  the DB, the auth gate or the handler, so a 200 there says nothing about health.

⚠ **DO NOT "FIX" THIS BY FORCING THE VACUUM.** It is `IO/DataFileRead`, never blocked
(`pg_blocking_pids` empty across repeated samples) — it is disk-bound, not stuck. And the throttle
is NOT the lever: `autovacuum_vacuum_cost_delay = 2ms` per 200 cost units (~100 page misses), and
100 random 8 KB reads cost far more than 2 ms here, so unthrottling buys ~10-20%, not 10x.
Cancelling it is worse — the dead tuples stay and autovacuum simply restarts. **It has to finish.**
Reloading the dashboard while you wait actively hurts: each load piles on concurrent connections
(16+ observed) competing for the same disk.

⚠⚠ **`index_vacuum_count` IS NOT A PROGRESS BAR, AND READING IT AS ONE COSTS AN HOUR OF "IS IT
STUCK?".** It counts COMPLETED PASSES OVER ALL INDEXES, so it reads `0` for the entire run of a
normal vacuum and then jumps to `1` at the end. `heap_blks_scanned` is no better — it pins at
`901471/901471` the instant the heap phase ends and never moves through the whole index phase. Both
are indistinguishable from a hung process, and `prod-health.ps1` originally printed exactly those
two. **The column that advances is `indexes_processed`/`indexes_total`** (PG17+; prod is **17.6**),
which read **1/3** while the other two showed nothing — it was working the whole time. Two further
non-proofs learned the hard way: `pg_statio_all_indexes.idx_blks_read` and
`pg_stat_database.blks_read` both showed **+0 over 60s**, because a long-running vacuum does not
flush its stats until it finishes; and `wait_event` sampled repeatedly shows
`IO/DataFileRead → LWLock/WALWrite → IO/WalSync → RUNNING` cycling, which IS the proof of life.
`dead_tuple_bytes` 2.7 MB of a 64 MB budget ⇒ one pass only, no looping.

**Watch it with `./scripts/prod-health.ps1 -Watch`** instead of reloading the app — read-only, and
it samples latency N times precisely because a single timing is meaningless at this spread.
⚠ That script hit **the scalar-unroll trap on its first run** (`db 2 + WAL 1 … read_only=o`, `dead
tuples: 4` — first characters of "27 GB", "1840 MB", "off", "4,027,178"): a PowerShell function
returning a one-element array hands back a bare string. `@()` must be re-applied at the **call
site**; `return @(...)` inside the function is not enough. Fourth incident of this trap in this repo.

**Still owed, in this order, once autovacuum has finished:**

1. `./scripts/prod-reclaim-disk.ps1 -Reindex -Apply` — the five committed clone batches built their
   indexes **by insertion**, which bloats them. The same pass earlier today took `metric_data`'s
   indexes from ~10 GB to 3.7 GB (covering index 5,999 → 1,973 MB; pkeys 4,088 → 1,699 and
   2,413 → 1,188), so this is worth several GB and is the cheapest headroom available.
2. `./scripts/prod-reclaim-disk.ps1 -Vacuum -Apply` — reclaims the dead tuples autovacuum marks.
3. Only then re-run the clone. It resumes; ~274 companies remain.

⚠ **Do not "fix" the half-applied clone** — see item 3 in the clone section. ⚠ **`VACUUM` /
`REINDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY` cannot run inside `psql -c "a; b; c"`** (one
transaction) — the scripts feed them over stdin (`-f -`) for exactly this reason.

⚠ **I told you to drop `idx_metric_data_source_date` and that was wrong** — 30 lifetime scans made
it look dead, but it is load-bearing for `get_distinct_dates` / `GET /api/longequity/snapshots`.
It is recorded in `scripts/dropped-indexes.sql`; recreate it if that endpoint is slow.

---

## ✅ 2026-08-11: a transient stall no longer 500s a page (`deps._CachingSession._send`)

`httpcore.ReadTimeout` on `GET /api/airs/portfolios/overview`, out of `_year_perf`. ⚠ **NOT a slow
query** — that read is `airs_performance`, **1,815 rows / 608 kB / 53 accounts, two pages** — so
optimising it would have fixed nothing. One stall against the 30s PostgREST timeout, on a client
with no retry, took the page down. `deps`' own comment already described the intent that was never
implemented ("read endpoints catch + return empty … degrades gracefully rather than wedging").

GET/HEAD now retry once on a transport fault. ⚠⚠ **Writes never replay** — a timed-out POST may
already have been applied, and the timeout describes the missing RESPONSE, not the write. Pinned by
`tests/test_postgrest_retry.py`.

**Not done, deliberately:** making the overview DEGRADE (render without the perf columns) when both
attempts fail. For financial figures a blank that isn't labelled is worse than an error, and
labelling it is a UI change worth deciding on its own.

## 🐌 /management-dashboard speed — one N+1 fixed, the ranking is measured, more is left.

Measured 2026-08-11 (local; ⚠ **round trips are the number that matters** — local PostgREST is
~5ms, production is a ~60ms network hop, so a call count predicts production far better than a
local stopwatch):

```
load                                   trips  local ms      after
overview (page load)                      17     1,109   -> 277ms  (read_cache on the endpoint)
performance (the grid's columns)          93     6,417   -> 37 trips / 3,669ms
benchmark SP500 (tab 3)                   14     1,704   -> 433ms  (read_cache; tab fires 3)
one account's holdings (row expand)        2        25    fine
correlations (tab 2)                      ?         ?    NOT MEASURED — probe used a wrong name
```

**✅ Fixed: an N+1 in `_prepend_opening_bars`.** Its docstring said the missing-opening-bar case is
"normally none"; measured, **71 series lacked it**, each fetched by its own
`analysis_id=eq.N … limit 1` round trip — ~4.3 seconds of pure latency in production, one row at a
time. All share ONE anchor, so they collapse into a single `DISTINCT ON (analysis_id)` COPY.
Verified byte-identical to the loop across three anchors (95/96/83 series filled), with the
per-series path kept as the no-COPY fallback because that bar is a CORRECTNESS input.

**Still open, in order:**
1. **`performance` is still 37 trips / 3.7s**, now dominated by `COPY x16` — the price load looks
   per-portfolio rather than once for the fleet. Batching that is the next real win.
2. **Correlations is unmeasured.** `compute_correlations_async` is not the export name; find it and
   profile before assuming it is cheap — it is an N×N over every portfolio.
3. **The memo is opt-in per endpoint** and now covers analysis, basket-analysis, overview and the
   benchmark index. `/api/airs/model-portfolios/performance` and the account routes do not have it.

---

## 📅 Smart fundamentals refresh — pieces 1 & 2 built, piece 3 (the button) is next.

**1. The detector** (`ingest/earnings/due.py` + `tests/test_fundamentals_due.py`). `period_due()`
projects the next fiscal period from a company's OWN cadence (3/6/12 months, median-inferred) and
reports it due `MIN_PUBLICATION_LAG_DAYS` (25) after it ends. Pure, clock injected.

**2. The shared fill** (`routers/_fundamental_fill.py`). The index job's body moved here so the
portfolio button reuses it rather than copying it; `POST /api/airs/model-portfolios/{id}/
fundamentals/ingest/job?force=true&only_due=true` is the second entry point.

**3. The button** — `PortfolioFundamentalsRefresh`, in **`OwnerEarningsModal`** (the tabbed
"Fundamental" modal: Long Equity · Quick Valuation · Deep Valuation · Old charts), right-aligned on
the tab row immediately left of the SBC-correction tickbox. Progress is the generic job toast, so
it outlives the dialog and carries the quota spend and a Cancel.

⚠ `FundamentalsModal.tsx` IS A DIFFERENT MODAL — the four-chart one opened from the positions
table's Soundness column. It has no tabs and no SBC box. The button went in the wrong one first;
if a second entry point is ever wanted there, the component is standalone and takes `{portfolio,
onDone}`.

⚠ AND THE SCOPE PROP IS **NOT** `portfolioId`. That existing prop means "show a whole portfolio as
an aggregate" and drives `isAgg`, which decides the tab set — reusing it for provenance would
silently strip Quick and Deep Valuation from the modal. `refreshPortfolio` is separate, and absent
for an ad-hoc basket, which has no book to refresh by.

**⚠ THE `n of m` IS TWO DIFFERENT ABSENCES AND THE BUTTON SAYS WHICH.** Measured after the fix:

```
AITopSelectie OFF FX : 20 of 20 have company fundamentals · would fetch 15
BUS_Neutraal_FX      : 24 of 40 · 11 are funds/bonds/cash (none exist)
                                · 5 have NO COMPANY RECORD  <- the real gap
                                · would fetch 22
```

**✅ Taiwan Semiconductor is no longer the gap it was.** `canonical_map` (ISIN aliases) already
had the ADR→home-line mapping; the endpoint's raw `company.isin` lookup was simply not using it,
which is why the first measurement said 19 of 20. `_fundamental_coverage` resolves aliases first
for the same reason — the two now agree about what "reachable" means.

**⚠ Still open: write amplification.** Each refetch upserts ~24,000 `metric_data` rows of which
~258 are the new quarter. Fine for one portfolio; a habit across 56 of them is the dead-tuple
pattern that filled prod's disk on 2026-08-11. The fix is the same `IS DISTINCT FROM` guard the
clone got — deliberately kept out of this feature so it can land on its own.

---

## ⚡ Analyse modal — memoized. The remaining cost is REAL work, and one number is unverified.

2026-08-11. Profiled one press of Analyse on BUS_Neutraal_FX: **212 database round trips, 103 of
them byte-identical repeats** (`airs_performance` x9, `airs_model_portfolio` x5, `asset_grid` x3,
the SP500 universe id x6). Nobody wrote that loop — a dozen collaborating loaders each correctly
fetch what they need, and the duplication exists only in their composition. Fixed with a
**per-request read memo** (`common/read_cache.py`, opened at the request boundary in
`compute_portfolio_analysis_async`), covering both transports: PostgREST GETs and direct COPY.
Measured: **212 → 109 round trips, 0 repeats left**, payloads equal to the uncached ones within
1e-9 across 6 portfolios. Pinned by `tests/test_read_cache.py`.

**⚠ 1. THE PRODUCTION GAIN IS ESTIMATED, NOT MEASURED.** Local wall went 3,436 → 2,949 ms
(interleaved medians, 14%) — but a local PostgREST call is ~5ms and prod is a network hop at
~50-80ms, so the 103 removed round trips are worth *seconds* there rather than the ~0.5s here.
Nobody has timed it against prod. The modal already logs `timings_ms` per phase to the console;
read that on a real load before quoting a number.

**⚠ 2. THE PAYLOAD IS NOT BYTE-REPRODUCIBLE, AND IT WASN'T BEFORE EITHER.** Two consecutive
UNCACHED runs of portfolio 1878 differ in the last ULP (`benchmark_pct` 31.472360860646393 vs
...386) — a float sum over rows Postgres returns in an unspecified order. Harmless at 2 dp, but it
means any future equality check here needs a tolerance, and an `ORDER BY` on those reads would be
the real fix.

**What is left, in order:** the phases now sum to the wall clock
(`composition_and_benchmark` 351ms · `book_holdings` 897 · `axes` 765 · `returns_and_benchmark`
1,903). The remaining 109 trips are distinct queries doing real work; the biggest single item is
**8 `asset_price` COPY loads (~1.4s)** over OVERLAPPING id sets and windows — not duplicates, so
the memo cannot touch them. Merging them into one load sliced per consumer is the next real win,
and it is a structural change across `_book_port_items` / `_basis_axes` / `_returns` /
`_benchmark_index`.

**Not done:** the memo is opt-in per endpoint. `/attribution`, the portfolios grid and the
benchmark endpoints have the same shape and would benefit; each needs its own `read_cache()` at
its request boundary.

---

## 🧬 clone-local-to-prod — FK cascade fixed 2026-08-11. The DRY RUN still cannot see it.

The clone died 47 tables in on `company` → `metric_data_company_id_fkey`. Step [5]'s comment
claimed "FK ON DELETE CASCADE/SET NULL cleans their dependents"; **8 edges in this schema are
`NO ACTION`** and do not (`company` ← metric_data / portfolio_weight / earnings_portfolio_member,
`currency` ← fx_rate + gurufocus_exchange, `country` ← gurufocus_exchange,
`gurufocus_exchange` ← company, `portfolio` ← portfolio_weight). Both delete sites now stage the
doomed PKs and walk the blocking edges depth-first (`Remove-RowsWithDependents`), sparing any
parent an **additive** table still points at. Verified by planning all 47 generated statements
against the local catalogue with `EXPLAIN` — nothing executed.

**⚠ 1. NOT YET RUN AGAINST PROD.** Every statement is planned-valid and the walk is proven on the
real FK graph, but no clone has completed with it. Next run: `./scripts/clone-local-to-prod.ps1`.

**⚠ 2. `-DryRun` DOES NOT REPORT THE CASCADE.** It compares row counts per table, so it says
"company: prod has 3 more" and nothing about the ~30k `metric_data` rows that go with them. That
is exactly the number worth seeing before pressing go. It also cannot predict a **spare** (a
parent kept because an additive child references it), which is the one case where the final
verify legitimately shows a surplus.

**⚠ 3. THE CLONE IS HALF-APPLIED ON PROD AND THAT IS SAFE — DO NOT "REPAIR" IT.** The 2026-08-11
run committed 5 of 6 `metric_data` batches before it was aborted. The diff sync re-copies a
company's **entire** row set when its signature differs, so every company it reached matches local
exactly and every one it didn't still holds prod's old data: each company is internally consistent,
just from a different vintage. Re-running resumes — the signature scan skips the ~1,500 already
done and copies only the remainder. There is nothing to undo.

### Fixed 2026-08-11 after the outage (both proven, neither exercised by a full run yet)

**Aborting the script no longer strands a server-side statement.** Ctrl-C killed only the local
psql client; the `INSERT INTO public.metric_data` ran **46 more minutes**, filled the volume and
exhausted the pool, and every `/api/*` returned 500 `ECHECKOUTTIMEOUT`. Postgres only notices a
vanished client when it next tries to *write* to it, and an `INSERT … SELECT` writes nothing until
it finishes; TCP keepalives don't help because the peer the server sees is the **pooler**. Now every
prod session stamps `application_name` and `Stop-CloneBackends` cancels anything wearing the stamp.
Proven end to end: a stamped statement was started, its client container killed outright, the server
confirmed *still running it*, and the canceller cleared it.

- **It runs at STARTUP, not only in the `finally`.** A `finally` needs this process to still be
  alive — exactly what's false when the window was closed or the machine slept. Startup-cancel makes
  the *next* run the recovery mechanism. ⚠ **Never call it mid-run**: step [5]'s concurrent jobs make
  a second stamped backend a sibling, not an orphan.
- **In the `finally` it runs BEFORE the `clone_stg` drop**, or `DROP … CASCADE` blocks on the
  orphan's locks and cleanup becomes another hang.

**⚠ THE STAMP HAD TO BE APPLIED IN SQL — SUPAVISOR OVERWRITES THE STARTUP PACKET.** Measured:
`PGAPPNAME=clone-local-to-prod` reads back as literally **`Supavisor`**, so the first version of
this fix would have matched zero rows — a cleanup reporting success having done nothing. Same for
`PGOPTIONS`: `statement_timeout` read **`2min`**, not 0, which means the inline
`SET statement_timeout = 0;` on the bulk statements is not a fallback, it is *the* mechanism —
don't remove it from a long statement believing the environment handles it. And `psql -q` is now
load-bearing: without it psql echoes a `SET` command tag as a **result row**, and these results are
read positionally (`[int](Invoke-Prod "SELECT count(*)…")`), so every count would shift by one row
while still looking like data.

**A disk preflight refuses to start into a full volume.** `default_transaction_read_only = on` is a
hard refusal (no `-Force`); a projected peak over 90% of `-ProdDiskGB` warns and needs `-Force`.
⚠ **`-ProdDiskGB` defaults to 40 and must be updated when the volume is resized** — Postgres cannot
see its own filesystem, so this is the one number that can't be discovered. ⚠ **WAL is measured
separately because `pg_database_size` excludes it** while it sits on the same volume (2.3 GB during
this run). Measured: proceeds at today's 26.8 GB + 1.9 GB WAL of 40 GB (81% projected peak) and
would have **refused** the run that caused the outage (162% of the then-20 GB disk).

**Still open here:** `-DryRun` doesn't exercise either new guard, and `airs_holding` still has no
natural unique key — the scraper writes duplicates within a single run (proven on local, which has
never been cloned to), independently of the clone's PK-keyed upsert.

---

## ⏱ Benchmarks Refresh — the speed-up is DERIVED, not measured. And one bounded slice is left.

Built 2026-08-11 (`/management-dashboard` → Benchmarks → **Refresh**). The button now fills an
index end to end: prices for every constituent, then a **forced** GuruFocus refetch of every
constituent's fundamentals. Force had to defeat **two** caches — the `metric_data` sentinel (a
company loaded once was never selected again) and the Storage blob (`is_cache_fresh` keeps it
fresh for the data's own cadence + 50%, i.e. weeks past the quarter it is missing). Defeating one
gives a press that spends nothing and changes nothing. Pinned by `tests/test_fundamental_refetch.py`.

The price step then went from a serial loop with a hardcoded `time.sleep(0.4)` onto a pool
(`_PRICE_WORKERS`, 2× `YAHOO_CONCURRENCY`, capped at 8), the "what did we hold before?" read was
hoisted into one grouped COPY (`latest_close_by_analysis`), and the cap WRITES were parallelised
(the quotes were always batched at 100; storing them was ~490 serial round trips for the S&P).
Pinned by `tests/test_benchmark_price_pool.py`.

**⚠ 1. NOBODY HAS TIMED IT.** The "S&P ~10–15 min → ~3–4" figure is arithmetic off the pacing
constants (`YAHOO_RPS` 10/s, semaphore 4, the removed 0.4s sleep, ~4 round trips per constituent),
not a stopwatch. Time one AEX run (25 constituents, ~1 min, cheap) and one SP500 run before
quoting it to anyone. The interesting question is whether the governor or our database is now the
limit — if `extend_series`' COPY is, more workers will not help.

**⚠ 2. THE RESOLVE SLICE IS STILL 25 PER PRESS** (`_benchmark_fill._RESOLVE_PER_PRESS`), so a
fresh or heavily-unresolved index still reports "N still unresolved — press again". Deliberately
not widened: that path is `resolve()`, where an overloaded Yahoo returns an EMPTY search rather
than a 429 and the thin foreign listing wins (NVDA-on-Stuttgart, Alphabet-on-Vienna). The price
pool is safe **only** because `extend_series` asks about a symbol we already identified. If this
is widened, loop `process_slice` over this benchmark's ISINs — never a second resolver.

**Offered, not built:** the caps step still writes one row per constituent. A PostgREST `upsert`
of the batch was rejected on purpose — it becomes INSERT … ON CONFLICT, so a constituent with no
`asset_analysis` row would be CREATED from four cap columns, a junk row that then looks like an
instrument.

### ⚠⚠ AIRS DUPLICATES HAVE **TWO** CAUSES, AND ONLY ONE IS THE CLONE (2026-08-11)

**1. The SCRAPER writes duplicates inside a single run.** Proven on LOCAL, which has never been
cloned *to*: 24 pairs in `airs_holding` with consecutive ids, identical `retrieved_at`, identical
ISIN/quantity/value (Apple 91 @ 27,225.55 twice, ASML 16 @ 22,214.40 twice, all
`BUS_MTS_NEU_AFS_DYN` 2026-07-28). **Root fix, not yet done:** `airs_holding` has no natural
unique key, so the writer inserts where it should upsert. Needs a migration adding
UNIQUE (portefeuille, as_of_date, holding_name, quantity, current_value_eur) — or the
delete-then-insert-per-(book, date) pattern `airs_model_portfolio_position` already uses.

**2. The clone upserted surrogate-PK tables BY id.** Six AIRS tables key on a serial `id` that each
side assigns independently, so `ON CONFLICT (id)` overwrote prod rows with unrelated local ones AND
inserted copies beside rows prod already had. **Fixed:** `$skipTables` — those six are now neither
staged, upserted nor deleted (prod authors them; local's copies are dev artifacts). Two of them
(`airs_model_portfolio_link`, `airs_account_model_link`) *do* have a natural key, but as an
EXPRESSION unique index, which `information_schema.table_constraints` cannot see — so the script's
unique pre-clear was blind to them anyway.

**⚠ Quantify prod with `scripts/airs-duplicates.sql`** (read-only). ⚠ Its `airs_holding` key
includes `quantity` + `current_value_eur` on purpose: (portefeuille, as_of_date, holding_name)
alone over-reports 83 groups on local, because a bond and its accrued-interest line share a display
name ("6,5% Rabobank Certificaten 14-perp." at EUR 8,347.20 and EUR 112.23, same scrape).

### ⚠ 2026-08-11 INCIDENT: the failed clone filled prod's disk and put it in READ-ONLY mode.

Two defects, both now fixed, both of which only bite on a run that DIES:

1. **`clone_stg` was dropped on the success path only.** The FK failure in step [5] left a full
   copy of every staged table on prod (~500MB; `universe_membership` alone is 444MB), and each
   retry left another. Now dropped in a `finally`.
2. **The staging upsert rewrote EVERY row of EVERY table, every run.** Postgres is MVCC, so
   `SET x = x` still writes a new tuple, kills the old one and WAL-logs it — the exact
   "disk-filling event" the metric_data lane has guarded against since it was written. Step [5]
   now carries the same `IS DISTINCT FROM` guard (type-checked against all 55 staged tables).

Together those crossed 95% of disk → Supabase read-only → four disk expansions inside 24h → the
disk-modification limit, with a ~4h cooldown.

**⚠ STILL OPEN: `universe_membership` is 8,444 rows in 444MB, and `last_autovacuum` is NULL** (on
LOCAL — prod is likely worse, it runs the pipeline daily). That is ~55KB per row; the table is
almost entirely dead tuples from repeated rewrites. `VACUUM FULL public.universe_membership` takes
seconds on 8k rows and needs only its own size free. Worth doing on both sides, and worth finding
out why autovacuum never ran on it.

**⚠ NEVER `VACUUM FULL` metric_data on prod** — it rewrites the table and needs that much free
space. Plain `VACUUM` only.

**Measured on prod 2026-08-11** (via the new `scripts/prod-reclaim-disk.ps1`, which reports by
default and never picks what to drop):

```
DB 20 GB, of which metric_data 13 GB (heap 2,729 MB + indexes 10 GB) and asset_price 6,693 MB.
No replication slots. clone_stg confirmed gone (22 GB -> 20 GB when it was dropped).

metric_data_pkey                            4,088 MB   37,966,199 scans  (99.5%)
idx_metric_data_metric_source_company_date  5,999 MB      189,364 scans  ( 0.5%)  <- USED, keep
asset_price_pkey                            2,413 MB      915,799 scans
idx_metric_data_source_date                   471 MB           30 scans  <- DROPPED 2026-08-11
```

**Still open:** ~3.3 GB of estimated index bloat on `metric_data` (prod runs 3.33 GB of index per
GB of heap; local runs 2.54 — scaled by heap, prod's indexes should be ~7.5 GB, not 10). `-Reindex`
reclaims it but BUILDS THE NEW INDEX FIRST, so the 4 GB pkey needs ~4 GB free — not available at
95% full. Do it after the disk has headroom, not during an incident.

**Also open:** `last_autovacuum` is NULL on both big tables and prod reported `n_live_tup 25,413`
for asset_price (off by orders of magnitude). Autovacuum sizes its thresholds off that estimate,
so a wrong one is self-perpetuating — `-Vacuum -Apply` (VACUUM ANALYZE) once healthy.

---

## 📋 AIRS Transacties — measured on ONE account. Two things still unverified.

Built 2026-08-05: `/portfolios` → expand an account → **Transactions** (the sheet, cached in
`airs_transactie_snapshot`) and **Total return** (the year, built from held + sold positions and
checked against AIRS's own `beleggingsresultaat`). `TRANS` is confirmed to be Transacties; the
columns are documented in `airs_transacties`'s docstring from a real download.

Measured on AITopSelectie OFF DYN: held 380,986.94 + realised 6,306.85 + sold-name income 0.00 =
**387,293.79** against the book's **387,293.75** — residual **€0.04** — and 38.729379% against
AIRS's own 38.729375%.

**✅ 1. `Res. YtD` vs `proceeds − Kostprijs` — RESOLVED, on real data (BUS_Offensief_Dyn).** The
first book measured (AITopSelectie) had `Res. voorg. jr.` = 0.00 on every row, so the two formulas
agreed exactly and it could not arbitrate. Bustelberg Offensief settles it — 12 of its 13 sold
names carry prior-year amounts (Novo Nordisk −24,866.94, Wolters Kluwer −20,819.13):

```
Res. YtD                 -28,656.47   -> total +69,792.94  =  +5.83%   AIRS: +5.83%  ✓
Res. voorg. jr.          -97,919.73
proceeds - Kostprijs    -126,576.20   -> total -28,126.79  =  -2.35%              ✗
identity: proceeds - cost == Res. YtD + Res. voorg. jr., to -0.00
```

The intuitive formula is **8pp out and the wrong sign**, and looks entirely plausible. Keep
`Res. YtD`. (The book's own YTD moves as it is re-scanned — these are 2026-08-05 mid-session; the
identity and the size of the error are what matter, not the exact totals.)

**⚠ 2. `Tt='D'` is uninterpreted.** One row: KLA-Tencor, 2026-06-12, 369 shares, every money column
`0.0`. KLA split 9:1 in 2026, so a corporate action is the obvious reading — and obvious is not
measured. It carries no money, so it is excluded from every total and **counted** (surfaced in the
panel). If a `D` ever arrives carrying a value, that count is what will show it.

**✅ The merged position ledger is built** (`airs_capital.py`, Analyse modal → "Every position this
year"). One row per instrument the book touched, held or sold, weighted by **average invested
capital** (Modified Dietz) — the only weight a sold position can carry, and the only one that
describes a book whose composition changed. Contributions sit on `beginvermogen` and sum to the
book's own YTD **exactly** (measured: 5.8267 vs 5.8267, 44.4624 vs 44.4624; residual 0.0000pp).

⚠ A 1-January weight was tried first and is WRONG: AITopSelectie's equities were worth EUR 40,319
on 1 Jan against a EUR 1m opening capital, because it began the year in cash and deployed on
5 January — a start-weighted table calls it 96% cash.

**Still approximate, and surfaced rather than hidden** (`capital_coverage_ratio`, measured
0.980 / 1.023):
- Modified Dietz ignores the price path *within* a position.
- A sold-out parcel's opening value is split proportionally by quantity between shares held at the
  open and shares bought during the year — AIRS does not publish its parcel matching.
- The de-restatement assumes `Beginwaarde ÷ quantity` is the 1-Jan price (linear restatement).

**Not yet done:**
- `start_gap_eur` in the /portfolios reconciliation is still the NET of two opposite effects. The
  ledger now separates them; that panel could read the ledger instead.
- A sold position has no sector, so it is absent from the composition bars and Brinson. Those
  report `realised_share_of_result_pct` instead. Classifying by name → `asset_execution` would
  close it, but a name match is exactly what `_airs_holding_isin` warns against.
- `kosten` is 0 on all 53 accounts, so whether costs sit inside `cumulatief_rendement` but outside
  `beleggingsresultaat` is untested. If a book ever charges them, the residual is where it shows.

---

## 🐞 GuruFocus RENAMED the financials sections — `/earnings` metric codes are drifting

Found 2026-07-13 while building the /asset-pipeline Revenue column. GuruFocus's
`financials` blob changed shape, and our Storage cache now holds **both**:

```
live API today   annuals.income_statement.Revenue        annuals.per_share_data_array.*
cached blobs     annuals["Income Statement"].Revenue     annuals["Per Share Data"].*
```

`ingest/earnings/financials.py::_parse_financials` derives each `metric_code` from the
**section name it happens to see**, so a company re-fetched TODAY writes
`annuals__income_statement__Revenue`, while every constant and every stored row says
`annuals__Income Statement__Revenue`. Nothing errors — the new rows simply land under
codes nobody queries, and the /earnings dashboard shows a company as having no data.

Blast radius: `/earnings` (its whole metric_data contract), `ANNUAL_CODE` /
`QUARTERLY_CODE` in `routers/_asset_dividends.py`, and `has_data` in the dividend
coverage map. NOT the asset-pipeline Div/share or Revenue charts — both read the raw
blob and already accept **both** spellings (`_asset_revenue._SECTIONS`).

Fix: normalise the section names in `_parse_financials` (map snake → the legacy Title
Case codes, or migrate the codes and backfill). Do it before the next earnings refresh
re-fetches anything, or the two schemas interleave inside one company's history.

---

## ⚠️ SESSION 2026-07-10 — NOTHING IS COMMITTED. START HERE.

Branch `dev`, working tree dirty: **19 modified + 19 new paths** (incl. this file and
`CLAUDE.md`). All of it is green — `1032` backend tests, `149` frontend, ruff/tsc/eslint
clean, `openapi.json` + `lib/api-types.ts` regenerated. Review and commit before doing
anything else, or a `git checkout` will destroy a day's work.

Re-verify everything in one go:

```bash
cd backend && SUPABASE_DB_URL= SUPABASE_URL= SUPABASE_SERVICE_KEY= PYTHONPATH=. uv run python -m pytest tests/ -q
cd backend && uv run ruff check .
cd frontend && npx tsc --noEmit && npm test -- --run
cd backend && uv run python scripts/dump_openapi.py && cd ../frontend && npm run gen:types   # must be no-ops
```

### What was built (suggested commit split)

1. **Golden master for the legacy momentum engine** — `tests/test_golden_rebalance.py`
   (34 tests, offline) + `scripts/capture_golden_rebalance.py` +
   `tests/fixtures/golden_rebalance_34*.npz` (13.7 MB, 2 fixtures).
   Replays strategy 34 through `run_current_portfolio` against frozen inputs and asserts
   exact holdings. Mutation-tested: catches `mean()`→`median()` sector aggregation, which
   **all 863 other tests miss**. The second fixture (trading-day `as_of`) is the only one
   that can observe the strict `<` cutoff.
2. **Phase 1 — `timeseries/` façade** (`load_series`, `to_panel`) + `common/pg.py`.
   Four bespoke loaders now delegate. Verified byte-identical against a pre-change baseline.
3. **Phase 2 (partial) — `signal_engine/`** (`registry.py`, `daily.py`, `context.py`).
   One declaration of all 19 signals; `momentum/signals.py` 660→174 lines. AlphaLab's
   Signal Lab can now also score the live `daily_asof` battery (`?include_daily=true`).
4. **GuruFocus API catalogue** — `docs/gurufocus_api.md`, `backend/gurufocus_api.json`,
   `scripts/gurufocus_catalog.py`, `tests/test_gurufocus_catalog.py`.
5. **Div/share column on `/asset-pipeline`** — `routers/_asset_dividends.py`,
   `AssetDividendModal.tsx`, `tests/test_asset_dividends.py`. Native|EUR dual chart,
   three cadences (annual / quarterly / **payments**), lazy fetch on open.
6. **OpenFIGI name-anchor fix** — `asset_pipeline/resolve.py` + `scripts/repoint_primary_listing.py`
   + `scripts/reresolve_asset_mismaps.py` + `tests/test_asset_resolve_anchor.py`. **This one
   changes resolution behavior for the whole pipeline** — commit it separately and read the
   diff.

### Local DB was mutated (dev only, intentional)

- `asset_execution` 1633 (NVIDIA) repointed `NVD.SG` → `NVDA`. Prices refetched.
- `fx_rate`: USD backfilled to 1999-01-04 (596 → 7,045 rows) by opening Apple's dividend chart.
- `metric_data`: dividend rows for a few companies (whitelisted, ~100 rows each).
- `gurufocus-raw` Storage: new `dividend.json` / `financials.json` blobs.
- Throwaway auth users created and deleted; only `reinier7175@gmail.com` +
  `reinier@bustelberg.nl` remain.

---

## 1. Do next — the 302-row primary-listing sweep (HIGH VALUE, NOT RUN)

`scripts/repoint_primary_listing.py` is written, tested, dry-run reviewed, and **only
applied to NVIDIA**. 302 equities are still on thin cross-listings.

```bash
cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py           # review the diff
cd backend && PYTHONPATH=. uv run python scripts/repoint_primary_listing.py --apply   # ~2,100 Yahoo calls
```

- Detection is **ADV ÷ market cap** (`< 1e-5`; p5 is `1.07e-5`, median `4.6e-3`). NOT an
  exchange-code map (Vienna≠Prague trap) and NOT "ISIN country ≠ listing country" (that
  flags every deliberate ADR→ordinary mapping like `US7595091023`→`RELIANCE.NS`).
- Three gates before any rewrite: different symbol, `same_company()` identity holds,
  ≥2× liquidity gain. The gates work — Exxon's re-resolution proposed *another* Stuttgart
  line and was correctly rejected.
- [ ] Run the dry-run, read it, then `--apply`. **This rewrites the price panel under
      AlphaLab, Signal Lab and every saved universe.** Don't run it while a big ingest
      is competing for the Yahoo throttle.
- [ ] Then re-run `scripts/reresolve_asset_mismaps.py` (dry-run): its suspect filter was
      producing 636 suspects of which 555 were false; the real count is 81.

---

## 2. Engine unification — Phase 2 remainder, then Phase 3

Done: golden master (Phase 0), `timeseries/` façade (Phase 1), `signal_engine/` registry
+ `daily.py` port (Phase 2, most of it).

- [ ] **`asset_pipeline/signals.py` still has its own month-end battery.** It derives from
      the registry, but the two cadences remain separate implementations. Decide whether
      AlphaLab's 9 month-end signals stay, or collapse into the daily engine.
      **Do NOT merge `vol_trend_3m`** — measured spearman **0.58**, 29% sign disagreements,
      opposite signs on the same universe. `daily.mom_12_1` and `me.mom_12_1` ARE the same
      signal (spearman 0.996). Both facts encoded in `signal_engine.registry.PARITY` and
      regenerable via `scripts/signal_divergence.py`.
- [ ] **Phase 3 — `available_at` (knowledge date).** Neither engine has one, so any
      fundamental signal is look-ahead-biased by construction. Price: `available_at = obs_date`.
      Fundamentals: announcement date, or `period_end + lag`. This is the prerequisite for
      any EPS/valuation factor.
- [ ] Optional Phase 4: decide whether `/schedule` moves to Yahoo prices. **Different vendor
      ⇒ different holdings even with identical code.** Make it a deliberate, measured
      decision, never a side effect of a refactor.
- [ ] Cheap wins not yet taken: `alphalab.load_panel` loads FULL history (10.2s) and caches
      30 min, where a windowed load is **24× cheaper** (418ms). Signal compute is ~2% of a
      backtest run — never optimize it.

### Blind spots in the golden master (documented in its docstring)

- `MAX_STALENESS_DAYS` 30→10 is caught by **nothing**. Add a fixture where a company sits
  in the stale band.
- ETF overlay + cash sleeve aren't covered (strategy 34 has neither) — pinned only by
  `tests/test_portfolio_math.py`.

---

## 3. FX coverage — a latent backtest bug

`momentum/data/fx.py::load_fx_rates` does `.reindex(daily).ffill().bfill()`. The **bfill**
silently extends the earliest stored rate backwards to whatever `start_date` you ask for.
And `sync_fx_rates_to_db` only ever extends **forward** (reads stored max, fetches max+1) —
nothing in the codebase backfills earlier history.

Before this session, USD/CZK/GBP/JPY/CHF all started at **2024-03-07** (596 rows), while
ISK/THB/IDR reached back to 2000. A backtest starting 2015 with USD holdings would convert
every pre-2024 price at the 2024-03-07 rate.

- Not currently biting: scheduled strategies start 2025-05-02, and the backtest stream calls
  `sync_fx_rates_to_db` before loading. But `/backtest` lets you pick an earlier start.
- [ ] Backfill `fx_rate` for every actively-used currency (ECB history starts 1999-01-04).
      `routers/_asset_dividends.py::_backfill_fx_history` shows the pattern; it already fixed
      USD as a side effect.
- [ ] Consider making `load_fx_rates` return NaN before coverage instead of `bfill()`, and
      audit the callers.

---

## 4. Smaller follow-ups from this session

- [ ] **`docs/schema.md` is stale** — no `asset_*` tables, no `signal_engine`/`timeseries`.
- [ ] `ingest/earnings/financials.py` now takes `metric_codes=` (a whitelist). The earnings
      dashboard still persists **all 263 fields** — ~36,700 `metric_data` rows per company.
      Whitelisting it to `_DASHBOARD_METRIC_CODES` is a ~10× storage cut for zero behavior
      change. `metric_data` is 6.4 GB for 26M rows (2.3 GB heap, **4.1 GB index**) because
      the PK carries a `varchar metric_code`.
- [ ] **Andritz AG (`AT0000730007`) is stored on exchange `XPRA` (Prague)** — the Vienna≠Prague
      trap. Its dividends therefore render in **CZK**. `GET isin/AT0000730007` returns both
      `WBO:ANDR` (EUR) and `XPRA:ANDR` (CZK); `WBO` is correct. Fix at `company.exchange_id`.
- [ ] `/asset-pipeline` Div/share `Fetch` spends one GuruFocus call per company opened, with
      no per-day cap. Consider a cap or a bulk backfill if it gets used heavily.
- [ ] `signal-lab?include_daily=true` roughly doubles compute (31s → 110s on the 4,006-name
      universe). Defaults to `false`; the daily engine's per-entity loop is the bottleneck.
- [ ] `GET /api/asset-pipeline/dividends/{id}` **writes to `fx_rate`** as a side effect of a
      read (backfills the currency's history). Precedent exists (the backtest stream syncs FX
      before loading), but it's worth naming.

---
## 5. Per-company staleness flags — "stale price" / "stale volume"

**Goal:** every company whose latest price and/or volume data is non-recent
should be flagged **stale price** and/or **stale volume** (the two are
*independent* — a company can be fresh on one and stale on the other), instead
of one stuck name silently dragging a whole universe's "most-stale" line.

**Why (the MASI case, 2026-06-23):** Masimo (`MASI`, cid 5344 local) showed as
LEONTEQ's most-stale at 06-12 while everything else was 06-22. Verified against
the **live GuruFocus API**: `MASI` *and* `NAS:MASI` both return history ending
06-12 (last 179.95), and the `/summary` quote is also frozen (~06-13) — while
AAPL/MSFT/NVDA/TGB all return 06-22. So GuruFocus's **API** is frozen for this
one ticker even though their **website** shows newer prices. Our DB is correct;
we can't pull newer data. But one upstream-frozen stock makes the universe look
stale and makes "Refresh" look broken (it runs, re-fetches 06-12, nothing moves).

**What to build:**
- [ ] Compute per company the latest `target_date` for `close_price` and for
      `volume` (source `gurufocus`), compare to the **global latest** close/volume
      date (mirror the delisting sweep's "N trading days behind" idea in
      `ingest/delisting.py`, but **non-destructive**). Beyond the threshold →
      `stale_price_at` / `stale_volume_at` (two independent markers).
- [ ] Keep distinct from the existing markers: `delisted_at` (dead/acquired),
      `illiquid_at` (manual, rarely trades), `out_of_scope_at` (no GF coverage).
      These new flags are **automatic + per-metric** and mean "still listed &
      priceable but its GF feed has gone stale" (often upstream, e.g. MASI).
- [ ] Surface on `/companies` (a **STALE PRICE** / **STALE VOL** badge, threaded
      through `/api/companies` incl. the COPY path
      `momentum/data/_pg.py::load_companies_via_copy`) and on the `/schedule`
      coverage cards, so an upstream-frozen ticker reads as "stale feed", not
      "our refresh failed".
- [ ] Decide whether stale-flagged companies drop out of the universe
      "most-stale" coverage measure (like `illiquid_at` does) so a single frozen
      feed stops dominating the card. Auto-clear the flag once the feed catches up.
- [ ] (Optional) flag "GuruFocus API frozen for this ticker" specifically — when
      a fetch succeeds (200) but returns no rows newer than what we already have,
      distinct from a 404/ticker-drift (see §2).

---

## 6. Prod data fixes (do soon — local is fixed, prod isn't)

- [ ] **Push the illiquid migration to prod**: `npx supabase db push` →
      `supabase/migrations/20260615005000_company_illiquid.sql` (adds
      `company.illiquid_at`). The /schedule "Mark illiquid" button +
      `POST /api/admin/company-illiquid` + the price-coverage exclusion all need
      the column on prod.
- [ ] **Repoint Bank of New York Mellon ticker `BK → BNY` on prod.** GuruFocus
      renamed the listing (BNY Mellon rebrand). Locally fixed (cid 3327); on prod
      it's still `BK` → every refresh 404s AND the daily delisting sweep will
      **wrongly** mark it `delisted_at` (it's actively trading under BNY). Run the
      same `company.gurufocus_ticker` update against prod.
- [ ] **Re-apply the illiquid marks on prod**: Telecom Italia savings (MIL:TITR)
      via the button once deployed. Covestro (XTER:1COV) is already `delisted_at`
      on prod via the sweep (acquired late 2025) — optionally also mark illiquid.

## 7. Ticker-drift handling (systemic — BK exposed this class)

When GuruFocus renames a ticker, our stored `gurufocus_ticker` 404s, the company
silently stops updating (a 404 loads 0 rows, no error/flag), shows as the
coverage "oldest", and the DB-only delisting sweep can't tell it apart from a
real delisting.

- [ ] **"Fix ticker from GuruFocus" action** — when a company 404s, probe
      GuruFocus by name to find the renamed symbol and repoint
      `gurufocus_ticker` (one click, mirrors the existing "GF name" button →
      `POST /api/admin/gurufocus-company-name`). Surface on /companies and/or the
      /schedule coverage "Oldest" line.
- [ ] **Guard the delisting sweep** (`ingest/delisting.py`) — before stamping
      `delisted_at` on a stale company, probe its GuruFocus symbol; a
      renamed-but-trading listing should be flagged "needs ticker review"
      (`gurufocus_lookup_failed_at`?), not delisted.
- [ ] **Make 404s visible in the price refresh** — a 404 ("Stock not found") for
      a company that HAS prior data currently loads 0 rows silently. Add a
      counter/flag in `ingest/phases/prices.py` so ticker drift isn't invisible.

## 8. Offered UI follow-ups (not built)

- [ ] **/companies "ILLIQUID" badge** — surface `illiquid_at` companies (thread
      it through `/api/companies` incl. the COPY path
      `momentum/data/_pg.py::load_companies_via_copy`, + a badge like
      UNSUBSCRIBED/DUPE).
- [ ] **/schedule "Mark delisted" button** on the coverage **Oldest** line
      (sibling to the new "Mark illiquid" button) — for stale-but-dead listings.
- [ ] **Attribution matrix sector source** (/earnings portfolio mode) — default
      the "Sectors" dropdown to each basket's OWN membership sectors instead of
      always Leonteq (holdings outside Leonteq show "Unclassified").
- [ ] **Mobile-viewport e2e test** (375×667) — assert the nav drawer
      opens/closes + no horizontal overflow. Needs an auth stub so the sidebar
      renders under `E2E_BYPASS_AUTH` (the responsive/drawer work from this
      session is otherwise only structurally verified).

## 9. Data jobs

- [ ] **Re-run the market-cap backfill** — the earlier full run FAILED
      (~73% `market_cap_eur` / 82% `isin` coverage). `uv run python -u -m
      index_universe.backfill_market_cap` (also corrects names) when convenient.
      (The name-only backfill `index_universe.backfill_company_names` completed:
      0 renames — names already correct.)
- [ ] **Re-run the month-end full price refresh after fixing tickers** — the run
      this session showed "0 prices refreshed" partly because stale names like BK
      were 404ing (ticker drift). Re-run to confirm it pulls the long tail once
      tickers are corrected.

## 10. Verify before investing (possibly obsolete)

- [ ] **Earnings basket-aggregate caching** — earlier offer to precompute a
      per-frozen-universe aggregate to speed the ~66s `member-metrics` load.
      Likely **MOOT** after the /earnings single-switch redesign (stock mode =
      stock-vs-stock; portfolio mode = the Allocation×Selection matrix only,
      prices-only). Confirm nothing still triggers the heavy member-metrics
      aggregate before building this.

## 11. Long Equity benchmark performance (2026-08-06)

Context: selecting ACWI as the benchmark on the Long Equity tab fires ~11 requests
(one per card) over 1,514 constituents. All three items below are **shipped and
ruff/tsc/eslint clean**; what is missing is production measurement, not code.

- [ ] **Confirm `SUPABASE_DB_URL` is set on Railway.** Without it the new COPY path
      in `routers/_earnings_pg.py` is INERT and everything silently keeps using the
      PostgREST pager (`common.pg` logs one warning at startup saying so). This is
      the single highest-value check — the COPY win scales with round-trip latency,
      which is ~2ms locally and 50–200ms to Supabase cloud.
- [ ] **Re-benchmark the tab INTERLEAVED, not in blocks.** Local block-measurements
      contradicted each other (COPY+dedupe 6.04s vs dedupe-only 4.55s), which is the
      ~15% run-to-run spread CLAUDE.md already warns about. A 4-way interleaved
      benchmark (PostgREST / +dedupe / COPY / COPY+dedupe, ≥3 rounds) is the only way
      to get a trustworthy number. **Do this on prod, or at least with a warm DB** —
      the local figures understate COPY by design.

### What was built (all in place, no follow-up needed unless the above says otherwise)

1. **Response cache** — `routers/_blend_cache.py`, `@cached_blend` on 13 endpoints.
   Verified: 1.99s → 6.8ms on a repeat; portfolio/holdings requests are NEVER cached;
   `openapi.json` byte-identical; `invalidate()` wired to both fundamentals ingest jobs
   and fires only when data was actually written.
2. **COPY transport** — `routers/_earnings_pg.py::rows_by_company_via_copy`, tried first
   in `_rows_by_company` with the pager as fallback. Verified **identical output**
   (`dict == dict`, 1,512 companies / 16,336 rows) and 3.2× on the raw read locally.
3. **Metric-read dedupe** — `cached_metric_read` with THREAD-based single-flight (the
   reads run inside `asyncio.to_thread`). The tab issues 27 metric reads of which only
   18 are distinct (`sbc` ×5, `fcf` ×4, `revenue` ×3). 60s TTL, 32-entry cap: it exists
   to dedupe a concurrent burst, not to persist — persistence is the response cache's job.

### Measured dead ends — do NOT redo these

- **Collapsing the ~11 card requests into one endpoint is NOT worth it.** The only work
  all 13 endpoints share is `_load_and_expand_members` = **0.100s** (1.3s of 16.6s = 8%);
  the rest is each endpoint reading its own metric codes. And the cards already run
  concurrently (16.6s of work in 11.1s), so serialising them inside one handler would make
  cold wall-clock *worse* unless it re-implements the same fan-out internally.
  ⚠ An earlier claim that this was worth ~72% came from subtracting `_blend_inputs` (1.08s)
  from `*-inputs` endpoints that **never call it** — they call `_load_and_expand_members`.
- **Truncating a benchmark to the top 90% of cap does NOT work for these charts.** Measured
  on ACWI 2025: levels and ratios are 7–53% off (revenue sum −11%, net margin +11%,
  net income/share +53%), because the dropped tail is a systematically different business
  mix (EM/financials/industrials, lower margin), not "the same companies, smaller".
  Single-year cap-weighted *growth* IS accurate (−0.11pp ACWI, +0.60pp SP500), and a
  properly chained per-year version lands at +1.15% (SP500) / −5.42% (ACWI) over a decade —
  so it is only defensible for growth-based series, and only if labelled on the chart.

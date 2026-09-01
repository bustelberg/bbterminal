"""The two small AIRS reference tables, read ONCE per request instead of a dozen times.

THE PROBLEM, MEASURED ON THE ANALYSE MODAL (2026-08-11)

    `airs_model_portfolio` is a **102-row table**. One press of Analyse read it **8 times**, and
    `airs_model_portfolio_position` (982 rows) **7 times** — 3,523 rows over 15 round trips for
    two tables that fit in a single response together.

    ⚠ THE PER-REQUEST MEMO COULD NOT HELP, AND THE REASON IS THE WHOLE POINT OF THIS MODULE.
    `common/read_cache.py` keys on the exact request, and no two callers asked the same one:

        id,name,positions_datum            x3        portfolio_id,isin,percentage,fonds   x2
        id,name,display_name               x3        isin,fonds,percentage,datum,categorie x2
        id,name,omschrijving,positions_datum x2      portfolio_id,isin                    x1
        id,name,display_name,omschrijving  x2        isin,datum                           x1
        ... eleven distinct column lists              ... eleven distinct column lists

    Eleven correct modules each asking for precisely what they needed — and *that precision* is
    what defeated the memo. Nobody wrote a loop; the duplication is a property of the COMPOSITION,
    which is exactly the cost no single module can see or fix.

⚠ ASKING FOR FEWER COLUMNS SAVED BYTES THAT WERE NEVER THE COST. Production is in eu-west-3 and
    the backend is elsewhere, so a round trip is ~40-80ms of latency against a payload measured in
    single-digit kilobytes. **One unfiltered read of a 102-row table beats eight filtered ones by
    an order of magnitude.** The right unit to minimise here is requests, not rows.

HOW IT WORKS — NO NEW CACHING SEMANTICS

    Each function issues ONE canonical query: the SUPERSET of every column any caller wants, no
    filter, every row. Because the request is now byte-identical from every call site, the
    EXISTING `read_cache` memo collapses them to a single HTTP call for free. There is no second
    cache here, nothing with a TTL, and nothing that outlives the request.

⚠ THAT ALSO PRESERVES THE MUTATION SAFETY, WHICH A HAND-ROLLED ROW CACHE WOULD HAVE BROKEN. The
    memo stores the HTTP RESPONSE, and postgrest re-parses it into FRESH dicts for every caller —
    so a module that mutates a row it got back cannot corrupt the next module's copy. Caching
    parsed lists here would have quietly introduced exactly that hazard for 982 shared dicts.

⚠ FILTER IN PYTHON, DO NOT ADD A FILTERED VARIANT. A `positions_for(pid)` that pushed
    `portfolio_id=eq.N` to the server would be a *different* request again and would re-fragment
    the memo — reintroducing the bug this module exists to remove, one convenience at a time.

⚠ READS ONLY. The write paths (`_airs_portfolio_store`) are untouched and must stay that way: a
    write invalidates the memo by design (`read_cache.note_write`), and routing writes through a
    "loader" would blur which of the two a call site is.
"""
from __future__ import annotations

import json
import logging

from common import read_cache

# ⚠ `deps.supabase` IS RESOLVED AT CALL TIME, NOT BOUND AT IMPORT. `from deps import
# supabase` captures the object once, so a test (or anything else) that swaps
# `deps.supabase` afterwards cannot reach this module — and because this module is a
# SHARED loader, the reads it performs used to live in the routers where the tests patch.
# Moving them here silently took them out of reach of every one of those patches and CI
# went red with `KeyError: 'SUPABASE_URL'` (the real proxy trying to build a client).
# Going through the module keeps one patch point for the whole app.
import deps

log = logging.getLogger(__name__)

# The union of every column any reader asks for. Adding one here is cheap (the row count is tiny);
# adding a NEW query shape elsewhere is what costs a round trip.
_MODEL_COLS = ("id,name,display_name,omschrijving,portfolio_type,"
               "positions_datum,positions_dates,positions_scanned_at")
_POSITION_COLS = "portfolio_id,isin,fonds,percentage,datum,categorie"

# ⚠⚠ BOTH READS PAGE, AND `airs_model_portfolio_position` IS *18 ROWS* FROM NEEDING IT.
#
# It holds **982 rows** and PostgREST's cap on Supabase cloud is **1,000** — it truncates SILENTLY,
# and locally the cap is 10,000, so the overflow would appear in PRODUCTION ONLY and look like a
# model that had quietly lost some positions. A `.limit(20000)` does not help: the server's cap is
# what binds, not ours. That is the exact shape of the bug measured in `common/fx_load.py`
# (a currency cut at 1,000 rows made a fully-priced holding vanish from its own portfolio, and the
# two environments reported different numbers off identical code).
#
# ⚠ THE SORT KEY IS THE PRIMARY KEY `id`, NOT `(portfolio_id, isin)` — AND THAT DISTINCTION IS
# REAL HERE, NOT THEORETICAL. Postgres promises nothing about tied rows across separate
# LIMIT/OFFSET queries, so a page boundary inside a tie serves a row twice or never. Measured
# 2026-08-11: this table contains **one genuine duplicate `(portfolio_id, isin)` pair** — a model
# that lists the same instrument at two weights (VTopSelectie OFF FX holds CapitaLand at 2% *and*
# 3%, documented in CLAUDE.md). So that pair is NOT unique and must not be paged on.
#
# ⚠ ADVANCE BY WHAT CAME BACK, STOP ON AN EMPTY PAGE. "A short page is the last page" only holds
# while the server's cap is at least the page size — the assumption that fails here.
_PAGE = 1000


def _paged(table: str, cols: str, order_by: tuple[str, ...]) -> list[dict]:
    """Every row of a small table, paged safely. One canonical request shape per page, so the
    per-request memo collapses repeat calls across modules.

    ⚠ `order_by` MUST BE THE PRIMARY KEY, WHOLE. It is passed as a tuple because not every table
    here has a surrogate `id`: `airs_model_weight`'s PK is the COMPOSITE
    `(portefeuille, fonds)`, and assuming an `id` column raised
    `column airs_model_weight.id does not exist` the first time this ran. A partial key is worse
    than a wrong one — it fails silently at a page boundary instead of loudly at the first query.
    """
    key = ("airs_ref", table, cols, order_by)
    # ⚠⚠ ONLY INSIDE A DECLARED-EXPENSIVE UNIT OF WORK. `read_cache.active()` is opened by exactly
    # four callers — the Analyse modal, the ad-hoc basket, the /portfolios overview and the
    # benchmark index build — and they are the ones with enough reads for this to pay. Consulting
    # the leg store costs a FINGERPRINT, which is one `pg_stat_user_tables` COPY (167ms locally,
    # one round trip in production) cached for 2s. At a dozen saved round trips that is a large
    # win; on an endpoint that reads one of these tables once it would be a small LOSS, and
    # applying it everywhere would have been an optimisation that slowed the cheap paths down.
    #
    # ⚠ AND A WRITER OPTS OUT. The fingerprint is re-read at most every 2 seconds, so a unit of
    # work that writes one of these tables and reads it again could be handed a snapshot from
    # before its own write — see `read_cache.wrote`.
    if read_cache.active() is None or read_cache.wrote():
        return _paged_uncached(table, cols, order_by)

    from routers._analysis_cache import leg  # noqa: PLC0415  (cycle at module level)

    blob = leg(key, lambda: _snapshot(table, cols, order_by))
    if blob is None:
        # ⚠ A TABLE THAT WILL NOT SERIALISE IS SERVED FROM THE DATABASE, NOT GUESSED AT. See
        # `_snapshot`: the fallback is correctness, and it costs a round trip nobody notices.
        return _paged_uncached(table, cols, order_by)
    # ⚠⚠ PARSED FRESH FOR EVERY CALLER, WHICH IS WHAT KEEPS THIS MODULE'S ORIGINAL PROMISE. The
    # note at the top of this file rejected caching PARSED LISTS precisely because the per-request
    # memo stores the HTTP RESPONSE, which postgrest turns into fresh dicts for every caller — so a
    # module that mutates a row it got back cannot corrupt the next module's copy, and "caching
    # parsed lists here would have quietly introduced exactly that hazard for 982 shared dicts".
    # That reasoning is still correct, so this does not cache a parsed list: it caches the SNAPSHOT
    # AS TEXT and parses it per call, which is the same shape of guarantee by the same mechanism.
    #
    # ⚠⚠ A STRING CANNOT BE DECORATED, and that is the point of choosing it over a deep copy. The
    # leg store's standing rule is "read, do not decorate" — a rule a future caller has to remember.
    # Here there is nothing to remember: the cached object is immutable, so the hazard is not
    # guarded against, it is absent.
    #
    # ⚠ AND IT IS THE FASTER OF THE TWO, measured against the deep copy it replaced (20 runs):
    # `airs_model_portfolio` 0.58ms -> 0.16ms, `airs_model_portfolio_position` 4.69 -> 0.98,
    # `airs_mutatie` 3.74 -> 1.16. Over the 31 `_paged` calls a warm Analyse request makes, that is
    # ~46ms of deep copying against ~13ms of parsing.
    return json.loads(blob)


def _snapshot(table: str, cols: str, order_by: tuple[str, ...]) -> str | None:
    """The table as JSON TEXT, or None if it will not round-trip.

    ⚠⚠ NO `default=` ON `dumps`, DELIBERATELY. A `default=str` would quietly turn anything the
    encoder does not recognise into its repr — a `Decimal` into `"12.5"`, a `date` into
    `"2026-09-01"` — and the cached snapshot would then differ in TYPE from what the database
    hands back, on some rows, only once a column changed type. That is a corruption that looks
    like data. Without it the encoder RAISES, and the caller falls back to reading the table.
    Verified at the time of writing: all four tables round-trip exactly (`json.loads(json.dumps(r))
    == r`), which is expected — postgrest has already decoded them from JSON.
    """
    rows = _paged_uncached(table, cols, order_by)
    try:
        return json.dumps(rows)
    except (TypeError, ValueError):
        log.warning("[airs_ref] %s will not serialise — serving it uncached", table)
        return None


def _paged_uncached(table: str, cols: str, order_by: tuple[str, ...]) -> list[dict]:
    """The read itself. Split out so the cached path above has something to call on a miss."""
    out: list[dict] = []
    off = 0
    widest = 0
    while True:
        q = deps.supabase.table(table).select(cols)
        for col in order_by:
            q = q.order(col)
        rows = q.range(off, off + _PAGE - 1).execute().data or []
        if not rows:
            break
        out += rows
        off += len(rows)
        # ⚠⚠ THE LAST PAGE COSTS A ROUND TRIP THAT PROVES NOTHING, AND THIS IS HOW TO SKIP IT
        # WITHOUT MAKING THE ASSUMPTION THAT IS BANNED. The rule everywhere in this codebase is
        # "advance by what came back, break on an EMPTY page" — because `len(rows) < page` assumes
        # the server cap is at least `page`, which is exactly the assumption that failed and cost
        # us a silently short read. So every paged read ends with one request that returns zero
        # rows: measured on the Analyse modal, `airs_model_portfolio_position` (1,016 rows) took
        # three trips for two pages of data, and `airs_mutatie` (1,082) the same.
        #
        # `widest` removes it by MEASURING the cap instead of assuming it. Once the server has
        # handed back a page of `widest` rows, it has demonstrated it will return that many — so a
        # later page of fewer than `widest` means it ran out of ROWS, not that it hit a cap, and
        # there is nothing after it. A short FIRST page still proves nothing (it could be the cap),
        # so that case asks again exactly as before.
        #
        # ⚠ STRICTLY STRONGER THAN THE OLD RULE, NEVER WEAKER: with a cap below `_PAGE` every page
        # comes back the same size and this never fires, which is the environment the rule was
        # written for.
        if 0 < len(rows) < widest:
            break
        widest = max(widest, len(rows))
    return out


def models() -> list[dict]:
    """Every `airs_model_portfolio` row, all the columns anyone needs. ~102 rows."""
    return _paged("airs_model_portfolio", _MODEL_COLS, ("id",))


def models_by_id() -> dict[int, dict]:
    """`{id: row}` over `models()`."""
    return {m["id"]: m for m in models()}


def model(portfolio_id: int) -> dict | None:
    """One model by id, from the shared read — not a `?id=eq.N` of its own (see the module note)."""
    return models_by_id().get(portfolio_id)


def positions() -> list[dict]:
    """Every `airs_model_portfolio_position` row. ~982 rows — see the paging note above."""
    return _paged("airs_model_portfolio_position", _POSITION_COLS, ("id",))


def positions_for(portfolio_id: int) -> list[dict]:
    """This model's positions, filtered in Python from the one shared read."""
    return [p for p in positions() if p.get("portfolio_id") == portfolio_id]


# ⚠ ONLY GENUINELY SMALL TABLES BELONG HERE, AND THE THRESHOLD IS NOT A STYLE PREFERENCE.
# A whole-table read costs `ceil(rows / 1000) + 1` round trips; a per-book filtered read costs one.
# So the pattern only wins when the table is small AND read for several books in one request.
# Measured 2026-08-11, which is why three tables were DELIBERATELY LEFT ALONE:
#
#     airs_mutatie         999 rows,  read 6x  ->  2   ✅ added
#     airs_model_weight    734 rows,  read 4x  ->  2   ✅ added
#     airs_holding       9,817 rows,  read 10x -> 11   ❌ no win
#     asset_analysis     8,376 rows,  read  3x ->  9   ❌ worse
#     asset_execution   16,150 rows,  read  6x -> 17   ❌ MUCH worse
#
# ⚠ Applying this pattern to `asset_execution` because it "looked like the others" would have
# nearly TRIPLED its round trips. Check the row count before adding a table here.
_MUTATIE_COLS = "portefeuille,boekdatum,grootboek,fonds,omschrijving,amount_eur"
_WEIGHT_COLS = "portefeuille,fonds,model_pct,actual_pct,drift_pct,drift_eur,buy,sell"


def mutaties() -> list[dict]:
    """Every `airs_mutatie` row (~999). Paged on the PK for the same reason as `positions()`."""
    return _paged("airs_mutatie", _MUTATIE_COLS, ("id",))


def mutaties_for(portefeuille: str) -> list[dict]:
    """One book's mutations, ordered as the ledger reader expects.

    ⚠ THE SORT IS REPRODUCED HERE, NOT LEFT TO THE SERVER. The original read ordered by
    `(boekdatum, grootboek, fonds)` and `direct_result` consumes them in that order; returning the
    shared read's PK order instead would silently change how the ledger is walked.
    ⚠ `None` sorts first rather than raising — a mutation with no `boekdatum` is a real row.
    """
    rows = [m for m in mutaties() if m.get("portefeuille") == portefeuille]
    return sorted(rows, key=lambda r: (str(r.get("boekdatum") or ""),
                                       str(r.get("grootboek") or ""),
                                       str(r.get("fonds") or "")))


def model_weights() -> list[dict]:
    """Every `airs_model_weight` row (~734)."""
    # PK is the COMPOSITE (portefeuille, fonds) — this table has no surrogate id.
    return _paged("airs_model_weight", _WEIGHT_COLS, ("portefeuille", "fonds"))


def model_weights_for(portefeuille: str) -> dict[str, dict]:
    """`{fonds: row}` for one book — the shape both callers already build."""
    return {r["fonds"]: r for r in model_weights() if r.get("portefeuille") == portefeuille}


def position_counts() -> dict[int, int]:
    """`{portfolio_id: n}` — ISIN-bearing rows only, matching how the grid counts holdings."""
    out: dict[int, int] = {}
    for p in positions():
        if p.get("isin"):
            out[p["portfolio_id"]] = out.get(p["portfolio_id"], 0) + 1
    return out

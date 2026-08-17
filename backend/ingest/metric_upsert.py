"""Shared batched upsert into `metric_data`.

The price/volume loaders (`ingest.prices`) and the earnings loaders
(`ingest.earnings`) both write time-series rows to `metric_data` keyed on the
same composite natural key. This is the single batched-upsert loop they used to
duplicate verbatim — prices wraps each batch in the transient-retry primitive;
earnings passes pre-validated rows and doesn't.

⚠⚠ AND `changed_rows` IS WHY A BULK FUNDAMENTALS FILL IS AFFORDABLE. See its docstring: the
earnings path re-writes a company's ENTIRE history on every refresh, and measured, essentially all
of it is already there.
"""
from __future__ import annotations

from supabase import Client

from common.retry import retry

# metric_data's natural key — the conflict target every metric upsert uses.
METRIC_CONFLICT = "company_id,metric_code,source_code,target_date"

#: What is read back to decide whether a row needs writing. ⚠ `is_prediction` IS PART OF THE
#: COMPARISON, not just the key: the estimates feed writes True and the other two False, so a row
#: that changed only in that flag must still be written or the forecast/actual split rots silently.
_COMPARE_COLUMNS = "metric_code,target_date,numeric_value,is_prediction"


def _key(row: dict) -> tuple[str, str]:
    """(metric_code, target_date) — the part of the natural key that varies within one company's
    feed. `company_id` and `source_code` are constant per group and are the filter, not the key."""
    return (str(row.get("metric_code")), str(row.get("target_date"))[:10])


def rows_match(new: dict, stored: dict) -> bool:
    """Is this parsed row already in the database, exactly?

    ⚠ `numeric_value` IS `double precision` (see the initial schema), so a Python float written and
    read back is the identical value — `==` is exact here rather than a tolerance. A tolerance would
    be the wrong instrument anyway: the question is "does writing this change anything", and any
    difference at all means yes.

    ⚠ NULL IS A VALUE, NOT AN ABSENCE. The financials parser deliberately emits a row with
    `numeric_value = None` where GuruFocus reported "N/A", so the dashboard can show that the period
    EXISTS with no figure rather than walking back to a numeric from years ago. Treating None as
    "no row" would make those the one thing this never stops re-writing.
    """
    if bool(new.get("is_prediction")) != bool(stored.get("is_prediction")):
        return False
    a, b = new.get("numeric_value"), stored.get("numeric_value")
    if a is None or b is None:
        return a is None and b is None
    return float(a) == float(b)


def partition_changed(rows: list[dict], stored: dict[tuple[str, str], dict]) -> tuple[list[dict], int]:
    """Split `rows` into (needs writing, count already identical). Pure — `stored` is whatever the
    caller read back, so this is testable without a database."""
    fresh = [r for r in rows if not rows_match(r, stored.get(_key(r)) or {})]
    return fresh, len(rows) - len(fresh)


def changed_rows(supabase: Client, rows: list[dict]) -> tuple[list[dict], int]:
    """Of these rows, the ones that would actually change something. Returns `(rows, n_unchanged)`.

    ⚠⚠ THIS IS THE COST OF A FUNDAMENTALS REFRESH, AND ALMOST ALL OF IT WAS BUYING NOTHING.
    `_parse_financials` flattens the whole GuruFocus blob — **263 leaf fields x ~160 periods**, so
    16,512 to 36,494 rows for ONE company — and every refresh upserted all of them, 500 at a time,
    into a table of 69,003,374 rows whose indexes are four times their reindexed size. Measured
    locally on five companies, 2026-08-17:

        Dassault Systemes  36,494 rows  ->  0 changed   73 upsert round trips   17.48s
        Legrand            25,800 rows  ->  0 changed   52 round trips           3.96s
        Lotus Bakeries     16,512 rows  ->  0 changed   34 round trips
        Sanofi             20,382 rows  ->  0 changed   41 round trips
        Vinci              19,092 rows  ->  0 changed   39 round trips

    **Zero changed rows in all five.** A company that has filed one new quarter has ~263 genuinely
    new rows and we rewrote up to 36,494 — the rest re-stating values already stored, at the cost of
    a dead tuple each. That is what produced the 1,065,898 dead tuples and the `57014` statement
    timeouts on the production SP500 run, and it is why `FILL_WORKERS` had to come down from eight to
    three: the concurrency was not the problem, the write volume was.

    Reading first costs ONE `COPY` — 0.14s to 1.04s on the same five — against 4 to 17 seconds of
    upserting. The read is also the cheap side of the asymmetry on purpose: it is a single streamed
    query, while the write is dozens of separate PostgREST round trips.

    ⚠ SCOPED TO THE CODES BEING WRITTEN, NOT TO THE COMPANY. `fetch_financials` takes a
    `metric_codes` filter — `routers/_asset_dividends.py` uses it to persist TWO codes (~320 rows) —
    and reading the company's whole 36,000-row history to diff 320 of them would make the narrow
    path dramatically worse than it was. The `metric_code = ANY(...)` filter also lands on
    `idx_metric_data_metric_source_company_date`, which leads on exactly that column.

    ⚠ NO COPY PATH MEANS NO FILTER, NOT A GUESS — the same rule as `due_company_ids`. Without a
    direct connection everything is handed back for writing, which is the behaviour this replaced:
    slower, never wrong. Anything unreadable degrades the same way.

    ⚠ AND THERE IS DELIBERATELY NO PostgREST FALLBACK, which is why `supabase` is accepted and
    unused (it keeps the signature symmetric with `upsert_metric_rows`, and is where such a fallback
    would go). Reading 36,000 rows through PostgREST's 1,000-row pages is ~37 round trips to save
    ~73 — a wash, for a paged reader that would have to be got exactly right (see the cap trap in
    `_has`). Writing everything is the better degraded behaviour.
    """
    from common.pg import load_rows_via_copy  # noqa: PLC0415 — optional transport, see common/pg

    if not rows:
        return [], 0

    # ⚠ GROUPED BY (company_id, source_code), because those two are the read's FILTER and a caller
    # is not forbidden from mixing them. Every earnings feed passes exactly one group, so this is
    # one query in practice — but a silent cross-company comparison would skip real writes.
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault((r.get("company_id"), r.get("source_code")), []).append(r)

    out: list[dict] = []
    unchanged = 0
    for (company_id, source_code), grp in groups.items():
        codes = sorted({str(r.get("metric_code")) for r in grp})
        got = load_rows_via_copy(
            "metric_data", _COMPARE_COLUMNS, "metric_code", codes,
            where={"company_id": company_id, "source_code": source_code})
        if got is None:                       # no direct connection — write everything, as before
            out.extend(grp)
            continue
        stored = {(str(g["metric_code"]), str(g["target_date"])[:10]): g for g in got}
        fresh, same = partition_changed(grp, stored)
        out.extend(fresh)
        unchanged += same
    return out, unchanged


def upsert_metric_rows(
    supabase: Client,
    rows: list[dict],
    *,
    batch_size: int = 500,
    with_retry: bool = False,
    description: str = "metric_data.upsert",
) -> int:
    """Upsert `rows` into `metric_data` in `batch_size` chunks, conflict-keyed
    on `METRIC_CONFLICT`. Returns the number of rows the DB reported written.

    `with_retry=True` wraps each batch in `common.retry.retry` (linear 5xx /
    timeout backoff) — the price/volume path wants that resilience; the
    earnings path passes pre-validated rows and doesn't.

    ⚠ IT WRITES WHAT IT IS GIVEN. Deciding what is worth writing is `changed_rows`, called by the
    earnings wrapper before this — kept separate so the price path, which already fetches only dates
    newer than its stored maximum and so has nothing to diff away, does not pay for a read it cannot
    use."""
    if not rows:
        return 0
    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]

        def _do(b: list[dict] = batch):
            return supabase.table("metric_data").upsert(
                b, on_conflict=METRIC_CONFLICT, ignore_duplicates=False,
            ).execute()

        resp = (
            retry(_do, base_delay=2, backoff="linear", description=description)
            if with_retry else _do()
        )
        total += len(resp.data or [])
    return total

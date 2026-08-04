"""The allocation POLICY: per risk profile, per asset class, a min / default / max share.

What each portfolio actually holds is measured elsewhere (`_airs_portfolio_analysis`); this is the
band it is SUPPOSED to hold, written down once per (risk profile, asset class) so the two can be
compared at all.

⚠ THE GRID IS ALWAYS COMPLETE, THE TABLE IS NOT. The reader edits a fixed 4x4 — four profiles the
app already classifies portfolios into, four invested classes it already buckets holdings into — so
the API returns all sixteen cells whether or not a row exists. A grid that renders only the cells
somebody has already filled in cannot be used to fill in the rest.

⚠ NULL IS NOT ZERO, AND THIS IS THE WHOLE REASON THE COLUMNS ARE NULLABLE. "No policy recorded" and
"hold none of this" are the same for a minimum and OPPOSITE for a default and a maximum. Seeding
the grid with zeros would publish a policy nobody wrote — one that reads "this profile may hold no
equities". Unset cells come back null and the editor shows them empty.

⚠ THE CLASSES ARE THE STORED KEYS, NOT THE LABELS. `Equity`, not `Stocks` — the display name lives
in ONE place (`allocationColors.bucketLabel`) and everything else in the app keys off the stored
value. A policy table spelling them its own way is a join waiting to break.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime

from deps import supabase

from ._airs_holding_isin import BUCKET_ALTS, BUCKET_BONDS, BUCKET_EQUITY, BUCKET_EQUITY_ETF
from ._airs_portfolio_variant import VARIANTS

_log = logging.getLogger(__name__)

# The four INVESTED classes, in the order the editor shows them. Cash and Unclassified are
# deliberately absent: cash is the remainder (which is why the defaults need not sum to 100), and
# "Unclassified" is our own inability to see inside a fund — neither is something a policy can set
# a target for.
POLICY_BUCKETS: tuple[str, ...] = (BUCKET_EQUITY, BUCKET_EQUITY_ETF, BUCKET_BONDS, BUCKET_ALTS)

_FIELDS = ("min_pct", "default_pct", "max_pct")


def _num(v: object) -> float | None:
    """A percent, or None. Postgres `numeric` arrives from PostgREST as a STRING, so a bare float()
    on the way out is not optional — a JSON payload of "60.0" would make the editor's inputs
    stringly-typed and its arithmetic (the defaults' sum) concatenate instead of add."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_bands() -> list[dict]:
    """All sixteen cells, stored values where they exist and nulls where they do not."""
    rows = (supabase.table("airs_allocation_band")
            .select("variant,bucket,min_pct,default_pct,max_pct,updated_at").execute().data or [])
    stored = {(r["variant"], r["bucket"]): r for r in rows}
    # ⚠ Rows for a variant or bucket we no longer recognise are LOGGED, not silently dropped and
    # not silently shown: the grid is fixed, so an orphan is invisible in the editor and would be
    # deleted by the next save without anyone seeing it go.
    orphans = [k for k in stored if k[0] not in VARIANTS or k[1] not in POLICY_BUCKETS]
    if orphans:
        _log.warning("[bands] %d stored row(s) outside the editable grid and therefore invisible "
                     "in the editor: %s", len(orphans), orphans)
    out: list[dict] = []
    for variant in VARIANTS:
        for bucket in POLICY_BUCKETS:
            r = stored.get((variant, bucket)) or {}
            out.append({
                "variant": variant,
                "bucket": bucket,
                **{f: _num(r.get(f)) for f in _FIELDS},
                "updated_at": r.get("updated_at"),
            })
    return out


def validate_band(cell: dict) -> str | None:
    """The one rule, stated once: each bound in 0..100 and min <= default <= max WHERE BOTH ARE SET.

    ⚠ A HALF-FILLED ROW IS LEGAL. The grid is filled in over time, and refusing to store a maximum
    until its minimum exists makes the editor unusable on the way there. Only pairs that are BOTH
    present are compared — the same rule the table's CHECK constraints enforce, so the API refuses
    with a sentence rather than letting Postgres refuse with a constraint name.
    """
    vals = {f: _num(cell.get(f)) for f in _FIELDS}
    for f, v in vals.items():
        if v is not None and not (0 <= v <= 100):
            return f"{f.replace('_pct', '')} must be between 0 and 100 (got {v})"
    lo, mid, hi = vals["min_pct"], vals["default_pct"], vals["max_pct"]
    if lo is not None and mid is not None and lo > mid:
        return f"minimum {lo} is above the default {mid}"
    if mid is not None and hi is not None and mid > hi:
        return f"default {mid} is above the maximum {hi}"
    if lo is not None and hi is not None and lo > hi:
        return f"minimum {lo} is above the maximum {hi}"
    return None


def save_bands(cells: list[dict]) -> int:
    """Apply `cells` to the policy — a PARTIAL update. Returns how many rows were written.

    ⚠ A CELL NOT IN THE LIST IS NOT TOUCHED, and that is load-bearing rather than an implementation
    detail. An all-null cell means "clear this row", so a caller that helpfully sends the whole
    grid is sending fifteen "clear that" instructions alongside its one edit — and if its view of
    the grid is stale, they land. Measured 2026-08-04: the seed migration wrote all 16 bands, an
    editor opened before it saved ONE cell, and 15 rows were deleted with no error and nothing on
    screen out of place. The editor now sends only what the reader touched; this docstring is the
    reason it must keep doing so.

    ⚠ AN ALL-NULL CELL IS A DELETE, NOT AN UPSERT OF NULLS. Clearing a row in the editor means "no
    policy here"; storing three nulls would leave a row whose `updated_at` claims somebody set
    something. Same end state, honest provenance.

    ⚠ VALIDATED BEFORE ANY WRITE, NOT PER ROW AS WE GO. A grid save is one intent; letting the
    first eight cells land and then rejecting the ninth leaves a policy half-updated, which is
    worse than refusing the lot — the reader believes what they typed, and half of it is true.
    """
    upserts: list[dict] = []
    deletes: list[tuple[str, str]] = []
    for c in cells:
        variant, bucket = c.get("variant"), c.get("bucket")
        if variant not in VARIANTS or bucket not in POLICY_BUCKETS:
            raise ValueError(f"unknown cell {variant!r} / {bucket!r}")
        err = validate_band(c)
        if err:
            raise ValueError(f"{variant} · {bucket}: {err}")
        vals = {f: _num(c.get(f)) for f in _FIELDS}
        if all(v is None for v in vals.values()):
            deletes.append((variant, bucket))
        else:
            # ⚠ An ISO timestamp, not the string "now()" — PostgREST sends the payload as JSON, so
            # a SQL expression arrives as six literal characters and the insert fails on the type.
            upserts.append({"variant": variant, "bucket": bucket, **vals,
                            "updated_at": datetime.now(UTC).isoformat()})
    for variant, bucket in deletes:
        (supabase.table("airs_allocation_band").delete()
         .eq("variant", variant).eq("bucket", bucket).execute())
    if upserts:
        supabase.table("airs_allocation_band").upsert(
            upserts, on_conflict="variant,bucket").execute()
    _log.warning("[bands] policy saved — %d cell(s) written, %d cleared", len(upserts), len(deletes))
    return len(upserts)

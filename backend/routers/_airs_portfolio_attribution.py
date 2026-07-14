"""WHY a model beat or lagged the index — Brinson-Fachler attribution + contributors.

An excess return is a fact; it is not an explanation. "-11.60% vs ACWI" says nothing about
whether the bet that failed was the SECTORS you chose or the STOCKS you chose inside them. Those
are different mistakes with different fixes, and Brinson-Fachler separates them:

    ALLOCATION_i  = (w_p,i - w_b,i) x (R_b,i - R_b)      did you tilt toward the right buckets?
    SELECTION_i   =  w_b,i         x (R_p,i - R_b,i)     inside a bucket, did your picks win?
    INTERACTION_i = (w_p,i - w_b,i) x (R_p,i - R_b,i)    the cross term

`R_b` (no subscript) is the benchmark's TOTAL return — that is the "-Fachler" part, and it is
what makes ALLOCATION mean "overweighting a bucket that beat the INDEX", not merely "a bucket
that went up". Overweighting a sector that rose 5% while the index rose 10% is a BAD allocation
call, and plain Brinson would score it positive.

⚠ THE IDENTITY IS THE WHOLE POINT — AND IT IS ASSERTED, NOT ASSUMED.

    sum_i (allocation_i + selection_i + interaction_i)  ==  R_p - R_b

If that does not hold, the decomposition is not a decomposition — it is three columns of numbers
that happen to sit next to each other. `reconciles` carries the residual, and it is returned, not
swallowed.

⚠ FUNDS AND CASH ARE EXCLUDED, AND THE EXCLUDED SHARE IS STATED.
    An ETF has no sector. Bucketed as `Fund (not looked through)`, the benchmark's weight there
    is ZERO — so Brinson would assign the fund's ENTIRE return to "allocation", i.e. it would
    report that holding a diversified world tracker was a *sector bet*. That is arithmetically
    true and analytically worthless. Cash is the same.

    So the attribution runs on the ATTRIBUTABLE sleeve only — the priced, sector-classified
    equities — with weights renormalised to sum to 1 within it. The funds and cash are reported
    as their own line (weight + return, undecomposed), and `attributable_pct` says how much of
    the model the table below actually explains.

    ⚠ Consequence, stated rather than hidden: the attributed excess is the excess OF THAT SLEEVE,
    and it does NOT equal the headline excess (which includes the fund/cash drag). A reader who
    assumes it does will misread the table, so the payload carries both and the UI shows both.
"""
from __future__ import annotations

import asyncio

from deps import supabase
from routers._airs_portfolio_analysis import (
    CASH_BUCKET,
    FUND_BUCKET,
    SP500_LABEL,
    UNKNOWN_BUCKET,
    _buckets,
    _country_by_code,
    _grid,
)
from routers._airs_portfolio_perf import compute_holding_marks, ytd_anchor_for
from routers._asset_benchmark import index_rows

# Buckets that are NOT a sector bet and must never be decomposed as one.
_NON_ATTRIBUTABLE = {FUND_BUCKET, CASH_BUCKET, UNKNOWN_BUCKET}

_AXIS_IDX = {"sector": 0, "region": 1, "currency": 2}


def _weighted(rows: list[tuple[float, float]]) -> float:
    """Weighted return of (weight, return_pct) pairs, renormalised over the weights given."""
    w = sum(x[0] for x in rows)
    return sum(x[0] * x[1] for x in rows) / w if w > 0 else 0.0


def compute_attribution(portfolio_id: int, benchmark_label: str = SP500_LABEL,
                        window: str = "ytd", axis: str = "sector") -> dict:
    """Brinson-Fachler over one window, plus the names that drove it."""
    idx = _AXIS_IDX.get(axis, 0)

    p = (supabase.table("airs_model_portfolio")
         .select("id,name,positions_datum").eq("id", portfolio_id).limit(1).execute().data or [])
    if not p:
        return {}
    p = p[0]
    eff = p.get("positions_datum")
    start = eff if window == "since" else ytd_anchor_for(eff)
    if not start:
        return {}

    pos = (supabase.table("airs_model_portfolio_position")
           .select("isin,fonds,percentage,datum")
           .eq("portfolio_id", portfolio_id).execute().data or [])
    if eff:
        pos = [r for r in pos if r.get("datum") == eff]

    held = sorted({r["isin"] for r in pos if r.get("isin")})
    grid = _grid(held)
    codes = _country_by_code()
    marks = compute_holding_marks(held, start)

    # --- the portfolio: split into attributable and not ---------------------------------
    attributable: list[dict] = []
    excluded: list[dict] = []
    total_w = 0.0
    for r in pos:
        w = float(r.get("percentage") or 0)
        if w <= 0:
            continue
        total_w += w
        isin = r.get("isin")
        row = grid.get(isin) if isin else None
        bucket = _buckets(row, is_cash=not isin, isin=isin, codes=codes)[idx]
        m = marks.get(isin) if isin else None
        # Cash returns a flat 0% — its drag is a FACT, so it is carried, not dropped.
        ret = 0.0 if not isin else (m or {}).get("return_pct")

        # ⚠ TWO KINDS OF EXCLUSION, AND THEY ARE NOT THE SAME FACT.
        #
        #   fund / cash  — genuinely NOT a sector bet. An ETF has no sector; the benchmark's
        #                  weight in the fund bucket is zero, so Brinson would score a world
        #                  tracker as a sector bet. Excluding it is right and costs nothing.
        #
        #   UNPRICED     — a real equity, in a real sector, that we cannot price. Dropping it
        #                  makes its sector read as UNOWNED: this model holds 6% Healthcare and,
        #                  with that holding dropped, the table credited it +1.73pp of allocation
        #                  for "avoiding" Healthcare. That is a false finding, not a missing one.
        #                  It still has to be excluded (there is no return to attribute), so it is
        #                  flagged LOUDLY instead — `unpriced_pct` + the affected buckets.
        reason = ("fund" if bucket == FUND_BUCKET
                  else "cash" if bucket == CASH_BUCKET
                  else "unpriced" if ret is None
                  else "unclassified" if bucket == UNKNOWN_BUCKET
                  else None)
        item = {"isin": isin, "name": r.get("fonds"), "weight_pct": w,
                "return_pct": ret, "bucket": bucket, "reason": reason}
        if reason:
            excluded.append(item)
        else:
            attributable.append(item)

    # --- the benchmark ------------------------------------------------------------------
    bench, coverage = index_rows(benchmark_label, start)
    bgrid = _grid(sorted({b["isin"] for b in bench if b.get("isin")}))
    b_by_bucket: dict[str, list[tuple[float, float]]] = {}
    for b in bench:
        row = bgrid.get(b.get("isin") or "")
        if not row:
            continue
        bucket = _buckets(row, is_cash=False, isin=b.get("isin"), codes=codes)[idx]
        b_by_bucket.setdefault(bucket, []).append((b["weight_pct"], b["return_eur_pct"]))

    # Renormalise BOTH sides over what each can attribute. The identity below needs weights that
    # sum to 1 on each side; anything else and the residual is just the missing weight.
    p_w_total = sum(i["weight_pct"] for i in attributable)
    b_w_total = sum(w for rows in b_by_bucket.values() for w, _ in rows)
    if p_w_total <= 0 or b_w_total <= 0:
        return {"portfolio_id": portfolio_id, "name": p["name"], "benchmark": benchmark_label,
                "window": window, "axis": axis, "start": start, "rows": [],
                "attributable_pct": 0.0, "note": "Nothing in this model can be attributed."}

    p_by_bucket: dict[str, list[tuple[float, float]]] = {}
    for i in attributable:
        p_by_bucket.setdefault(i["bucket"], []).append(
            (i["weight_pct"] / p_w_total * 100.0, i["return_pct"]))
    b_norm: dict[str, list[tuple[float, float]]] = {
        k: [(w / b_w_total * 100.0, r) for w, r in rows] for k, rows in b_by_bucket.items()
    }

    r_p_total = sum(w / 100.0 * ret for rows in p_by_bucket.values() for w, ret in rows)
    r_b_total = sum(w / 100.0 * ret for rows in b_norm.values() for w, ret in rows)

    rows_out: list[dict] = []
    for bucket in sorted(set(p_by_bucket) | set(b_norm)):
        pr = p_by_bucket.get(bucket, [])
        br = b_norm.get(bucket, [])
        w_p = sum(w for w, _ in pr) / 100.0
        w_b = sum(w for w, _ in br) / 100.0
        R_p = _weighted(pr)
        R_b = _weighted(br)

        # ⚠ `R_b - r_b_total`, not `R_b`. This is the Fachler refinement: overweighting a bucket
        # that rose 5% while the INDEX rose 10% is a bad call, and plain Brinson scores it +.
        allocation = (w_p - w_b) * (R_b - r_b_total)
        # A bucket the benchmark does not hold has w_b = 0, so selection is 0 and the whole
        # effect lands in allocation — correct here, because these ARE real sector bets (an
        # unheld sector), unlike a fund, which is why funds are excluded above.
        selection = w_b * (R_p - R_b) if br and pr else 0.0
        interaction = (w_p - w_b) * (R_p - R_b) if br and pr else 0.0
        # A bucket the PORTFOLIO does not hold: selection/interaction are undefined (there are no
        # picks to judge), and the entire effect is the decision not to own it — allocation.
        rows_out.append({
            "bucket": bucket,
            "portfolio_weight_pct": w_p * 100.0,
            "benchmark_weight_pct": w_b * 100.0,
            "portfolio_return_pct": R_p if pr else None,
            "benchmark_return_pct": R_b if br else None,
            "allocation_pct": allocation,
            "selection_pct": selection,
            "interaction_pct": interaction,
            "total_pct": allocation + selection + interaction,
        })
    rows_out.sort(key=lambda r: r["total_pct"])

    attributed = sum(r["total_pct"] for r in rows_out)
    excess = r_p_total - r_b_total
    # THE IDENTITY. Returned, never assumed: three columns that do not sum to the excess are not
    # a decomposition of it.
    residual = excess - attributed

    # --- contributors: the names, not the buckets ---------------------------------------
    contrib = [{**i, "contribution_pct": i["weight_pct"] / 100.0 * (i["return_pct"] or 0.0)}
               for i in attributable + [e for e in excluded if e["return_pct"] is not None]]
    contrib.sort(key=lambda c: -c["contribution_pct"])
    held_isins = {i["isin"] for i in attributable if i.get("isin")}

    # ⚠ "DID NOT OWN" IS A STATEMENT ABOUT THE COMPANY, NOT ABOUT THE ISIN.
    # Alphabet is GOOGL (class A) in the index and "Alphabet - C" (class C) in this model — two
    # ISINs, one business. Matching on the ISIN reported GOOGL as a winner they MISSED, at
    # +3.23pp, while they were holding it and it was their single largest contributor. A
    # "missed opportunity" that the portfolio actually captured is the worst kind of false
    # finding: it is actionable, and the action is wrong.
    from asset_pipeline.resolve import same_company  # noqa: PLC0415

    held_names = [str(grid[i]["name"]) for i in held_isins
                  if grid.get(i) and grid[i].get("name")]

    def _held(b: dict) -> bool:
        if b.get("isin") in held_isins:
            return True
        n = b.get("company_name")
        return bool(n) and any(same_company(n, h) for h in held_names)

    # The index's biggest winners you did NOT own — the other half of "why", and the half a
    # holdings-only view can never show.
    missed = [{"isin": b.get("isin"), "name": b.get("company_name"),
               # `_window_rows` emits this as `ticker`, not `gurufocus_ticker` — and here it is a
               # yfinance symbol regardless.
               "ticker": b.get("ticker"), "weight_pct": b["weight_pct"],
               "return_pct": b["return_eur_pct"],
               "contribution_pct": b["weight_pct"] / 100.0 * b["return_eur_pct"]}
              for b in bench if not _held(b)]
    missed.sort(key=lambda m: -m["contribution_pct"])

    excl_w = sum(e["weight_pct"] for e in excluded)
    excl_priced = [e for e in excluded if e["return_pct"] is not None]
    # The dangerous subset: real sector positions the table below will show as UNOWNED.
    unpriced = [e for e in excluded if e["reason"] == "unpriced"]
    unpriced_w = sum(e["weight_pct"] for e in unpriced)

    return {
        "portfolio_id": portfolio_id,
        "name": p["name"],
        "benchmark": benchmark_label,
        "benchmark_coverage_pct": coverage.get("covered_pct"),
        "window": window,
        "axis": axis,
        "start": start,
        # The attributable sleeve's own numbers. NOT the headline — see the module docstring.
        "portfolio_return_pct": r_p_total,
        "benchmark_return_pct": r_b_total,
        "excess_pct": excess,
        "attributed_pct": attributed,
        "residual_pct": residual,
        "reconciles": abs(residual) < 1e-6,
        # ⚠ How much of the model the table above explains. The rest is funds and cash, which are
        # not a sector bet and are NOT decomposed.
        "attributable_pct": (p_w_total / total_w * 100.0) if total_w > 0 else 0.0,
        "excluded_pct": (excl_w / total_w * 100.0) if total_w > 0 else 0.0,
        "excluded_return_pct": (_weighted([(e["weight_pct"], e["return_pct"])
                                           for e in excl_priced]) if excl_priced else None),
        # ⚠ NOT the same as `excluded_pct`. A fund is excluded because it is not a sector bet; an
        # UNPRICED equity is excluded because we failed to price it — and its sector then reads as
        # UNOWNED in the table, so the allocation effect there is a FALSE finding. Surfaced with
        # the buckets it corrupts, so a reader can discount exactly those rows.
        "unpriced_pct": (unpriced_w / total_w * 100.0) if total_w > 0 else 0.0,
        "unpriced_buckets": sorted({e["bucket"] for e in unpriced}),
        "excluded": [{"bucket": e["bucket"], "name": e["name"], "isin": e["isin"],
                      "weight_pct": e["weight_pct"], "return_pct": e["return_pct"],
                      "reason": e["reason"]}
                     for e in sorted(excluded, key=lambda e: -e["weight_pct"])],
        "rows": rows_out,
        "top_contributors": contrib[:5],
        "top_detractors": [c for c in reversed(contrib[-5:]) if c["contribution_pct"] < 0],
        "missed_winners": missed[:5],
    }


async def compute_attribution_async(portfolio_id: int, benchmark_label: str = SP500_LABEL,
                                    window: str = "ytd", axis: str = "sector") -> dict:
    return await asyncio.to_thread(
        compute_attribution, portfolio_id, benchmark_label, window, axis)

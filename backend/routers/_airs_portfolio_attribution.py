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
from datetime import date

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
from routers._airs_portfolio_perf import ytd_anchor_for
from routers._asset_benchmark import index_rows

# ⚠ THE WEIGHTING BASIS IS SHARED WITH THE COMPOSITION CHARTS, NOT DUPLICATED HERE. Both read
# `portfolio_legs` + `split_legs`, which is what makes a sector bar and its Brinson row the same
# number. See `_airs_attribution_basis` for what that basis is and what it excludes.
from ._airs_attribution_basis import portfolio_legs, split_legs  # noqa: E402

# Buckets that are NOT a sector bet and must never be decomposed as one.
_NON_ATTRIBUTABLE = {FUND_BUCKET, CASH_BUCKET, UNKNOWN_BUCKET}

_AXIS_IDX = {"sector": 0, "region": 1, "currency": 2}


def _weighted(rows: list[tuple[float, float]]) -> float:
    """Weighted return of (weight, return_pct) pairs, renormalised over the weights given."""
    w = sum(x[0] for x in rows)
    return sum(x[0] * x[1] for x in rows) / w if w > 0 else 0.0


def _display_name(row: dict | None, source_name: str | None) -> str | None:
    """ONE name vocabulary across both sides of the comparison.

    The two sides speak different languages for the same security: the model side carries AIRS's
    own fund label ("AMD", "Applied", "Nvidia"), the index side carries `company.company_name`
    from the S&P/ACWI reconstruction ("Advanced Micro Devices Inc"). Set side by side in one
    table, one security under two names reads as a data bug — and it is the first thing a reader
    asks about. Both sides already join `asset_grid` by ISIN, so both can simply say the same
    thing; the source label rides along (`airs_name`) rather than being thrown away.

    ⚠ DISPLAY ONLY — this is NOT what makes the overlap match work, and must never become that.
    See `_overlaps`: the ISIN is the key, and a LABEL must not be load-bearing for correctness.
    """
    return (row or {}).get("name") or source_name


def _overlaps(h: dict, other_isins: set[str], other_names: list[str]) -> bool:
    """Is this holding held on the OTHER side too? ISIN first (exact), name as the fallback.

    ⚠ THE TWO MATCHERS ARE COMPLEMENTARY AND NEITHER ALONE IS ENOUGH.

      ISIN  catches ONE ISIN under TWO NAMES — the model's "AMD" is the index's "Advanced Micro
            Devices Inc". `same_company` scores that pair **16.0**: the roots reduce to 'amd' vs
            'advanced micro devices', which share no tokens at all. An acronym against a
            spelled-out name is exactly what a token matcher cannot bridge, and AMD was the ONE
            of 16 tech names in the measured case that failed.
      NAME  catches TWO ISINs under ONE BUSINESS — Alphabet class A in the index vs class C in
            the model. No ISIN comparison can ever see that. But the name test is EXACT ROOT
            EQUALITY, not `same_company`'s fuzzy floor — see below.

    ⚠ THE NAME TEST MUST BE EXACT ROOT EQUALITY, NOT `same_company`. `same_company` is right for
    matching a listing to ITS OWN issuer ("NVIDIA CORP" ↔ "NVIDIA Corporation") — a loose floor
    that tolerates noise. Here it is catastrophic: `_company_root("S&P Global Inc")` reduces to the
    single generic token 'global' (the initials S, P drop as single letters), and token_set_ratio
    then matches 'global' to EVERY name containing it — "Coinbase Global", "Apollo Global
    Management" — so index-only names get marked as held. Measured: on a Financials bucket the model
    held 5 names and the panel claimed 8 index overlaps, Coinbase among them, held by nobody.
    A share class (Alphabet A vs C, different ISINs) shares the SAME root; an unrelated company that
    merely shares a word does not. Exact equality is the line between them.

    ⚠ Do NOT re-route this through a shared display name to make a fuzzy match succeed. The ISIN is
    already in both dicts; deriving a name FROM it only to match the names is strictly lossier, and
    it would make correctness depend on a LABEL — a rename (the /companies "GF name" correction, a
    Yahoo refresh) would silently break the match. Structural, not incidental.
    """
    if h.get("isin") and h["isin"] in other_isins:
        return True
    # Imported here, not at module top, to keep clear of a circular import through asset_pipeline.
    from asset_pipeline.resolve import _company_root  # noqa: PLC0415
    root = _company_root(h.get("name"))
    return bool(root) and any(_company_root(o) == root for o in other_names)


def compute_attribution(portfolio_id: int, benchmark_label: str = SP500_LABEL,
                        window: str = "ytd", axis: str = "sector",
                        source: str = "book") -> dict:
    """Brinson-Fachler over one window, plus the names that drove it.

    `source="book"` decomposes the paired AIRS BOOK's actual holdings + returns instead of the
    yfinance model reconstruction; the benchmark is priced from the book's window (the calendar
    year, since AIRS reports the book over 1 Jan -> today) and is yfinance either way.
    """
    idx = _AXIS_IDX.get(axis, 0)

    p = (supabase.table("airs_model_portfolio")
         .select("id,name,positions_datum").eq("id", portfolio_id).limit(1).execute().data or [])
    if not p:
        return {}
    p = p[0]
    eff = p.get("positions_datum")
    if source == "book":
        # AIRS reports the book over the calendar year; 'since' has no book equivalent.
        start = f"{date.today().year}-01-01"
    else:
        start = eff if window == "since" else ytd_anchor_for(eff)
    if not start:
        return {}

    holdings = portfolio_legs(source, portfolio_id, eff, start)
    if holdings is None:
        return {"portfolio_id": portfolio_id, "name": p["name"], "benchmark": benchmark_label,
                "window": window, "axis": axis, "start": start, "source": source, "rows": [],
                "attributable_pct": 0.0,
                "note": "No paired AIRS book to attribute for this model."}

    held = sorted({h["isin"] for h in holdings if h.get("isin")})
    grid = _grid(held)
    codes = _country_by_code()

    # --- the portfolio: split into attributable and not ---------------------------------
    # ⚠ THE LADDER LIVES IN `_airs_attribution_basis`, NOT HERE. The composition charts build their
    # bars from the SAME call, which is the only reason the two panels agree — see that module's
    # header for what "attributable" means and what it costs.
    attributable, excluded, total_w = split_legs(holdings, idx, grid, codes)
    # `name` is the CANONICAL label (asset_grid, joined by ISIN) so this table and the index's
    # speak one vocabulary; `airs_name` keeps AIRS's own label, which is what you see in AIRS
    # itself and is the row's identity there. Display only — see `_display_name`.
    for i in (*attributable, *excluded):
        i["name"] = _display_name(i.get("grid_row"), i.get("airs_name"))

    # --- the benchmark ------------------------------------------------------------------
    bench, coverage = index_rows(benchmark_label, start)
    bgrid = _grid(sorted({b["isin"] for b in bench if b.get("isin")}))
    b_by_bucket: dict[str, list[tuple[float, float]]] = {}
    # The index's NAMES per bucket, for the click-through detail (b_by_bucket keeps only the
    # (weight, return) the math needs; this keeps the identity so a reader can see the names).
    bench_holdings_by_bucket: dict[str, list[dict]] = {}
    for b in bench:
        row = bgrid.get(b.get("isin") or "")
        if not row:
            continue
        bucket = _buckets(row, is_cash=False, isin=b.get("isin"), codes=codes)[idx]
        b_by_bucket.setdefault(bucket, []).append((b["weight_pct"], b["return_eur_pct"]))
        bench_holdings_by_bucket.setdefault(bucket, []).append({
            # Canonical label from the SAME `asset_grid` row the buckets above are read off, so
            # this table and the model's speak one vocabulary — see `_display_name`.
            "isin": b.get("isin"), "name": _display_name(row, b.get("company_name")),
            "ticker": b.get("ticker"),
            "weight_pct": b["weight_pct"], "return_pct": b["return_eur_pct"],
            "contribution_pct": b["weight_pct"] / 100.0 * b["return_eur_pct"],
        })

    # The model's OWN holdings per bucket — raw weight (matches the composition chart) + the EUR
    # return over this window and its contribution. Only the attributable equities; funds/cash/
    # unpriced live in `excluded` (the UI reads those for their own buckets).
    port_holdings_by_bucket: dict[str, list[dict]] = {}
    for i in attributable:
        port_holdings_by_bucket.setdefault(i["bucket"], []).append({
            "isin": i["isin"], "name": i["name"], "ticker": None,
            "weight_pct": i["weight_pct"], "return_pct": i["return_pct"],
            "contribution_pct": i["weight_pct"] / 100.0 * (i["return_pct"] or 0.0),
        })

    # Renormalise BOTH sides over what each can attribute. The identity below needs weights that
    # sum to 1 on each side; anything else and the residual is just the missing weight.
    p_w_total = sum(i["weight_pct"] for i in attributable)
    b_w_total = sum(w for rows in b_by_bucket.values() for w, _ in rows)
    if p_w_total <= 0 or b_w_total <= 0:
        return {"portfolio_id": portfolio_id, "name": p["name"], "benchmark": benchmark_label,
                "window": window, "axis": axis, "start": start, "source": source, "rows": [],
                "attributable_pct": 0.0, "note": "Nothing in this model can be attributed."}

    p_by_bucket: dict[str, list[tuple[float, float]]] = {}
    for i in attributable:
        p_by_bucket.setdefault(i["bucket"], []).append(
            (i["weight_pct"] / p_w_total * 100.0, i["return_pct"]))
    b_norm: dict[str, list[tuple[float, float]]] = {
        k: [(w / b_w_total * 100.0, r) for w, r in rows] for k, rows in b_by_bucket.items()
    }

    # ⚠ THE HOLDINGS LISTS MUST BE ON THE SAME BASE AS THE BUCKET WEIGHT THEY SIT UNDER.
    # `w_p`/`w_b` are renormalised over what each side can attribute (the identity below needs
    # weights summing to 1), but the per-holding lists were left as raw shares of the WHOLE
    # portfolio. Measured on ToppenbergBeheer Defensief: the drill-down said Technology 34.38%
    # and its own holdings added to 9.11% — out by exactly 100/attributable_pct (3.77x), on every
    # bucket. A reader who adds up the list gets a different number from the heading above it,
    # and neither is wrong on its own, which is the worst kind of disagreement to debug.
    for legs in port_holdings_by_bucket.values():
        for h in legs:
            h["weight_pct"] = h["weight_pct"] / p_w_total * 100.0
            # The contribution is weight x return, so it has to be rescaled with the weight or it
            # stops being the product of the two numbers printed beside it.
            h["contribution_pct"] = h["weight_pct"] / 100.0 * (h["return_pct"] or 0.0)
    for legs in bench_holdings_by_bucket.values():
        for h in legs:
            h["weight_pct"] = h["weight_pct"] / b_w_total * 100.0
            h["contribution_pct"] = h["weight_pct"] / 100.0 * (h["return_pct"] or 0.0)

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
        p_hold = sorted(port_holdings_by_bucket.get(bucket, []),
                        key=lambda h: -h["weight_pct"])
        b_hold_all = sorted(bench_holdings_by_bucket.get(bucket, []),
                            key=lambda h: -h["weight_pct"])
        # HELD ON BOTH SIDES — ISIN first, name as the share-class fallback. See `_overlaps` for
        # why both matchers are needed and why a shared display name is NOT the fix. Marked so the
        # overlap between what you hold and what the index holds is visible at a glance — and, by
        # contrast, so are the genuinely different bets. Both sides carry the FULL index bucket, so
        # a match to a smaller index name is not missed.
        p_names = [h["name"] for h in p_hold if h.get("name")]
        b_names = [h["name"] for h in b_hold_all if h.get("name")]
        p_isins = {h["isin"] for h in p_hold if h.get("isin")}
        b_isins = {h["isin"] for h in b_hold_all if h.get("isin")}
        for h in p_hold:
            h["in_both"] = _overlaps(h, b_isins, b_names)
        # The full index bucket — the drill-down lists every constituent (sorted client-side); no
        # cap. Sorted largest-weight first so the default view is meaningful before any re-sort.
        b_hold = b_hold_all
        for h in b_hold:
            h["in_both"] = _overlaps(h, p_isins, p_names)
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
            "portfolio_holdings": p_hold,
            "benchmark_holdings": b_hold,
            "benchmark_holdings_count": len(b_hold_all),
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

    # ⚠ "DID NOT OWN" IS A STATEMENT ABOUT THE COMPANY, NOT ABOUT THE ISIN — and equally, not
    # about the NAME. Alphabet is GOOGL (class A) in the index and "Alphabet - C" (class C) in
    # this model — two ISINs, one business — and matching on the ISIN alone reported GOOGL as a
    # winner they MISSED, at +3.23pp, while they were holding it and it was their single largest
    # contributor. AMD is the mirror image: one ISIN, two names, which no name match survives. A
    # "missed opportunity" that the portfolio actually captured is the worst kind of false
    # finding: it is actionable, and the action is wrong.
    #
    # SAME `_overlaps` as the per-bucket `in_both` above — one definition of "held on both
    # sides", so the two cannot drift. They already did once: this gate had the ISIN check and
    # `in_both` did not, and only `in_both` was wrong about AMD.
    held_names = [str(grid[i]["name"]) for i in held_isins
                  if grid.get(i) and grid[i].get("name")]

    def _held(b: dict) -> bool:
        return _overlaps({"isin": b.get("isin"), "name": b.get("company_name")},
                         held_isins, held_names)

    # The index's biggest winners you did NOT own — the other half of "why", and the half a
    # holdings-only view can never show.
    missed = [{"isin": b.get("isin"),
               # Same canonical vocabulary as every other name in this payload.
               "name": _display_name(bgrid.get(b.get("isin") or ""), b.get("company_name")),
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
        # Where the portfolio legs came from: the yfinance model, or the paired AIRS book.
        "source": source,
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
                                    window: str = "ytd", axis: str = "sector",
                                    source: str = "book") -> dict:
    return await asyncio.to_thread(
        compute_attribution, portfolio_id, benchmark_label, window, axis, source)

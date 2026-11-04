"""Read-only bottom-up attribution pilot for MerkenTopSelectie in Toppenberg Neutraal.

Run from backend: .venv/Scripts/python.exe scripts/audit_merken_certificate.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from math import isclose, isfinite
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from deps import supabase  # noqa: E402
from routers._airs_monthly_positions import position_boundary, position_performance, section_data  # noqa: E402
from routers._airs_monthly_reconciliation import reconcile_positions  # noqa: E402
from routers._airs_monthly_report import _monthly_rows, month_window  # noqa: E402
from routers._airs_strategy_map import dynamic_account_for_holding  # noqa: E402

PARENT = "TOPS_NEU_BEH_DYN"
CHILD = "BUS_MTS_OFF_AFS_DYN"
TOLERANCE_EUR = 1.0


def _wrapper(rows):
    return position_boundary([row for row in rows if dynamic_account_for_holding(row["name"]) == CHILD])


def attribute_month(month, cutoff, parent, child, performance):
    """Attribute EUR results only when matched dates, fixed units and both sides agree."""
    parent_open, parent_close, parent_trades = (section_data(parent, key) for key in
                                               ("opening", "closing", "transactions"))
    child_open, child_close = (section_data(child, key) for key in ("opening", "closing"))
    if not all((parent_open, parent_close, parent_trades, child_open, child_close)):
        return {"status": "missing_monthly_sources"}
    if (parent_open["as_of"] != child_open["as_of"] or
            parent_close["as_of"] != cutoff or child_close["as_of"] != cutoff or
            str(performance.get("periode", ""))[:10] != cutoff):
        return {"status": "valuation_dates_do_not_match", "expected_close": cutoff,
                "parent_dates": [parent_open["as_of"], parent_close["as_of"]],
                "child_dates": [child_open["as_of"], child_close["as_of"]]}

    first, first_error = _wrapper(parent_open["rows"])
    last, last_error = _wrapper(parent_close["rows"])
    if first_error or last_error or not first or not last:
        return {"status": "certificate_valuation_unavailable"}
    if (not parent_trades.get("readable") or parent_trades.get("unclassified_unassigned") or
            any(dynamic_account_for_holding(name) == CHILD for name in parent_trades.get("unclassified_names", []))):
        return {"status": "certificate_activity_unverified"}
    if (not isinstance(first.get("quantity"), (int, float)) or first["quantity"] <= 0
            or first["quantity"] != last.get("quantity") or
            any(dynamic_account_for_holding(trade["name"]) == CHILD for trade in parent_trades.get("activity", []))):
        return {"status": "certificate_units_changed_within_month"}

    positions = position_performance(child, month + "-01", cutoff)
    reconciliation = reconcile_positions(child, performance, positions)
    if reconciliation["status"] != "reconciled":
        return {"status": "child_does_not_reconcile", "issues": reconciliation["issues"],
                "difference_pp": reconciliation["difference_pp"]}

    opening_value, closing_value = first.get("value_eur"), last.get("value_eur")
    if (not isinstance(opening_value, (int, float)) or not isfinite(opening_value) or opening_value <= 0
            or not isinstance(closing_value, (int, float)) or not isfinite(closing_value) or closing_value <= 0):
        return {"status": "certificate_valuation_unavailable"}
    opening_capital = performance["beginvermogen"]
    exposure = opening_value / opening_capital
    if exposure > 1:
        return {"status": "certificate_exposure_exceeds_child_capital", "exposure": exposure}
    rows = [{"name": row["name"], "isin": row.get("isin"),
             "result_eur": row["contribution_pp"] * opening_capital / 100 * exposure}
            for row in positions if row["contribution_pp"] is not None]
    rows.extend({"name": adjustment["kind"], "isin": None,
                 "result_eur": adjustment["contribution_pp"] * opening_capital / 100 * exposure}
                for adjustment in reconciliation["adjustments"])
    calculated = sum(row["result_eur"] for row in rows)
    observed = closing_value - opening_value
    ties = isclose(calculated, observed, rel_tol=0, abs_tol=TOLERANCE_EUR)
    return {
        "status": "matched" if ties else "certificate_result_differs",
        "opening_date": parent_open["as_of"], "closing_date": cutoff,
        "certificate_units": first["quantity"], "exposure": exposure,
        "child_result_eur": performance["beleggingsresultaat"],
        "bottom_up_eur": round(calculated, 2), "certificate_value_change_eur": round(observed, 2),
        "difference_eur": round(observed - calculated, 2),
        "attribution": rows if ties else [],
    }


def audit():
    from routers._airs_portfolio_analysis import _position_ledger
    from routers._airs_return_reconciliation import account_return_reconciliation

    parent = account_return_reconciliation(PARENT)
    through = parent.get("book_as_of")
    result = {"parent": PARENT, "child": CHILD, "through": through, "months": {}}
    if not through or parent.get("holdings_as_of") != through:
        return {**result, "status": "parent_ytd_dates_do_not_match", "holdings_as_of": parent.get("holdings_as_of")}
    year = int(through[:4])
    ledger = _position_ledger(PARENT, parent)
    wrappers = [row for row in ledger.get("positions", [])
                if dynamic_account_for_holding(row["name"]) == CHILD]
    if (len(wrappers) != 1 or wrappers[0].get("capital_unknown")
            or not isinstance(wrappers[0].get("result_eur"), (int, float))):
        return {**result, "status": "parent_certificate_result_unavailable"}
    parent_result = wrappers[0]["result_eur"]
    result["certificate_ytd_eur"] = parent_result

    stored = (supabase.table("airs_monthly_report").select("portefeuille,month,report")
              .in_("portefeuille", [PARENT, CHILD]).gte("month", f"{year}-01-01")
              .lte("month", through).execute().data or [])
    sections = {(row["portefeuille"], row["month"][:7]): (row.get("report") or {}).get("sections", {})
                for row in stored}
    observations = {row["month"]: row for row in _monthly_rows(CHILD, year)}
    total, breakdown = 0.0, defaultdict(float)
    for number in range(1, int(through[5:7]) + 1):
        month = f"{year}-{number:02d}"
        cutoff = through if month == through[:7] else month_window(month, date.fromisoformat(through))[1]
        check = attribute_month(month, cutoff, sections.get((PARENT, month), {}),
                                sections.get((CHILD, month), {}), observations.get(month, {}))
        result["months"][month] = check
        for row in check.pop("attribution", []):
            breakdown[(row["isin"], row["name"])] += row["result_eur"]
            total += row["result_eur"]
    if any(row["status"] != "matched" for row in result["months"].values()):
        return {**result, "status": "monthly_coverage_incomplete"}

    difference = parent_result - total
    result.update(bottom_up_ytd_eur=round(total, 2), difference_eur=round(difference, 2))
    if not isclose(total, parent_result, rel_tol=0, abs_tol=TOLERANCE_EUR):
        return {**result, "status": "certificate_ytd_result_differs"}
    basis = parent.get("book_start_eur") if not (parent.get("deposits_eur") or parent.get("withdrawals_eur")) else None
    return {**result, "status": "verified", "attribution": [
        {"isin": isin, "name": name, "result_eur": round(value, 2),
         "contribution_pp": 100 * value / basis if basis and basis > 0 else None}
        for (isin, name), value in sorted(breakdown.items(), key=lambda item: -abs(item[1]))
    ]}


if __name__ == "__main__":
    try:
        report = audit()
    except Exception as exc:
        print(json.dumps({"status": "source_read_failed", "error": type(exc).__name__}))
        sys.exit(2)
    print(json.dumps(report, indent=2, ensure_ascii=True))
    sys.exit(0 if report["status"] == "verified" else 1)

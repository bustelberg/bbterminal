"""Dated constituent positions for the Toppenberg Performance bundle."""
from __future__ import annotations

import logging
from collections import defaultdict
from math import isclose

from deps import supabase
from routers._airs_monthly_positions import position_boundary, position_performance, section_data
from routers._airs_monthly_reconciliation import reconcile_positions
from routers._airs_strategy_map import dynamic_account_for_holding, nickname_for_holding

log = logging.getLogger(__name__)


def _attribute_certificate(wrapper, sections, positions, closing_date):
    calculation = wrapper.get("contribution_calculation")
    performance = section_data(sections, "performance") or {}
    if (not calculation or calculation["trades"] or str(performance.get("periode", ""))[:10] != closing_date
            or calculation["opening_date"] != section_data(sections, "opening")["as_of"]
            or calculation["closing_date"] != closing_date):
        return None

    checked = [dict(position) for position in positions]
    reconciliation = reconcile_positions(sections, performance, checked)
    if reconciliation["status"] != "reconciled":
        return None
    scale = calculation["opening_value_eur"] / calculation["portfolio_opening_capital_eur"]
    position_pp = [row["contribution_pp"] * scale for row in checked]
    accounting = [(row["kind"], row["contribution_pp"] * scale) for row in reconciliation["adjustments"]]
    explained_eur = (sum(position_pp) + sum(value for _, value in accounting)) * calculation["portfolio_opening_capital_eur"] / 100
    if not isclose(explained_eur, calculation["result_eur"], rel_tol=0, abs_tol=.01):
        return None
    for position, contribution in zip(positions, position_pp):
        position.update(contribution_pp=contribution, attribution_kind="underlying")
    return [
        {"name": wrapper["name"], "isin": None, "status": "held", "return_pct": None,
         "start_date": None, "end_date": None, "contribution_pp": value,
         "certificate": wrapper["name"], "attribution_kind": kind}
        for kind, value in accounting if value
    ]


def _child_reports(account: str, months: dict[str, str]) -> dict:
    from dashboard_daily import prepare_month
    from routers._airs_monthly_report import _PREPARATION_LOCKS, _PerformancePreparation, collect_report

    def read(keys):
        rows = (supabase.table("airs_monthly_report").select("month,report")
                .eq("portefeuille", account).in_("month", [key + "-01" for key in keys]).execute().data or [])
        return {str(row["month"])[:7]: row["report"] for row in rows if row.get("report")}

    def complete(report, closing_date):
        sections = report.get("sections", {})
        return (bool(section_data(sections, "opening"))
                and (section_data(sections, "closing") or {}).get("as_of") == closing_date
                and bool((section_data(sections, "transactions") or {}).get("readable")))

    with _PREPARATION_LOCKS[hash(account) % len(_PREPARATION_LOCKS)]:
        reports = read(months)
        missing = [month for month in sorted(months, reverse=True) if not complete(reports.get(month, {}), months[month])]
        for month in missing:
            try:
                if prepare_month(account, month=month, complete_only=True)["errors"]:
                    collect_report(account, month, _PerformancePreparation())
            except Exception:
                log.exception("[monthly-lookthrough] cannot prepare %s %s", account, month)
        if missing:
            reports.update(read(missing))
    return reports


def _constituents(wrapper: dict, snapshot: dict, report: dict, month: str) -> list[dict]:
    sections = report.get("sections", {})
    opening = section_data(sections, "opening")
    closing = section_data(sections, "closing")
    if not opening or not closing or closing.get("as_of") != snapshot["closing_date"]:
        return []
    rows = opening.get("rows", [])
    values = [row.get("value_eur") for row in rows]
    total = sum(values) if all(isinstance(value, (int, float)) for value in values) else 0
    same_open = opening["as_of"] == snapshot.get("opening_weight_date")
    parent_weight = wrapper.get("opening_weight_pct")
    by_name = defaultdict(list)
    for row in rows:
        by_name[row["name"]].append(row)
    positions = position_performance(sections, month + "-01", closing["as_of"])
    label = nickname_for_holding(wrapper["name"]) or wrapper["name"]
    for position in positions:
        record, error = position_boundary(by_name[position["name"]])
        value = (record or {}).get("value_eur", 0)
        child_weight = 100 * value / total if not error and total > 0 and isinstance(value, (int, float)) else None
        position["opening_weight_pct"] = (parent_weight * child_weight / 100
                                          if same_open and parent_weight is not None and child_weight is not None else None)
        position["certificate"] = wrapper["name"]
        position["via"] = [{"label": label, "weight_pct": child_weight}]
        position["is_cash"] = not position.get("isin") and position["name"].casefold() in {"effectenrekening", "liquiditeiten"}
        if position["is_cash"]:
            position["return_pct"] = None
    accounting = _attribute_certificate(wrapper, sections, positions, closing["as_of"])
    if accounting is not None:
        return positions + accounting
    if positions and wrapper.get("contribution_pp") is not None:
        positions.append({
            **wrapper, "opening_weight_pct": None, "return_pct": None, "start_date": None, "end_date": None,
            "start_price_eur": None, "end_price_eur": None, "attribution_kind": "certificate",
        })
    return positions


def _combine_routes(positions: list[dict]) -> None:
    by_isin = defaultdict(dict)
    for position in positions:
        if not position.get("isin") or not position.get("via"):
            continue
        route, = position["via"]
        weights = by_isin[position["isin"]]
        label, weight = route["label"], route["weight_pct"]
        previous = weights.get(label, 0)
        weights[label] = previous + weight if previous is not None and weight is not None else None
    for position in positions:
        if position.get("isin") and position.get("via"):
            weights = by_isin[position["isin"]]
            position["via"] = [{"label": label, "weight_pct": weight}
                               for label, weight in sorted(weights.items(), key=lambda item: -(item[1] or 0))]


def add_lookthrough(bundle: dict) -> None:
    snapshots = {}
    requests = defaultdict(dict)
    accounts_by_name = {}
    for year in bundle["years"].values():
        for month, detail in year["details"].items():
            snapshot = detail.get("snapshot")
            if not snapshot or not snapshot.get("closing_date"):
                continue
            routes = []
            for row in snapshot.get("positions") or []:
                name = row["name"]
                if name not in accounts_by_name:
                    accounts_by_name[name] = dynamic_account_for_holding(name)
                account = accounts_by_name[name]
                routes.append((row, account))
                if account and account != bundle["portefeuille"]:
                    requests[account][month] = snapshot["closing_date"]
            snapshots[month] = (snapshot, routes)

    reports = {}
    for account, months in requests.items():
        try:
            reports[account] = _child_reports(account, months)
        except Exception:
            log.exception("[monthly-lookthrough] cannot load reports for %s", account)

    for month, (snapshot, routes) in snapshots.items():
        positions, unavailable = [], []
        for wrapper, account in routes:
            if not account:
                positions.append(wrapper)
                continue
            children = _constituents(wrapper, snapshot, reports.get(account, {}).get(month, {}), month)
            positions.extend(children or [wrapper])
            if not children:
                unavailable.append(wrapper["name"])
        if any(account for _, account in routes):
            _combine_routes(positions)
            snapshot["lookthrough_positions"] = positions
            snapshot["lookthrough_unavailable"] = unavailable

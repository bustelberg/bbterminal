"""Holding-period EUR price changes from AIRS valuations/execution records only.

No portfolio-capital ratios, annual-result differencing, or Yahoo data. Income
is excluded: these are per-unit price/FX moves, not an account-return attribution.
"""
from __future__ import annotations

import math


def _positive(value) -> bool:
    return isinstance(value, (float, int)) and math.isfinite(value) and value > 0


def position_boundary(records: list[dict]) -> tuple[dict | None, str | None]:
    """One comparable security mark; explicit interest components remain separate.

    Unlabelled duplicate rows require either a unique EUR valuation verified by
    their common AIRS quote/quantity, or agreeing EUR unit marks for aggregation.
    Never identify the principal by picking the largest row.
    """
    security = [r for r in records if r.get('valuation_component') != 'accrued_interest']
    if not security:
        return None, 'marks_missing' if records else None
    isins = {str(r['isin']).strip().upper() for r in security if r.get('isin')}
    currencies = {str(r['currency']).strip().upper() for r in security if r.get('currency')}
    if len(isins) > 1 or len(currencies) > 1:
        return None, 'identity_changed'
    if len(security) == 1:
        return dict(security[0]), None
    if not isins or any(not r.get('isin') for r in security):
        return None, 'ambiguous_position'
    first = security[0]
    # Some AIRS exports repeat the same quantity/quote on an unlabelled
    # supplementary valuation line. Identify the principal by its reported quote,
    # not by value ranking. EUR avoids guessing FX; /100 supports percentage quotes.
    price, quantity = first.get('price_local'), first.get('quantity')
    if (currencies == {'EUR'} and _positive(price) and _positive(quantity)
            and all(r.get('price_local') == price and r.get('quantity') == quantity for r in security)):
        verified = [r for r in security if isinstance(r.get('value_eur'), (int, float))
                    and any(math.isclose(r['value_eur'], quantity * price * factor, rel_tol=0, abs_tol=.011)
                            for factor in (1, .01))]
        if len(verified) == 1:
            return dict(verified[0]), None
    for record in security:
        value, quantity = record.get('value_eur'), record.get('quantity')
        if not _positive(quantity) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            return None, 'ambiguous_position'
        if not math.isclose(value * first['quantity'], first['value_eur'] * quantity,
                            rel_tol=0, abs_tol=.011 * (first['quantity'] + quantity)):
            return None, 'ambiguous_position'
    return {**first, 'value_eur': sum(r['value_eur'] for r in security),
            'quantity': sum(r['quantity'] for r in security)}, None


def position_performance(sections: dict, start: str, end: str) -> list[dict]:
    def data(key):
        section = sections.get(key) or {}
        return section.get("data") if section.get("status") == "ok" else None

    opening, closing, transactions = data("opening"), data("closing"), data("transactions")
    # Both boundaries are required even for newly bought/sold rows: otherwise
    # absent holdings could simply be a failed report, not an entry or exit.
    opening_rows = (opening or {}).get("rows") or []
    closing_rows = (closing or {}).get("rows") or []
    names = sorted({row["name"] for row in opening_rows + closing_rows}
                   | {trade["name"] for trade in (transactions or {}).get("activity", [])})
    result = []
    for name in names:
        before = [r for r in opening_rows if r["name"] == name]
        after = [r for r in closing_rows if r["name"] == name]
        activity = [t for t in (transactions or {}).get("activity", []) if t["name"] == name]
        row = {"name": name, "isin": next((r.get("isin") for r in before + after if r.get("isin")), None),
               "return_pct": None, "start_date": None, "end_date": None, "status": "unavailable"}
        result.append(row)
        if not opening or not closing or not transactions or not transactions.get("readable"):
            row["reason"] = "reports_missing"
            continue
        if name in transactions.get("unclassified_names", []) or transactions.get("unclassified_unassigned"):
            row["reason"] = "unclassified_activity"
            continue
        first_record, first_error = position_boundary(before)
        last_record, last_error = position_boundary(after)
        if first_error or last_error:
            row['reason'] = first_error or last_error
            continue
        row['accrued_interest_opening_eur'] = sum(r.get('value_eur') or 0 for r in before if r.get('valuation_component') == 'accrued_interest')
        row['accrued_interest_closing_eur'] = sum(r.get('value_eur') or 0 for r in after if r.get('valuation_component') == 'accrued_interest')
        before = [first_record] if first_record is not None else []
        after = [last_record] if last_record is not None else []
        row['isin'] = next((r.get('isin') for r in before + after if r.get('isin')), None)
        if before and after and before[0].get("isin") and after[0].get("isin") and before[0]["isin"] != after[0]["isin"]:
            row["reason"] = "identity_changed"
            continue
        currencies = {r.get("currency") for r in before + after if r.get("currency")}
        if len(currencies) > 1:
            row["reason"] = "identity_changed"
            continue

        def mark(records, fallback, kind):
            if records:
                record = records[0]
                value, quantity = record.get("value_eur"), record.get("quantity")
                return (value / quantity, fallback) if isinstance(value, (float, int)) and math.isfinite(value) and value >= 0 and _positive(quantity) else (None, None)
            trades = sorted((t for t in activity if t["kind"] == kind), key=lambda t: t["date"])
            trade = (trades[0] if kind == "buy" else trades[-1]) if trades else None
            if trade and _positive(trade.get("amount_eur")) and _positive(trade.get("quantity")):
                return trade["amount_eur"] / trade["quantity"], trade["date"]
            return None, None

        first, first_date = mark(before, opening["as_of"], "buy")
        last, last_date = mark(after, closing["as_of"], "sell")
        if not first or last is None or not first_date or not last_date or first_date > last_date:
            row["reason"] = "marks_missing"
            continue
        if last_date < start or last_date > end or first_date > end:
            row["reason"] = "marks_missing"
            continue
        # A return cannot silently cross missing stock transfers or splits.
        q0 = before[0].get("quantity", 0) if before else 0
        q1 = after[0].get("quantity", 0) if after else 0
        if not isinstance(q0, (float, int)) or not isinstance(q1, (float, int)):
            row["reason"] = "marks_missing"
            continue
        if any(not _positive(t.get("quantity")) for t in activity):
            row["reason"] = "quantity_not_reconciled"
            continue
        expected = q0 + sum(t.get("quantity", 0) * (1 if t["kind"] == "buy" else -1) for t in activity)
        if not math.isfinite(expected) or abs(expected - q1) > max(1e-6, abs(q1) * 1e-6):
            row["reason"] = "quantity_not_reconciled"
            continue
        inventory = q0
        disconnected = False
        for trade in sorted(activity, key=lambda t: t["date"]):
            if trade["kind"] == "buy":
                if inventory <= 1e-6 and disconnected:
                    row["reason"] = "holding_period_discontinuous"
                    break
                inventory += trade["quantity"]
            else:
                inventory -= trade["quantity"]
                if inventory <= 1e-6:
                    disconnected = True
        else:
            row.update(return_pct=round((last / first - 1) * 100, 6),
                       start_price_eur=first, end_price_eur=last,
                       start_date=first_date, end_date=last_date,
                       status="held" if before and after else "bought" if after else "sold")
    return sorted(result, key=lambda r: (r["return_pct"] is None, -(r["return_pct"] or 0), r["name"]))

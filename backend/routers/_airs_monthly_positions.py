"""Holding-period EUR price changes from AIRS valuations/execution records only.

No portfolio-capital ratios, annual-result differencing, or Yahoo data. Income
is excluded: these are per-unit price/FX moves, not an account-return attribution.
"""
from __future__ import annotations

import math


def _positive(value) -> bool:
    return isinstance(value, (float, int)) and math.isfinite(value) and value > 0


def missing_position_returns(positions: list[dict]) -> list[dict]:
    return [row for row in positions if row.get('return_pct') is None
            and not row.get('is_cash')
            and not (not row.get('isin') and (row.get('name') or '').casefold() in {'effectenrekening', 'liquiditeiten'})]


def position_return_issue(row: dict) -> str:
    reason = row.get('reason') or 'unknown_reason'
    actions = {
        'reports_missing': 'check missing/unreadable AIRS sections, then reload this month',
        'unclassified_activity': 'inspect unclassified AIRS transactions and their dates/types',
        'ambiguous_position': 'check duplicate valuation lines and principal/accrued-interest labels',
        'identity_changed': 'check ISIN/currency changes between the two AIRS valuations',
        'marks_missing': 'check dated EUR values, quantities and buy/sell execution marks',
        'quantity_not_reconciled': 'reconcile opening quantity plus buys minus sells to closing quantity; check transfers/splits',
        'holding_period_discontinuous': 'the position was sold and repurchased; a single continuous holding-period return is unavailable',
    }
    evidence = row.get('return_diagnostics') or {}
    return (f"{row['name']} [{row.get('isin') or 'ISIN absent'}] ({reason}); "
            f"evidence={evidence}; next: {actions.get(reason, 'inspect the saved monthly source reports')}")


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
    # An unlabelled supplementary line can repeat the principal quantity and
    # quote without matching that quote's valuation. A line without either
    # quantity or quote cannot provide an independent security mark.
    if all(str(r.get('currency') or '').strip().upper() == 'EUR' for r in security):
        verified = []
        for record in security:
            price, quantity, value = (record.get(key) for key in ('price_local', 'quantity', 'value_eur'))
            if (_positive(price) and _positive(quantity) and _positive(value)
                    and any(math.isclose(value, quantity * price * factor, rel_tol=0, abs_tol=.011)
                            for factor in (1, .01))):
                verified.append(record)
        if len(verified) == 1:
            principal = verified[0]
            if all(
                (r.get('quantity') == principal['quantity'] and r.get('price_local') == principal['price_local'])
                or (r.get('quantity') in (None, 0) and r.get('price_local') in (None, 0))
                for r in security
            ):
                return dict(principal), None
    for record in security:
        value, quantity = record.get('value_eur'), record.get('quantity')
        if not _positive(quantity) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            return None, 'ambiguous_position'
        if not math.isclose(value * first['quantity'], first['value_eur'] * quantity,
                            rel_tol=0, abs_tol=.011 * (first['quantity'] + quantity)):
            return None, 'ambiguous_position'
    return {**first, 'value_eur': sum(r['value_eur'] for r in security),
            'quantity': sum(r['quantity'] for r in security)}, None


def section_data(sections: dict, key: str) -> dict | None:
    section = sections.get(key) or {}
    if key == "transactions" and section.get("status") == "no_data":
        return {"readable": True, "activity": []}
    return section.get("data") if section.get("status") == "ok" else None


def position_performance(sections: dict, start: str, end: str) -> list[dict]:
    opening, closing, transactions = (section_data(sections, key) for key in ("opening", "closing", "transactions"))
    opening, closing, transactions, aliases = _align_names_by_isin(opening, closing, transactions)
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
        if name in aliases:
            row['source_names'] = aliases[name]
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
            if row['reason'] == 'ambiguous_position':
                row['ambiguous_valuation_dates'] = sorted({
                    valuation['as_of']
                    for valuation, error in ((opening, first_error), (closing, last_error))
                    if error == 'ambiguous_position' and valuation.get('as_of')
                })
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
    for row in missing_position_returns(result):
        def boundary_evidence(valuation):
            return {'as_of': (valuation or {}).get('as_of'), 'marks': [
                {key: record.get(key) for key in ('isin', 'currency', 'quantity', 'value_eur', 'valuation_component')}
                for record in (valuation or {}).get('rows', []) if record['name'] == row['name']]}
        row['return_diagnostics'] = {
            'opening': boundary_evidence(opening), 'closing': boundary_evidence(closing),
            'activity': [dict(trade) for trade in (transactions or {}).get('activity', []) if trade['name'] == row['name']],
            'unclassified_activity': _unclassified_evidence(transactions or {}, row, start, end),
            'source_names': row.get('source_names', [row['name']]),
            'unclassified_names': (transactions or {}).get('unclassified_names', []),
            'unclassified_unassigned': (transactions or {}).get('unclassified_unassigned', False),
            'sections': {key: {'status': section.get('status'), 'message': section.get('message'),
                               **({'readable': (section.get('data') or {}).get('readable')} if key == 'transactions' else {})}
                         for key, section in sections.items()},
        }
    return sorted(result, key=lambda r: (r["return_pct"] is None, -(r["return_pct"] or 0), r["name"]))


def _align_names_by_isin(opening, closing, transactions):
    """Join renamed holdings by an exact security identity, never by name similarity."""
    names_by_isin, isins_by_name = {}, {}
    for valuation in (opening, closing):
        for record in (valuation or {}).get('rows', []):
            isin = str(record.get('isin') or '').strip().upper()
            if isin and record.get('name'):
                names_by_isin.setdefault(isin, set()).add(record['name'])
                isins_by_name.setdefault(record['name'], set()).add(isin)
    rename, aliases = {}, {}
    for isin, names in names_by_isin.items():
        if len(names) < 2 or any(len(isins_by_name[name]) != 1 for name in names):
            continue
        preferred = next((r['name'] for r in (closing or {}).get('rows', [])
                          if str(r.get('isin') or '').strip().upper() == isin), sorted(names)[0])
        rename.update({name: preferred for name in names})
        aliases[preferred] = sorted(names)
    def align(records):
        return [{**record, 'name': rename.get(record['name'], record['name']),
                 **({'source_name': record['name']} if record['name'] in rename else {})} for record in records]
    opening = {**opening, 'rows': align(opening.get('rows', []))} if opening else opening
    closing = {**closing, 'rows': align(closing.get('rows', []))} if closing else closing
    if transactions:
        transactions = {**transactions, 'activity': align(transactions.get('activity', [])),
                        'unclassified_names': [rename.get(name, name) for name in transactions.get('unclassified_names', [])]}
    return opening, closing, transactions, aliases


def _unclassified_evidence(transactions, position, start, end):
    from airs_transacties import COL_DATE, COL_FONDS, TT, TT_BUY, TT_SELL
    names = set(position.get('source_names') or [position['name']])
    rows = transactions.get('unclassified_activity')
    if rows is None:
        # Older saved reports already retain the raw sheet, so expose the cause
        # without requiring another AIRS download just to improve the receipt.
        rows = [row for row in transactions.get('rows', [])
                if row.get(TT) not in (TT_BUY, TT_SELL) or not row.get(COL_DATE)]
    return [dict(row) for row in rows if (not row.get(COL_FONDS) or row.get(COL_FONDS) in names)
            and (not row.get(COL_DATE) or start <= str(row[COL_DATE])[:10] <= end)]

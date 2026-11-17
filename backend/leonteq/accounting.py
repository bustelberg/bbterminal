"""Leonteq-only source ledger and reconciled, geometrically linked contributions.

Contracts observed in LYNQS 2026-11-16: composition?date=YYYY-MM-DD,
performance?from=...&to=..., and the paged index-transactions feed. netAmount
is signed in instrument currency (positive = outflow); bookingFx converts
it to bookingCurrency. Never substitute grossAmount for missing net cash.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal

from leonteq.portal_api import ApiUnavailable, _number

TABLE = 'leonteq_accounting_snapshot'
VERSION = 2
ZERO = Decimal(0)
HUNDRED = Decimal(100)


def transactions(payload):
    rows = payload.get('rows') if isinstance(payload, dict) else None
    if not isinstance(rows, list) or payload.get('totalRows') != len(rows):
        raise ApiUnavailable('Incomplete Leonteq accounting transaction feed')
    seen = {}
    for row in rows:
        try:
            stamp = date.fromisoformat(row['date'][:10]).isoformat()
            if row.get('reportId') is None or row.get('transactionNumber') is None:
                raise ValueError('Missing identity')
            identity = f"{row['reportId']}:{row['transactionNumber']}"
            event = row['event']
            if not isinstance(event, str) or not row.get('name'):
                raise ValueError('Missing event')
            booked = str(row.get('status') or 'booked').casefold() in ('booked', 'executed', 'settled', 'completed')
            def number(field):
                value = row.get(field)
                while isinstance(value, dict):
                    value = value.get('value', value.get('data'))
                return str(_number(value)) if value is not None else None
            record = {'id': identity, 'date': stamp, 'event': event, 'name': row['name'],
                'isin': (row.get('instrumentIsin') or '').strip().upper() or None,
                'category': row.get('instrumentCategory'), 'booked': booked,
                'quantity': number('totalQuantity'), 'gross_price': number('grossPrice'),
                'net_price': number('netPrice'), 'gross_amount': number('grossAmount'),
                'net_amount': number('netAmount'), 'booking_fx': number('bookingFx'),
                'base_fx': number('amcBaseCcyFx'), 'currency': row.get('instrumentCurrency'),
                'booking_currency': row.get('bookingCurrency')}
            net, fx = record['net_amount'], record['booking_fx']
            record['cash_eur'] = str(-Decimal(net) * Decimal(fx)) if (
                net is not None and fx is not None and Decimal(fx) > 0
                and record['booking_currency'] == 'EUR') else None
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiUnavailable('Incomplete Leonteq accounting transaction') from exc
        if identity in seen and seen[identity] != record:
            raise ApiUnavailable('Conflicting Leonteq accounting transaction identities')
        seen[identity] = record
    return sorted(seen.values(), key=lambda r: (r['date'], r['id']))


def valuation(payload, expected_date):
    """Validate a source boundary; an empty/ignored dated request is never today's book."""
    stamp = payload.get('closingPriceDate')
    if stamp != expected_date:
        raise ApiUnavailable('Leonteq valuation date does not match the requested boundary')
    raw = payload.get('rows')
    if not isinstance(raw, list) or not raw or not payload.get('totals'):
        raise ApiUnavailable(f'Leonteq opening valuation is unavailable for {expected_date}')
    positions = {}
    for row in raw:
        if row.get('amcCurrency') != 'EUR':
            raise ApiUnavailable('Leonteq accounting requires EUR base valuations')
        isin = (row.get('instrumentIsin') or '').strip().upper() or None
        cash = row.get('instrumentCategory') == 'Cash'
        if cash and row.get('currency') != 'EUR':
            raise ApiUnavailable('Non-EUR cash needs a complete Leonteq FX cash ledger')
        if not cash and not isin:
            raise ApiUnavailable('Leonteq security has no accounting identity')
        identity = 'cash:EUR' if cash else isin
        if identity in positions:
            raise ApiUnavailable('Duplicate Leonteq accounting position identity')
        value = _number(row['totalValueAmcCcy'])
        quantity = _number(row['totalUnits'])
        if quantity < 0 and not cash:
            raise ApiUnavailable('Short holdings require a separate Leonteq accounting method')
        previous = positions.get(identity, {'value': ZERO, 'quantity': ZERO})
        positions[identity] = {'name': row['name'], 'isin': isin, 'cash': cash,
            'value': previous['value'] + value, 'quantity': previous['quantity'] + quantity}
    nav = _number(payload['totals']['totalValueAmcCcy'])
    if nav <= 0 or abs(sum(p['value'] for p in positions.values()) - nav) > Decimal('.01') * len(raw):
        raise ApiUnavailable('Leonteq opening/closing values do not reconcile to NAV')
    return {'date': stamp, 'nav': nav, 'positions': positions}


def calculate(boundaries, records, points, reported_ytd):
    """Daily source P&L, checked capital-flow timing, then forward-link contributions.

    Trading net cash already includes transaction charges/taxes. AMC fees are a
    separate position. EUR cash has no investment P&L; its balance identifies
    external funding. Unknown events, missing dates/FX, quantity changes (splits,
    transfers), or an unresolved external-flow timing fail closed.
    """
    if len(points) < 2 or len(points) != len(boundaries):
        raise ApiUnavailable('Incomplete Leonteq opening and closing valuation history')
    ledger = {}
    linked = defaultdict(lambda: ZERO)
    total_return = ZERO
    has_external_flows = False
    for before, after, (start, start_index), (end, end_index) in zip(boundaries, boundaries[1:], points, points[1:]):
        if before['date'] != start or after['date'] != end or start_index <= 0:
            raise ApiUnavailable('Leonteq valuation and return windows differ')
        interval = [r for r in records if r['booked'] and start < r['date'] <= end]
        by_security = defaultdict(list)
        fees = ZERO
        cash_total = ZERO
        for record in interval:
            if record['cash_eur'] is None:
                raise ApiUnavailable('Leonteq transaction is missing net cash or booking FX')
            amount = Decimal(record['cash_eur'])
            cash_total += amount
            event = record['event']
            if event == 'AMC Fees':
                if amount > 0:
                    raise ApiUnavailable('Unexpected Leonteq fee cash sign')
                fees += amount
                continue
            if event not in ('Buy', 'Sell', 'Dividend') or not record['isin']:
                raise ApiUnavailable(f'Unsupported Leonteq accounting event: {event}')
            if (event == 'Buy' and amount >= 0) or (event in ('Sell', 'Dividend') and amount < 0):
                raise ApiUnavailable('Unexpected Leonteq transaction cash sign')
            by_security[record['isin']].append(record)
        day = {}
        keys = set(before['positions']) | set(after['positions']) | set(by_security)
        for identity in keys - {'cash:EUR'}:
            old = before['positions'].get(identity)
            new = after['positions'].get(identity)
            trades = by_security[identity]
            quantity_change = ZERO
            for trade in trades:
                if trade['event'] in ('Buy', 'Sell'):
                    if trade['quantity'] is None:
                        raise ApiUnavailable('Missing Leonteq transaction quantity')
                    quantity = abs(Decimal(trade['quantity']))
                    quantity_change += quantity if trade['event'] == 'Buy' else -quantity
            if abs((new or {}).get('quantity', ZERO) - (old or {}).get('quantity', ZERO) - quantity_change) > Decimal('.000001'):
                raise ApiUnavailable('Leonteq quantities do not reconcile (transfer/split or missing transactions)')
            profit = (new or {}).get('value', ZERO) - (old or {}).get('value', ZERO) + sum(Decimal(t['cash_eur']) for t in trades)
            day[identity] = profit
            item = ledger.setdefault(identity, {'name': (new or old or trades[0])['name'], 'isin': identity,
                'opening': ZERO,
                'profit': ZERO, 'buys': ZERO, 'dividends': ZERO})
            item['profit'] += profit
            item['buys'] += sum(-Decimal(t['cash_eur']) for t in trades if t['event'] == 'Buy')
            item['dividends'] += sum(Decimal(t['cash_eur']) for t in trades if t['event'] == 'Dividend')
        if fees:
            day['fee:AMC'] = fees
            fee = ledger.setdefault('fee:AMC', {'name': 'Leonteq AMC fees', 'isin': None,
                'opening': ZERO, 'profit': ZERO, 'buys': ZERO, 'dividends': ZERO})
            fee['profit'] += fees
        old_cash = before['positions'].get('cash:EUR', {}).get('value', ZERO)
        new_cash = after['positions'].get('cash:EUR', {}).get('value', ZERO)
        external = new_cash - old_cash - cash_total
        has_external_flows = has_external_flows or abs(external) > Decimal('.02')
        pnl = sum(day.values(), ZERO)
        if abs(after['nav'] - before['nav'] - pnl - external) > Decimal('.02'):
            raise ApiUnavailable('Leonteq NAV and cash ledger do not reconcile')
        rate = end_index / start_index - 1
        # Source amounts round to cents and index marks have finite precision.
        money_tolerance = Decimal('.01') * (len(before['positions']) + len(after['positions']) + len(interval))
        candidates = [before['nav']]
        if abs(external) > Decimal('.02'):
            candidates.append(before['nav'] + external)
        valid = [basis for basis in candidates if basis > 0 and abs(pnl / basis - rate) <= money_tolerance / basis + Decimal('.000001')]
        if not valid:
            raise ApiUnavailable(f'Leonteq daily P&L does not reconcile to its reported return/flow timing ({start} to {end})')
        if len(valid) > 1 and any(abs(value / valid[0] - value / valid[1]) > Decimal('.00001') for value in day.values()):
            raise ApiUnavailable('Leonteq external cash-flow timing is ambiguous')
        basis = valid[0]
        for identity in set(linked) | set(day):
            linked[identity] = linked[identity] * (1 + rate) + day.get(identity, ZERO) / basis
        total_return = (1 + total_return) * (1 + rate) - 1
    if reported_ytd is None or abs(total_return * HUNDRED - Decimal(str(reported_ytd))) > Decimal('.0051'):
        raise ApiUnavailable('Leonteq contribution window does not match its reported YTD')
    if abs(sum(linked.values(), ZERO) - total_return) > Decimal('.00005'):
        raise ApiUnavailable('Linked Leonteq contributions do not reconcile to reported YTD')
    first, last = boundaries[0], boundaries[-1]
    for identity, item in ledger.items():
        item['opening'] = first['positions'].get(identity, {}).get('value', ZERO)
        item['closing'] = last['positions'].get(identity, {}).get('value', ZERO)
        capital = item['opening'] + item['buys']
        item['return_pct'] = item['profit'] / capital * HUNDRED if capital > 0 else None
        item['contribution_pct'] = linked[identity] * HUNDRED
    return {'available': True, 'from': first['date'], 'to': last['date'],
            'reported_ytd_pct': float(reported_ytd), 'basis_eur': float(first['nav']),
            'has_external_flows': has_external_flows,
            'cash': {'opening': float(first['positions'].get('cash:EUR', {}).get('value', ZERO)),
                     'closing': float(last['positions'].get('cash:EUR', {}).get('value', ZERO))},
            'positions': {k: {n: float(v) if isinstance(v, Decimal) else v for n,v in item.items()} for k,item in ledger.items()}}


def saved(name):
    from deps import supabase
    from leonteq.portfolio_cache import _account_key
    account = _account_key()
    if not account:
        return None
    try:
        rows = (supabase.table(TABLE).select('payload').eq('account_key', account)
                .eq('portfolio_name', name).limit(1).execute().data or [])
        payload = rows[0]['payload'] if rows else None
        return payload if payload and payload.get('version') == VERSION else None
    except Exception:
        return None


def _collect(page, ids, name, as_of, progress):
    from leonteq.portal_api import read
    records = read(page, ids, name, accounting=True)
    history_source = read(page, ids, name, history=True, end=as_of)
    history = history_source['history']
    payload = {'version': VERSION, 'source': 'Leonteq', 'transactions': records['transactions'],
               'history': history, 'index_details': history_source['details'],
               'valuations': {}, 'retrieved_at': datetime.now(timezone.utc).isoformat()}
    try:
        if history_source['details'].get('currency') != 'EUR':
            raise ApiUnavailable('Leonteq contribution requires an EUR-denominated index history')
        series = history.get('series')
        if not isinstance(series, list) or len(series) < 2:
            raise ApiUnavailable('Leonteq has no opening return/valuation history')
        marks = {}
        for stamp, value in series:
            stamp = date.fromisoformat(stamp[:10]).isoformat()
            if stamp in marks:
                raise ApiUnavailable('Duplicate dates in Leonteq return history')
            marks[stamp] = _number(value)
        dates = sorted(d for d in marks if d <= as_of)
        if len(dates) < 2:
            raise ApiUnavailable('Leonteq has no complete return window')
        year_start = f'{dates[-1][:4]}-01-01'
        anchors = [d for d in dates if d < year_start]
        issue = history_source['details'].get('issueDate')
        if not anchors and (not issue or issue[:10] < year_start):
            raise ApiUnavailable('Leonteq has no year-opening index valuation for this existing portfolio')
        anchor = anchors[-1] if anchors else dates[0]
        dates = [d for d in dates if d >= anchor]
        if len(dates) > 400:
            raise ApiUnavailable('Leonteq valuation window is oversized')
        source_return = next((r for r in history.get('performances') or [] if r.get('nameOrIsin') == name), None)
        if not source_return:
            raise ApiUnavailable('Leonteq has no matching portfolio YTD')
        target = _number(source_return.get('performanceYTD'))
        boundaries = []
        for index, stamp in enumerate(dates):
            progress(index, len(dates), f'Leonteq accounting · valuation {stamp}')
            raw = read(page, ids, name, valuation_date=stamp)['accounting_valuation']
            payload['valuations'][stamp] = raw
            if (index == 0 and not anchors and stamp == series[0][0][:10]
                    and raw.get('closingPriceDate') == stamp and raw.get('rows') == [] and raw.get('totals') == {}):
                # A source-checked all-cash inception may be reconstructed from
                # the next composition and its BOOKED initial executions.
                from leonteq.opening import initial_records, reconstruct
                first_raw = read(page, ids, name, valuation_date=dates[1])['accounting_valuation']
                first = valuation(first_raw, dates[1])
                orders = read(page, ids, name, instructions=True)['instructions']
                payload['initial_instructions'] = orders
                payload['valuations'][dates[1]] = first_raw
                records['transactions'] = initial_records(records['transactions'], orders, first_raw, stamp, dates[1])
                payload['transactions'] = records['transactions']
                opening = reconstruct(first, records['transactions'], stamp, [(d, marks[d]) for d in dates])
                payload['opening_reconstruction'] = {
                    'method': 'booked_initial_executions_and_cash', 'date': stamp,
                    'first_composition_date': dates[1], 'opening_cash_eur': str(opening['nav']),
                    'verified_against_source_index': True}
                boundaries.append(opening)
            else:
                boundaries.append(valuation(raw, stamp))
        result = calculate(boundaries, records['transactions'], [(d, marks[d]) for d in dates], target)
        payload.update(success=True, status='reconciled', result=result)
    except ApiUnavailable as exc:
        payload.update(success=False, status='accounting_unavailable', reason=str(exc), result={'available': False})
    except (KeyError, TypeError, ValueError, AttributeError):
        payload.update(success=False, status='accounting_unavailable',
                       reason='Leonteq accounting response is incomplete or has an unsupported shape', result={'available': False})
    return payload


def refresh(name, as_of, progress=None):
    """Explicit acquisition only. Dashboard reads never log in or download history."""
    from deps import supabase
    from leonteq.portfolio_cache import _account_key
    from leonteq.portal_login import check_login, _login_lock
    from leonteq.session import session, on_session_thread
    if not as_of:
        return {'success': False, 'status': 'missing_valuation_date', 'reason': 'Leonteq has no dated closing valuation'}
    login = check_login()
    if not login.get('success'):
        return {'success': False, 'status': login.get('status'), 'reason': 'Leonteq accounting login failed'}
    if not _login_lock.acquire(blocking=False):
        return {'success': False, 'status': 'busy', 'reason': 'Another Leonteq read is running'}
    try:
        try:
            payload = on_session_thread(lambda: _collect(session.resources[3], session.portfolio_ids, name, as_of,
                                                         progress or (lambda *_: None)))
        except ApiUnavailable as exc:
            payload = {'version': VERSION, 'source': 'Leonteq', 'success': False,
                       'status': 'accounting_unavailable', 'reason': str(exc),
                       'retrieved_at': datetime.now(timezone.utc).isoformat(), 'result': {'available': False}}
        supabase.table(TABLE).upsert({'account_key': _account_key(), 'portfolio_name': name,
            'retrieved_at': payload['retrieved_at'], 'payload': payload},
            on_conflict='account_key,portfolio_name').execute()
        return payload
    finally:
        _login_lock.release()

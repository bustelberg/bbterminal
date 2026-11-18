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
            # Corporate-action pairs may settle in the instrument currency.
            # Use the source's native-to-AMC rate, never today's market FX.
            if (record['cash_eur'] is None and net is not None
                    and record['booking_currency'] not in (None, 'EUR')
                    and record['booking_currency'] == record['currency']
                    and record['base_fx'] is not None and Decimal(record['base_fx']) > 0):
                record['cash_eur'] = str(-Decimal(net) * Decimal(record['base_fx']))
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
        if not cash and not isin:
            raise ApiUnavailable('Leonteq security has no accounting identity')
        identity = 'cash:' + row['currency'] if cash else isin
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


def calculate(boundaries, records, points, reported_ytd, *, allow_estimates=False, cash_flows=None,
              positions_only=False):
    """Daily source P&L, checked capital-flow timing, then forward-link contributions.

    Trading net cash already includes transaction charges/taxes. AMC fees are a
    separate position. EUR cash has no investment P&L; its balance identifies
    external funding. Unknown events, missing dates/FX, quantity changes (splits,
    transfers), or an unresolved external-flow timing fail closed.
    """
    if len(points) < 2 or len(points) != len(boundaries):
        raise ApiUnavailable('Incomplete Leonteq opening and closing valuation history')
    ledger = {}
    unreconciled_cash = set()
    linked = defaultdict(lambda: ZERO)
    total_return = ZERO
    has_external_flows = False
    estimated = False
    cash_differences = []
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
            if record.get('booking_currency') == 'EUR':
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
            foreign_cash = identity.startswith('cash:')
            if foreign_cash:
                currency = identity.split(':', 1)[1]
                settled = [r for r in interval if r.get('booking_currency') == currency]
                native_change = sum((-Decimal(r['net_amount']) * Decimal(r['booking_fx'])
                    for r in settled if r.get('net_amount') is not None and r.get('booking_fx') is not None), ZERO)
                balance_change = (new or {}).get('quantity', ZERO) - (old or {}).get('quantity', ZERO)
                if abs(balance_change - native_change) > Decimal('.02'):
                    if positions_only:
                        estimated = True
                        unreconciled_cash.add(identity)
                        continue
                    raise ApiUnavailable(
                        f'Foreign cash quantities need complete source settlement movements '
                        f'for {currency} between {before["date"]} and {after["date"]}: '
                        f'balance changed by {balance_change}, recorded settlements explain {native_change}'
                    )
                profit = ((new or {}).get('value', ZERO) - (old or {}).get('value', ZERO)
                          - sum((Decimal(r['cash_eur']) for r in settled), ZERO))
                day[identity] = profit
                item = ledger.setdefault(identity, {'name': (new or old)['name'], 'isin': None,
                    'opening': ZERO, 'profit': ZERO, 'buys': ZERO, 'dividends': ZERO})
                item['profit'] += profit
                continue
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
        if cash_flows is not None:
            observed = sum((Decimal(r['cash_eur']) for r in cash_flows if start < r['date'] <= end), ZERO)
            difference = external - observed
            if abs(difference) > Decimal('.02'):
                if not allow_estimates:
                    raise ApiUnavailable(f'Leonteq cash movements and position ledger differ ({start} to {end})')
                estimated = True
                cash_differences.append({'from': start, 'to': end, 'difference_eur': float(difference)})
            external = observed
        has_external_flows = has_external_flows or abs(external) > Decimal('.02')
        pnl = sum(day.values(), ZERO)
        if abs(after['nav'] - before['nav'] - pnl - external) > Decimal('.02') and not allow_estimates:
            raise ApiUnavailable('Leonteq NAV and cash ledger do not reconcile')
        rate = end_index / start_index - 1
        # Source amounts round to cents and index marks have finite precision.
        money_tolerance = Decimal('.01') * (len(before['positions']) + len(after['positions']) + len(interval))
        candidates = [before['nav']]
        if abs(external) > Decimal('.02'):
            candidates.append(before['nav'] + external)
        valid = [basis for basis in candidates if basis > 0 and abs(pnl / basis - rate) <= money_tolerance / basis + Decimal('.000001')]
        if not valid:
            if not allow_estimates:
                raise ApiUnavailable(f'Leonteq daily P&L does not reconcile to its reported return/flow timing ({start} to {end})')
            valid = [before['nav']]
            estimated = True
        if len(valid) > 1 and any(abs(value / valid[0] - value / valid[1]) > Decimal('.00001') for value in day.values()):
            if not allow_estimates:
                raise ApiUnavailable('Leonteq external cash-flow timing is ambiguous')
            valid = [before['nav']]
            estimated = True
        basis = valid[0]
        for identity in set(linked) | set(day):
            linked[identity] = linked[identity] * (1 + rate) + day.get(identity, ZERO) / basis
        total_return = (1 + total_return) * (1 + rate) - 1
    if reported_ytd is None or abs(total_return * HUNDRED - Decimal(str(reported_ytd))) > Decimal('.0051'):
        raise ApiUnavailable('Leonteq contribution window does not match its reported YTD')
    if not estimated and abs(sum(linked.values(), ZERO) - total_return) > Decimal('.00005'):
        raise ApiUnavailable('Linked Leonteq contributions do not reconcile to reported YTD')
    first, last = boundaries[0], boundaries[-1]
    for identity in unreconciled_cash:
        ledger.pop(identity, None)
        linked.pop(identity, None)
    for identity, item in ledger.items():
        item['opening'] = first['positions'].get(identity, {}).get('value', ZERO)
        item['closing'] = last['positions'].get(identity, {}).get('value', ZERO)
        capital = item['opening'] + item['buys']
        item['return_pct'] = item['profit'] / capital * HUNDRED if capital > 0 else None
        item['contribution_pct'] = linked[identity] * HUNDRED
    return {'available': True, 'estimated': estimated, 'from': first['date'], 'to': last['date'],
            'unreconciled_cash': sorted(unreconciled_cash),
            'calculated_contribution_pct': float(sum(linked.values(), ZERO) * HUNDRED),
            'difference_pct': float((total_return - sum(linked.values(), ZERO)) * HUNDRED),
            'source_cash_flows_available': cash_flows is not None, 'cash_differences': cash_differences,
            'reported_ytd_pct': float(reported_ytd), 'basis_eur': float(first['nav']),
            'has_external_flows': has_external_flows,
            'cash': {'opening': float(first['positions'].get('cash:EUR', {}).get('value', ZERO)),
                     'closing': float(last['positions'].get('cash:EUR', {}).get('value', ZERO))},
            'positions': {k: {n: float(v) if isinstance(v, Decimal) else v for n,v in item.items()} for k,item in ledger.items()}}


def monthly_cash_verified(snapshot, report):
    """Verify cash for an older estimate using its saved source evidence."""
    if 'unreconciled_cash' in report:
        return report['unreconciled_cash'] == []
    try:
        start, end = report['from'], report['to']
        marks = sorted((stamp[:10], _number(value)) for stamp, value in snapshot['history']['series']
                       if start <= stamp[:10] <= end)
        if len(marks) < 2 or marks[0][0] != start or marks[-1][0] != end:
            return False
        opening = snapshot.get('opening_reconstruction') or {}
        values = snapshot['valuations']
        if (opening.get('verified_against_source_index')
                and opening.get('date') == start and opening.get('first_composition_date') == marks[1][0]):
            from leonteq.opening import reconstruct

            boundaries = [valuation(values[stamp], stamp) for stamp, _ in marks[1:]]
            first = reconstruct(boundaries[0], snapshot['transactions'], start, marks,
                                opening_cash=opening['opening_cash_eur'])
            boundaries.insert(0, first)
        else:
            boundaries = [valuation(values[stamp], stamp) for stamp, _ in marks]
        checked = calculate(boundaries, snapshot['transactions'], marks, report['reported_ytd_pct'],
                            allow_estimates=True, positions_only=True)
        return checked['unreconciled_cash'] == []
    except (ApiUnavailable, KeyError, TypeError, ValueError, ArithmeticError):
        return False


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


def _collect(page, ids, name, as_of, progress, previous=None):
    from leonteq.portal_api import read
    previous = previous or {}
    records = read(page, ids, name, accounting=True)
    # The source's performanceYTD is always current, even when ?to= truncates
    # its curve. Fetch both without a historical cutoff so they share one end.
    history_source = read(page, ids, name, history=True)
    history = history_source['history']
    payload = {'version': VERSION, 'source': 'Leonteq', 'transactions': records['transactions'],
               'history': history, 'index_details': history_source['details'],
               'valuations': {}, 'retrieved_at': datetime.now(timezone.utc).isoformat()}
    try:
        payload['certificate_cash_history'] = read(page, ids, name, cash_history=True,
            previous_cash_history=previous.get('certificate_cash_history'))
    except ApiUnavailable as exc:
        payload['certificate_cash_history'] = {'complete': False, 'reason': str(exc)}
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
        # Some source deployments ignore ?to=. Their published YTD then refers
        # to the actual latest history point, not the requested composition date.
        # Validate and collect that authoritative window as one coherent unit.
        from leonteq.return_windows import parse as return_window
        window = return_window(history, history_source['details'], name, payload['retrieved_at'])
        dates = sorted(d for d in marks if window['from'] <= d <= window['to'])
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
        target = Decimal(str(window['ytd_pct']))
        boundaries = []
        previous = previous or {}
        previous_marks = {stamp[:10]: value
                          for stamp, value in (previous.get('history') or {}).get('series') or []}
        previous_values = previous.get('valuations') or {}
        previous_end = max(previous_values, default='')
        cached_count = 0
        for index, stamp in enumerate(dates):
            progress(index, len(dates), f'Leonteq accounting · valuation {stamp}')
            if stamp in payload['valuations']:
                raw = payload['valuations'][stamp]
            elif (stamp < previous_end and stamp < dates[-1] and stamp in previous_values
                    and previous_marks.get(stamp) is not None
                    and _number(previous_marks[stamp]) == marks[stamp]):
                raw = previous_values[stamp]
                cached_count += 1
            else:
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
                records['transactions'] = initial_records(records['transactions'], orders, first_raw, stamp, dates[1],
                    prefer_transactions=True)
                payload['transactions'] = records['transactions']
                source_cash = payload.get('certificate_cash_history') or {}
                funding = sum((_number(row['netFlows']) for certificate in source_cash.get('certificates') or []
                    for row in certificate.get('cash_movements') or [] if row['dateTime'][:10] == dates[1]), ZERO)
                opening = reconstruct(first, records['transactions'], stamp, [(d, marks[d]) for d in dates],
                    opening_cash=funding if source_cash.get('complete') and funding > 0 else None)
                payload['opening_reconstruction'] = {
                    'method': 'booked_initial_executions_and_cash', 'date': stamp,
                    'first_composition_date': dates[1], 'opening_cash_eur': str(opening['nav']),
                    'verified_against_source_index': True}
                boundaries.append(opening)
            else:
                boundaries.append(valuation(raw, stamp))
        cash_history = payload.get('certificate_cash_history') or {}
        cash_flows = None
        if cash_history.get('complete'):
            cash_flows = [{'date': row['dateTime'][:10], 'cash_eur': str(_number(row['netFlows']))}
                for certificate in cash_history.get('certificates', []) for row in certificate['cash_movements']]
            reconstruction = payload.get('opening_reconstruction')
            if reconstruction:
                first_date = reconstruction['first_composition_date']
                launch = sum((Decimal(r['cash_eur']) for r in cash_flows if r['date'] == first_date), ZERO)
                if abs(launch - boundaries[0]['nav']) <= Decimal('.02'):
                    # Initial funding is already the reconstructed opening capital.
                    cash_flows = [r for r in cash_flows if r['date'] != first_date]
                    payload['opening_reconstruction']['source_funding_verified'] = True
        monthly = {}
        payload['monthly_attribution'] = monthly
        for month in dict.fromkeys(stamp[:7] for stamp in dates[1:]):
            indices = [i for i, stamp in enumerate(dates) if stamp[:7] == month]
            start, end = max(0, indices[0] - 1), indices[-1]
            month_dates = dates[start:end + 1]
            month_return = (marks[dates[end]] / marks[dates[start]] - 1) * HUNDRED
            month_boundaries = boundaries[start:end + 1]
            month_marks = [(stamp, marks[stamp]) for stamp in month_dates]
            try:
                monthly[month] = calculate(month_boundaries, records['transactions'],
                    month_marks, month_return, cash_flows=cash_flows)
            except ApiUnavailable as exc:
                try:
                    # Position P&L can still be supported when the portfolio funding basis is unclear.
                    monthly[month] = {**calculate(month_boundaries, records['transactions'],
                        month_marks, month_return, allow_estimates=True, cash_flows=cash_flows, positions_only=True),
                        'estimated': True, 'reason': str(exc)}
                except ApiUnavailable as position_error:
                    monthly[month] = {'available': False, 'reason': str(position_error)}
        result = calculate(boundaries, records['transactions'], [(d, marks[d]) for d in dates], target,
                           allow_estimates=True, cash_flows=cash_flows)
        payload['cached_valuations'] = cached_count
        payload.update(success=True, status='estimated' if result['estimated'] else 'reconciled', result=result)
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
        previous = saved(name)
        try:
            payload = on_session_thread(lambda: _collect(session.resources[3], session.portfolio_ids, name, as_of,
                                                         progress or (lambda *_: None), previous))
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

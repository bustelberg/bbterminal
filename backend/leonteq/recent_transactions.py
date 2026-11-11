"""Booked Leonteq trades and fresh inputs for individually reviewed Fixed weight changes."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from leonteq.portal_api import ApiUnavailable, _number


def parse_transactions(payload):
    if (not isinstance(payload, dict) or not isinstance(payload.get('rows'), list)
            or payload.get('totalRows') != len(payload['rows'])):
        raise ApiUnavailable('The transaction feed is incomplete; no weight actions can be prepared.')
    trades = {}
    for row in payload['rows']:
        if not isinstance(row, dict):
            raise ApiUnavailable('Invalid transaction row.')
        if row.get('event') not in ('Buy', 'Sell') or row.get('instrumentCategory') in ('Cash', 'Currency'):
            continue
        if row.get('status') and str(row['status']).casefold() not in ('booked', 'executed', 'settled', 'completed'):
            continue
        try:
            stamp = datetime.fromisoformat(row['date']).date().isoformat()
            report_id, number = row['reportId'], row['transactionNumber']
            if report_id is None or number is None or not row.get('name'):
                raise ValueError('Missing trade identity')
            identity = f'{report_id}:{number}'
            trade = {'id': identity, 'date': stamp, 'event': row['event'], 'name': row['name'],
                     'isin': (row.get('instrumentIsin') or '').strip().upper() or None,
                     'quantity': float(abs(_number(row['totalQuantity']))),
                     'price': float(_number(row['grossPrice'])), 'currency': row.get('instrumentCurrency')}
            if trade['quantity'] <= 0 or trade['price'] < 0:
                raise ValueError('Invalid trade quantity or price')
        except (KeyError, TypeError, ValueError):
            raise ApiUnavailable('A booked trade has incomplete identity, date or numeric fields.') from None
        if identity in trades and trades[identity] != trade:
            raise ApiUnavailable('Conflicting transaction identifiers were returned.')
        trades[identity] = trade
    return sorted(trades.values(), key=lambda row: (row['date'], row['id']), reverse=True)


def read(name):
    from leonteq.portal_login import check_login, _login_lock
    from leonteq.session import session, on_session_thread
    from leonteq.portal_api import read as read_api
    login = check_login()
    if not login.get('success'):
        raise HTTPException(502, 'Leonteq could not be reached. Retry Fetch transactions.')
    if not _login_lock.acquire(blocking=False):
        raise HTTPException(409, 'Another Leonteq read is running. Retry shortly.')
    try:
        return on_session_thread(lambda: read_api(session.resources[3], session.portfolio_ids, name, transactions=True))
    except ApiUnavailable as exc:
        raise HTTPException(502, str(exc)) from None
    finally:
        _login_lock.release()


def run(ctx, name, portfolio_id, days=7):
    from leonteq import portfolio_links, composition_cache, airs_models, holding_connections
    links = portfolio_links.options('fixed')['links']
    if not any(row['leonteq_name'] == name and row['airs_portfolio_id'] == portfolio_id for row in links):
        raise HTTPException(404, 'The Fixed portfolio connection no longer exists.')
    ctx.check()
    ctx.progress(0, 3, 'Reading booked Leonteq transactions…')
    transactions = read(name)
    ctx.check()
    ctx.progress(1, 3, 'Reading current Leonteq holdings and weights…')
    left = composition_cache.composition(name, refresh=True)
    if not left.get('success') or left.get('refresh_error') or not left.get('groups'):
        raise HTTPException(502, 'Fresh Leonteq holdings are unavailable. No weight actions were prepared.')
    ctx.check()
    ctx.progress(2, 3, 'Reading AIRS Fixed weights and saved holding connections…')
    right = airs_models.composition(portfolio_id, refresh=True)
    if right.get('no_fixed_model') or not right.get('rows'):
        raise HTTPException(502, 'A complete AIRS Fixed composition is required.')
    rules = holding_connections.read(name, portfolio_id)['rules']
    today = datetime.now(ZoneInfo('Europe/Amsterdam')).date()
    start = (today - timedelta(days=days - 1)).isoformat()
    trades = [row for row in transactions['transactions'] if start <= row['date'] <= today.isoformat()]
    ctx.progress(3, 3, f'{len(trades)} booked trades loaded. Review individual weights before applying.')
    return {'success': True, 'transactions': trades, 'from': start, 'to': today.isoformat(),
            'retrieved_at': transactions['retrieved_at'], 'leonteq': left, 'airs': right, 'rules': rules}

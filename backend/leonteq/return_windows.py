"""Authoritative Leonteq return boundaries, independent of position accounting."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import logging

from leonteq.portal_api import ApiUnavailable, _number

log = logging.getLogger(__name__)


def parse(history, details, name, retrieved_at):
    if not isinstance(history, dict) or not isinstance(details, dict) or details.get('currency') != 'EUR':
        raise ApiUnavailable('Leonteq has no verified EUR return window')
    marks = {}
    for stamp, value in history.get('series') or []:
        stamp = date.fromisoformat(stamp[:10]).isoformat()
        if stamp in marks:
            raise ApiUnavailable('Leonteq return history has duplicate dates')
        number = _number(value)
        if number <= 0:
            raise ApiUnavailable('Leonteq return history has an invalid index value')
        marks[stamp] = number
    dates = sorted(marks)
    if len(dates) < 2:
        raise ApiUnavailable('Leonteq return history has no complete window')
    year_start = f'{dates[-1][:4]}-01-01'
    previous = [stamp for stamp in dates if stamp < year_start]
    if previous:
        anchor = previous[-1]
    else:
        issue = details.get('issueDate')
        if not issue or issue[:10] < year_start:
            raise ApiUnavailable('Leonteq return history lacks the year-opening index mark')
        anchor = dates[0]
    reported = next((row for row in history.get('performances') or [] if row.get('nameOrIsin') == name), None)
    if not reported:
        raise ApiUnavailable('Leonteq has no matching published YTD return')
    ytd = _number(reported.get('performanceYTD'))
    measured = (marks[dates[-1]] / marks[anchor] - 1) * 100
    if anchor >= dates[-1] or abs(measured - ytd) > Decimal('.0051'):
        raise ApiUnavailable('Leonteq return dates do not reconcile to its published YTD')
    return {'portfolio_name': name, 'currency': 'EUR', 'from': anchor, 'to': dates[-1],
            'ytd_pct': float(ytd), 'retrieved_at': retrieved_at, 'source': 'lynqs_api'}


def _fresh(window, name, catalogue_fetched_at):
    try:
        start, end = date.fromisoformat(window['from']), date.fromisoformat(window['to'])
        fetched = datetime.fromisoformat(window['retrieved_at'].replace('Z', '+00:00'))
        catalogue = datetime.fromisoformat(catalogue_fetched_at.replace('Z', '+00:00')) if catalogue_fetched_at else None
        _number(window['ytd_pct'])
        return (window['portfolio_name'] == name and window['currency'] == 'EUR' and start < end
                and (catalogue is None or fetched >= catalogue))
    except (KeyError, TypeError, ValueError):
        return False


def cached(source):
    name = source.get('leonteq_name')
    performance = source.get('performance') or {}
    accounting = source.get('accounting') or {}
    candidates = [source.get('return_window'), performance.get('return_window')]
    if accounting.get('history') and accounting.get('index_details'):
        try:
            candidates.append(parse(accounting['history'], accounting['index_details'], name, accounting['retrieved_at']))
        except (ApiUnavailable, KeyError, TypeError, ValueError):
            pass
    valid = [window for window in candidates if isinstance(window, dict)
             and _fresh(window, name, source.get('catalogue_retrieved_at'))]
    return max(valid, key=lambda window: window['retrieved_at']) if valid else None


def read(name):
    """Only index history/details: no opening holdings, transactions, or AIRS pairing."""
    from leonteq.portal_login import check_login, _login_lock
    from leonteq.session import session, on_session_thread
    from leonteq.portal_api import read as read_api
    login = check_login()
    if not login.get('success'):
        raise ApiUnavailable('Leonteq return dates could not be read. Refresh this portfolio.')
    if not _login_lock.acquire(blocking=False):
        raise ApiUnavailable('Another Leonteq read is running. Retry shortly.')
    try:
        def acquire():
            result = read_api(session.resources[3], session.portfolio_ids, name, history=True)
            return parse(result['history'], result['details'], name, datetime.now(timezone.utc).isoformat())
        return on_session_thread(acquire)
    finally:
        _login_lock.release()


def ensure(source):
    window = cached(source)
    if window:
        return window, None
    name = source.get('leonteq_name')
    if not name:
        return None, 'Leonteq return identity is unavailable. Refresh this portfolio.'
    try:
        window = read(name)
    except (ApiUnavailable, KeyError, TypeError, ValueError) as exc:
        return None, str(exc) if isinstance(exc, ApiUnavailable) else 'Leonteq return dates are unavailable. Refresh this portfolio.'
    except Exception as exc:
        log.warning('Leonteq return-window transport failed (%s)', type(exc).__name__)
        return None, 'Leonteq return dates could not be read. Refresh this portfolio.'
    # Store the independently acquired window without restamping old monthly returns.
    from deps import supabase
    from leonteq.portfolio_cache import _account_key
    existing = source.get('performance') or {}
    payload = {**existing, 'success': True, 'status': 'success', 'return_window': window,
               'months': existing.get('months') or [], 'yearly_totals': existing.get('yearly_totals') or {},
               'retrieved_at': existing.get('retrieved_at') or window['retrieved_at']}
    try:
        supabase.table('leonteq_performance_snapshot').upsert({'account_key': _account_key(),
            'portfolio_name': name, 'payload': payload, 'retrieved_at': payload['retrieved_at']},
            on_conflict='account_key,portfolio_name').execute()
    except Exception:
        log.warning('Leonteq return window could not be cached')
    return window, None

"""AIRS quoted-price changes, with explicit inventory changes and missing weeks."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from math import isfinite
from zoneinfo import ZoneInfo

from deps import supabase


def week_end(day: date) -> date:
    return day + timedelta(days=4 - day.weekday())


def save_snapshot(account: str, as_of: str, holdings: list[dict], *, client=None) -> None:
    quotes = [{key: row.get(key) for key in ('isin', 'holding_name', 'currency',
               'current_price_local', 'valuation_component', 'quantity', 'quantity_exact',
               'current_value_eur')} for row in holdings]
    (client if client is not None else supabase).rpc('save_airs_weekly_holding_snapshot', {
        'p_portefeuille': account, 'p_as_of': as_of, 'p_holdings': quotes}).execute()


def _quotes(rows):
    grouped = {}
    for row in rows:
        name = str(row.get('holding_name') or '').strip()
        if not name or row.get('valuation_component') == 'accrued_interest':
            continue
        quantity = row.get('quantity_exact') if row.get('quantity_exact') is not None else row.get('quantity')
        if quantity == 0 and row.get('current_value_eur') == 0:
            continue  # A closed position retained for reporting is not a held instrument.
        # Cash has no instrument return. Never turn its balance/quantity into a quote.
        if not row.get('isin') and name.casefold() in {'liquiditeiten', 'cash', 'kasgeld', 'liquidities'}:
            continue
        currency = str(row.get('currency') or '').strip()
        if currency.casefold() in {'nan', 'none', 'unknown', '?', '-'}:
            currency = ''
        identity = str(row.get('isin') or 'name:' + name.casefold()) + '|' + currency
        quote = row.get('current_price_local')
        price = float(quote) if quote is not None else None
        if price is not None and (not currency or not isfinite(price) or price <= 0):
            price = None
        if identity not in grouped:
            grouped[identity] = {'id': identity, 'name': name, 'isin': row.get('isin'),
                                 'currency': currency, 'price': price}
        elif grouped[identity]['price'] != price:
            # Different quotes for one identity are ambiguous; don't average prices.
            grouped[identity]['price'] = None
    return grouped


def matrix(account: str, snapshots: list[dict], weeks: int, through: date) -> dict:
    end = week_end(through)
    ends = [end - timedelta(weeks=i) for i in reversed(range(weeks + 1))]
    by_week = {}
    for snapshot in snapshots:
        actual = date.fromisoformat(str(snapshot['as_of_date'])[:10])
        if actual > through:
            continue
        key = week_end(actual).isoformat()
        if key not in by_week or str(snapshot['as_of_date']) > str(by_week[key]['as_of_date']):
            by_week[key] = snapshot
    periods, instruments, seen = [], {}, set()
    previous = None
    previous_date = None
    for index, end in enumerate(ends):
        snapshot = by_week.get(end.isoformat())
        current = _quotes(snapshot['holdings']) if snapshot else None
        actual = str(snapshot['as_of_date'])[:10] if snapshot else None
        if index:
            periods.append({'week_end': end.isoformat(), 'as_of_date': actual,
                            'from_date': previous_date, 'partial': end > through,
                            'available': snapshot is not None})
            identities = set(instruments) | set(current or {}) | set(previous or {})
            for key in identities:
                now, before = (current or {}).get(key), (previous or {}).get(key)
                info = now or before
                if key not in instruments and info:
                    instruments[key] = {**{k: info[k] for k in ('id', 'name', 'isin', 'currency')},
                                        'weeks': [{'status': ('missing_snapshot' if not p['available']
                                                   else 'missing_previous' if p['from_date'] is None else 'not_held'),
                                                   'return_pct': None, 'price': None}
                                                  for p in periods[:-1]]}
                if now:
                    instruments[key]['name'] = now['name']
                change = None
                if current is None:
                    state = 'missing_snapshot'
                elif previous is None:
                    state = 'missing_previous'
                elif not now:
                    state = 'sold' if before else 'not_held'
                elif not before:
                    state = 'reentered' if key in seen else 'added'
                elif before['price'] is None or now['price'] is None:
                    state = 'missing_price'
                else:
                    change = (now['price'] / before['price'] - 1) * 100
                    state = 'priced'
                instruments[key]['weeks'].append({'status': state, 'return_pct': change,
                                                   'price': now['price'] if now else None,
                                                   'from_price': before['price'] if before else None})
        seen.update(current or {})
        previous, previous_date = current, actual
    for key, instrument in instruments.items():
        instrument['held'] = key in previous if previous is not None else None
    return {'portefeuille': account, 'through': through.isoformat(), 'periods': periods,
            'instruments': sorted(instruments.values(), key=lambda r: (r['name'].casefold(), r['currency'], r['id'])),
            'missing_weeks': sum(not period['available'] for period in periods),
            'basis': 'AIRS Huidige koers; local quoted price, not realised or total return'}


def read_returns(account: str, weeks: int = 12) -> dict:
    from airs_vermogen import latest_completed_market_date
    from routers._airs_monthly_report import _account
    _account(account)
    through = date.fromisoformat(latest_completed_market_date(datetime.now(ZoneInfo('Europe/Amsterdam')).date()))
    first = week_end(through) - timedelta(weeks=weeks)
    rows = (supabase.table('airs_weekly_holding_snapshot').select('week_end,as_of_date,holdings')
            .eq('portefeuille', account).gte('week_end', first.isoformat())
            .lte('week_end', week_end(through).isoformat()).order('week_end').execute().data or [])
    return matrix(account, rows, weeks, through)


def collect_history(account: str, weeks: int, ctx) -> str:
    from airs_vermogen import _LOCK, _acquire_session
    from airs_scanner import AirsNoData, download_vermogensoverzicht_sync
    from dashboard_daily import daily_lease
    from portfolio import parse_airs_excel
    from dataclasses import asdict

    ctx.check()
    saved = read_returns(account, weeks)
    through = date.fromisoformat(saved['through'])
    ends = [week_end(through) - timedelta(weeks=i) for i in range(weeks, -1, -1)]
    have = {row['week_end']: row['as_of_date'] for row in (
        supabase.table('airs_weekly_holding_snapshot').select('week_end,as_of_date')
        .eq('portefeuille', account).execute().data or [])}
    targets = [min(end, through) for end in ends
               if not have.get(end.isoformat()) or have[end.isoformat()] < min(end, through).isoformat()]
    if not targets:
        return 'All requested weekly snapshots are already saved.'
    complete, unavailable = 0, []
    with daily_lease('airs_weekly_returns', 'manual'):
        if not _acquire_session(30):
            raise RuntimeError('AIRS is busy with another refresh. Please try again shortly.')
        try:
            for index, target in enumerate(reversed(targets)):
                ctx.check()
                ctx.progress(index, len(targets), f'Loading weekly prices: {target.isoformat()}')
                for back in range(target.weekday() + 1):
                    ctx.check()
                    actual = (target - timedelta(days=back)).isoformat()
                    try:
                        blob = download_vermogensoverzicht_sync(account, actual[:4] + '-01-01', actual)
                    except AirsNoData:
                        continue
                    except RuntimeError as error:
                        # AIRS answers an unvalued date with its known tiny response.
                        # Authentication, transport and parsing failures still fail the job.
                        if str(error).startswith('Response too small ('):
                            continue
                        raise
                    holdings = [asdict(holding) for holding in parse_airs_excel(blob)]
                    if not holdings:
                        continue
                    # Never put historical quotes into the live holdings cache.
                    save_snapshot(account, actual, holdings)
                    complete += 1
                    ctx.emit('diagnostic', 'Weekly snapshot saved', details={
                        'requested': target.isoformat(), 'as_of': actual, 'positions': len(holdings)})
                    break
                else:
                    unavailable.append(target.isoformat())
        finally:
            _LOCK.release()
    ctx.emit('diagnostic', 'Weekly history results', details={'saved': complete, 'unavailable': unavailable})
    ctx.progress(len(targets), len(targets), 'Weekly price history saved')
    return f'{complete} weekly snapshots saved' + (f'; {len(unavailable)} weeks unavailable in AIRS' if unavailable else '')

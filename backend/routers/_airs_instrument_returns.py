"""One instrument's saved weekly AIRS returns, starting on 1 January 2026."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from deps import supabase
from routers._airs_weekly_returns import _quotes, _observations_matrix, week_end

START = date(2026, 1, 1)
TABLE = 'airs_weekly_instrument_snapshot'


def _name(value):
    return ' '.join(value.casefold().split())


def _quote_for(holdings, instrument):
    quotes = _quotes(holdings)
    if instrument['id'] in quotes:
        return quotes[instrument['id']]
    aliases = [row for row in quotes.values() if not row['isin'] and row['currency'] == instrument['currency']
               and _name(row['name']) in instrument.get('name_aliases', [])]
    if aliases:
        prices = {row['price'] for row in aliases}
        return {**instrument, 'price': aliases[0]['price'] if len(prices) == 1 else None}
    if instrument['isin'] and any(not row['isin'] and row['currency'] == instrument['currency']
                                  and row['name'].casefold() == instrument['name'].casefold()
                                  for row in quotes.values()):
        # An old report without ISIN does not prove the selected instrument was
        # absent. Re-request its identity/quote instead of inventing a sale.
        return {**instrument, 'price': None}
    return None


def targets(through):
    if through < START:
        return []
    out = [START - timedelta(days=1)]
    end = week_end(START) + timedelta(days=2)
    while end < through:
        out.append(end)
        end += timedelta(weeks=1)
    out.append(through if end > through else end)
    return out


def latest_date():
    # Custodian reports can be valued on Saturdays/Sundays. Use the completed
    # calendar day in Amsterdam, not an equity exchange's weekday calendar.
    return datetime.now(ZoneInfo('Europe/Amsterdam')).date() - timedelta(days=1)


def _pages(query):
    rows, offset = [], 0
    while True:
        page = query.range(offset, offset + 499).execute().data or []
        rows.extend(page)
        if len(page) < 500:
            return rows
        offset += len(page)


def state(account, instrument_id=None):
    from routers._airs_monthly_report import _account
    _account(account)
    through = latest_date()
    snapshots = _pages(supabase.table('airs_weekly_holding_snapshot').select('as_of_date,holdings')
                       .eq('portefeuille', account).lte('as_of_date', through.isoformat())
                       .order('as_of_date', desc=True))
    catalog, named = {}, {}
    for snapshot in snapshots:
        # Zero-quantity reporting rows also identify instruments previously held.
        metadata = [{k: v for k, v in row.items() if k not in ('quantity', 'quantity_exact', 'current_value_eur')}
                    for row in snapshot['holdings']]
        for key, row in _quotes(metadata).items():
            catalog.setdefault(key, {k: row[k] for k in ('id', 'name', 'isin', 'currency')})
            if row['isin']:
                named.setdefault((_name(row['name']), row['currency']), set()).add(key)
    aliases = {}
    for key, row in list(catalog.items()):
        candidates = named.get((_name(row['name']), row['currency']), set())
        if not row['isin'] and len(candidates) == 1:
            aliases[key] = next(iter(candidates))
            del catalog[key]
    options = sorted(catalog.values(), key=lambda row: (row['name'].casefold(), row['currency'], row['id']))
    instrument_id = aliases.get(instrument_id, instrument_id)
    if instrument_id and instrument_id not in catalog:
        raise HTTPException(404, 'This instrument is not in the saved AIRS portfolio history')
    chosen = catalog.get(instrument_id) if instrument_id else (options[0] if options else None)
    if chosen and chosen['isin']:
        chosen = {**chosen, 'name_aliases': [name for (name, currency), ids in named.items()
                                           if currency == chosen['currency'] and ids == {chosen['id']}]}
    individual = []
    if chosen:
        individual = _pages(supabase.table(TABLE).select('target_date,as_of_date,holdings,status,checked_at')
                            .eq('portefeuille', account).in_('instrument_id', [chosen['id'], *[key for key, value in aliases.items() if value == chosen['id']]])
                            .gte('target_date', (START - timedelta(days=1)).isoformat())
                            .lte('target_date', through.isoformat()).order('target_date'))
    return through, snapshots, individual, options, chosen


def observations(instrument, dates, snapshots, individual, through):
    """Select actual observations without borrowing a different week's price."""
    out, missing = [], []
    for target in dates:
        candidates = []
        for snapshot in snapshots:
            actual = date.fromisoformat(str(snapshot['as_of_date'])[:10])
            limit = target if target < START else through
            if actual <= limit and week_end(actual) == week_end(target):
                row = _quote_for(snapshot['holdings'], instrument)
                candidates.append((actual, 2 if row and row['price'] is not None else 0, row))
        for snapshot in individual:
            requested = date.fromisoformat(str(snapshot['target_date'])[:10])
            if (requested == dates[0]) != (target == dates[0]):
                continue  # An opening observation is never a week-ending observation.
            if week_end(requested) != week_end(target):
                continue
            if snapshot['status'] == 'observed':
                actual = date.fromisoformat(str(snapshot['as_of_date'])[:10])
                if actual <= (target if target < START else through):
                    row = _quote_for(snapshot['holdings'], instrument)
                    candidates.append((actual, 3 if row and row['price'] is not None else 1, row))
        best = max(candidates, key=lambda item: (item[0], item[1])) if candidates else None
        if best:
            actual, _, row = best
            holdings = [] if row is None else [{'isin': row['isin'], 'holding_name': row['name'],
                        'currency': row['currency'], 'current_price_local': row['price']}]
            out.append((target, {'as_of_date': actual.isoformat(), 'holdings': holdings}))
            needs_price = row is not None and row['price'] is None
        else:
            out.append((target, None))
            needs_price = True
        if needs_price:
            missing.append(target)
    return out, missing


def read(account, instrument_id=None):
    through, snapshots, individual, options, chosen = state(account, instrument_id)
    dates = targets(through)
    if chosen and dates:
        points, missing = observations(chosen, dates, snapshots, individual, through)
        data = _observations_matrix(account, points, through, seed=[chosen])
        selected = data['instruments'][0]
        selected.pop('name_aliases', None)
        periods = data['periods']
        for period in periods:
            period['partial'] = week_end(date.fromisoformat(period['target_date'])) + timedelta(days=2) > through
    else:
        selected, periods, missing = None, [], []
    return {'portefeuille': account, 'start_date': START.isoformat(), 'through': through.isoformat(),
            'options': options, 'selected': selected, 'periods': periods, 'missing_observations': len(missing)}


def collect(account, instrument_id, ctx):
    from airs_scanner import AirsNoData, download_vermogensoverzicht_sync
    from airs_vermogen import _LOCK, _acquire_session
    from dashboard_daily import daily_lease
    from portfolio import parse_airs_excel

    ctx.check()
    through, snapshots, individual, _, instrument = state(account, instrument_id)
    instrument_id = instrument['id']
    _, pending = observations(instrument, targets(through), snapshots, individual, through)
    if not pending:
        return 'No missing history to request for this instrument.'
    saved, quotes_by_date = 0, {}
    with daily_lease('airs_instrument_history', 'manual'):
        if not _acquire_session(30):
            raise RuntimeError('AIRS is busy with another refresh. Please try again shortly.')
        try:
            ctx.progress(0, len(pending), f"{instrument['name']} · loading missing history")
            for index, target in enumerate(pending):
                ctx.check()
                entry = {'portefeuille': account, 'instrument_id': instrument_id,
                         'target_date': target.isoformat(), 'as_of_date': None, 'holdings': [], 'status': 'unavailable'}
                for back in range(target.weekday() + 1):
                    ctx.check()
                    actual = (target - timedelta(days=back)).isoformat()
                    if actual not in quotes_by_date:
                        try:
                            blob = download_vermogensoverzicht_sync(account, actual[:4] + '-01-01', actual)
                        except AirsNoData:
                            quotes_by_date[actual] = (False, None)
                        except RuntimeError as error:
                            if not str(error).startswith('Response too small ('):
                                raise
                            quotes_by_date[actual] = (False, None)
                        else:
                            holdings = [asdict(row) for row in parse_airs_excel(blob)]
                            quotes_by_date[actual] = (bool(holdings), _quote_for(holdings, instrument))
                    available, quote = quotes_by_date[actual]
                    if not available:
                        continue
                    entry.update(as_of_date=actual, status='observed', holdings=[] if quote is None else [{
                        'isin': quote['isin'], 'holding_name': quote['name'], 'currency': quote['currency'],
                        'current_price_local': quote['price']}])
                    break
                entry['checked_at'] = datetime.now(timezone.utc).isoformat()
                supabase.table(TABLE).upsert(entry, on_conflict='portefeuille,instrument_id,target_date').execute()
                saved += 1
                # Publish progress only after the quote is committed, so clients
                # can immediately reread this week's saved chart observation.
                ctx.progress(index + 1, len(pending), f"{instrument['name']} · {target.isoformat()}")
                ctx.emit('diagnostic', 'Instrument history checked', details={
                    'instrument_id': instrument_id, 'target': entry['target_date'],
                    'as_of': entry['as_of_date'], 'status': entry['status'],
                    'has_price': bool(entry['holdings'] and entry['holdings'][0]['current_price_local'] is not None)})
        finally:
            _LOCK.release()
    ctx.progress(len(pending), len(pending), f"{instrument['name']} · history checked")
    return f"{instrument['name']}: {saved} missing observations checked."

"""One instrument's saved weekly AIRS returns, with resumable historical discovery."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from deps import supabase
from routers._airs_weekly_returns import _quotes, _observations_matrix, week_end

START = date(2026, 1, 1)
TABLE = 'airs_weekly_instrument_snapshot'


def _name(value):
    return ' '.join(value.casefold().split())


def _quote_for(holdings, instrument):
    return _instrument_quote(_quotes(holdings), instrument)


def _instrument_quote(quotes, instrument):
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


def targets(through, start=START):
    if through < start:
        return []
    out = [start - timedelta(days=1)]
    end = week_end(start) + timedelta(days=2)
    while end < through:
        out.append(end)
        end += timedelta(weeks=1)
    out.append(through if end > through else end)
    return out


def latest_date():
    # Ask for today's AIRS report; collection can fall back within the week if it is not published.
    return datetime.now(ZoneInfo('Europe/Amsterdam')).date()


def _pages(query):
    rows, offset = [], 0
    while True:
        page = query.range(offset, offset + 499).execute().data or []
        rows.extend(page)
        if len(page) < 500:
            return rows
        offset += len(page)


def _snapshots(account):
    from routers._airs_monthly_report import _account
    _account(account)
    through = latest_date()
    snapshots = _pages(supabase.table('airs_weekly_holding_snapshot').select('as_of_date,holdings')
                       .eq('portefeuille', account).lte('as_of_date', through.isoformat())
                       .order('as_of_date', desc=True))
    return through, snapshots


def _catalogue(snapshots):
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
    return options, catalog, aliases, named


def _with_aliases(instrument, named):
    if not instrument or not instrument["isin"]:
        return instrument
    return {**instrument, "name_aliases": [name for (name, currency), ids in named.items()
            if currency == instrument["currency"] and ids == {instrument["id"]}]}


def state(account, instrument_id=None):
    through, snapshots = _snapshots(account)
    options, catalog, aliases, named = _catalogue(snapshots)
    instrument_id = aliases.get(instrument_id, instrument_id)
    if instrument_id and instrument_id not in catalog:
        raise HTTPException(404, 'This instrument is not in the saved AIRS portfolio history')
    chosen = catalog.get(instrument_id) if instrument_id else (options[0] if options else None)
    chosen = _with_aliases(chosen, named)
    individual = []
    if chosen:
        individual = _pages(supabase.table(TABLE).select('target_date,as_of_date,holdings,status,checked_at')
                            .eq('portefeuille', account).in_('instrument_id', [chosen['id'], *[key for key, value in aliases.items() if value == chosen['id']]])
                            .lte('target_date', through.isoformat()).order('target_date'))
    return through, snapshots, individual, options, chosen


def observations(instrument, dates, snapshots, individual, through, quote_for=_quote_for, strict_dates=False):
    """Select actual observations without borrowing a different week's price."""
    out, missing = [], []
    snapshot_weeks, instrument_weeks = {}, {}
    for snapshot in snapshots:
        key = week_end(date.fromisoformat(str(snapshot['as_of_date'])[:10]))
        snapshot_weeks.setdefault(key, []).append(snapshot)
    for snapshot in individual:
        key = week_end(date.fromisoformat(str(snapshot['target_date'])[:10]))
        instrument_weeks.setdefault(key, []).append(snapshot)
    for target in dates:
        candidates = []
        for snapshot in snapshot_weeks.get(week_end(target), []):
            actual = date.fromisoformat(str(snapshot['as_of_date'])[:10])
            opening = target.month == 12 and target.day == 31
            limit = target if strict_dates or opening or target < START else through
            if actual <= limit and week_end(actual) == week_end(target):
                row = quote_for(snapshot['holdings'], instrument)
                candidates.append((actual, 2 if row and row['price'] is not None else 0, row))
        for snapshot in instrument_weeks.get(week_end(target), []):
            requested = date.fromisoformat(str(snapshot['target_date'])[:10])
            if (requested.month == 12 and requested.day == 31) != (target.month == 12 and target.day == 31):
                continue  # An opening observation is never a week-ending observation.
            if week_end(requested) != week_end(target):
                continue
            if snapshot['status'] == 'observed':
                actual = date.fromisoformat(str(snapshot['as_of_date'])[:10])
                if actual <= (target if strict_dates or target.month == 12 and target.day == 31 or target < START else through):
                    row = quote_for(snapshot['holdings'], instrument)
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


def history_start(individual, through):
    years = [date.fromisoformat(row['target_date'][:10]).year for row in individual
             if row['status'] == 'observed' and row['target_date'][5:10] != '12-31']
    return date(min([START.year, through.year, *years]), 1, 1)


def _series(account, through, snapshots, individual, chosen, quote_for=_quote_for, start=None, rolling=False):
    dates = targets(through, start or history_start(individual, through))
    if rolling:
        dates = []
        target = through
        lower = history_start(individual, through)
        while target >= lower:
            dates.append(target)
            target -= timedelta(weeks=1)
        dates.reverse()
    if chosen and dates:
        points, missing = observations(chosen, dates, snapshots, individual, through, quote_for, strict_dates=rolling)
        data = _observations_matrix(account, points, through, seed=[chosen])
        selected = data['instruments'][0]
        selected.pop('name_aliases', None)
        periods = data['periods']
        for period in periods:
            period['partial'] = week_end(date.fromisoformat(period['target_date'])) + timedelta(days=2) > through
    else:
        selected, periods, missing = None, [], []
    return {'selected': selected, 'periods': periods, 'missing_observations': len(missing)}


def _return(data):
    if not data['selected'] or not data['periods']:
        return None
    cells = data['selected']['weeks']
    first, last = cells[0].get('from_price'), cells[-1].get('price')
    if first is None or last is None or first <= 0 or last <= 0:
        return None
    value = (last / first - 1) * 100
    return {'value': value, 'from': data['periods'][0]['from_date'], 'to': data['periods'][-1]['as_of_date']} if isfinite(value) else None


def flat_history_cutoff(data):
    """User-selected discovery rule: two consecutive zero-return weeks end older history."""
    cells = (data.get('selected') or {}).get('weeks', [])
    run = 0
    for index in range(len(cells) - 1, -1, -1):
        cell = cells[index]
        value = cell.get('return_pct')
        run = run + 1 if cell.get('status') == 'priced' and value is not None and abs(value) <= 1e-10 else 0
        if run == 2:
            return data['periods'][index + 1]['target_date']
    return None


def _full_series(account, through, snapshots, individual, chosen, quote_for=_quote_for):
    checked = {row['target_date'] for row in individual}
    rolling = through.isoformat() in checked and (through - timedelta(weeks=1)).isoformat() in checked
    data = _series(account, through, snapshots, individual, chosen, quote_for, rolling=rolling)
    cutoff = flat_history_cutoff(data)
    if cutoff:
        keep = [index for index, period in enumerate(data['periods']) if period['target_date'] > cutoff]
        data = {**data, 'periods': [data['periods'][index] for index in keep],
                'selected': {**data['selected'], 'weeks': [data['selected']['weeks'][index] for index in keep]}}
        data['missing_observations'] = sum(not period['available'] or cell['status'] == 'missing_price'
                                           for period, cell in zip(data['periods'], data['selected']['weeks']))
    year = latest_date().year
    ytd = _series(account, through, snapshots, individual, chosen, quote_for, date(year, 1, 1))
    return {**data, 'start_date': cutoff or history_start(individual, through).isoformat(),
            'history_cutoff': cutoff, 'ytd_return': _return(ytd)}


def read(account, instrument_id=None):
    through, snapshots, individual, options, chosen = state(account, instrument_id)
    return {'portefeuille': account, 'start_date': START.isoformat(), 'through': through.isoformat(),
            'options': options, **_full_series(account, through, snapshots, individual, chosen)}


def bundle(account):
    """Read the catalogue and all saved series without per-instrument database queries."""
    from collections import defaultdict

    through, snapshots = _snapshots(account)
    options, _, aliases, named = _catalogue(snapshots)
    individual = defaultdict(list)
    if options:
        rows = _pages(supabase.table(TABLE).select('instrument_id,target_date,as_of_date,holdings,status')
                      .eq('portefeuille', account)
                      .lte('target_date', through.isoformat()).order('target_date').order('instrument_id'))
        for row in rows:
            key = row['instrument_id']
            individual[aliases.get(key, key)].append(row)

    quotes = {id(snapshot['holdings']): _quotes(snapshot['holdings']) for snapshot in snapshots}
    for rows in individual.values():
        for row in rows:
            quotes[id(row['holdings'])] = _quotes(row['holdings'])

    def quote_for(holdings, instrument):
        return _instrument_quote(quotes[id(holdings)], instrument)

    return {'portefeuille': account, 'start_date': START.isoformat(), 'through': through.isoformat(),
            'options': options, 'instruments': {
                option['id']: _full_series(account, through, snapshots, individual[option['id']],
                                      _with_aliases(option, named), quote_for)
                for option in options
            }}


def collect(account, instrument_id, ctx, full_history=False, earliest_date=None):
    if full_history:
        return collect_backwards(account, instrument_id, ctx, earliest_date=earliest_date)
    from airs_scanner import AirsNoData, download_vermogensoverzicht_sync
    from airs_vermogen import _LOCK, _acquire_session
    from dashboard_daily import daily_lease
    from portfolio import parse_airs_excel

    ctx.check()
    through, snapshots, individual, _, instrument = state(account, instrument_id)
    instrument_id = instrument["id"]
    _, pending = observations(instrument, targets(through), snapshots, individual, through)
    if not pending:
        return "No missing history to request for this instrument."
    saved, quotes_by_date = 0, {}
    with daily_lease("airs_instrument_history", "manual"):
        if not _acquire_session(30):
            raise RuntimeError("AIRS is busy with another refresh. Please try again shortly.")
        try:
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
                            if full_history and not holdings:
                                raise ValueError('AIRS returned an empty report; cannot confirm the historical boundary.')
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
                individual[:] = [row for row in individual if row['target_date'] != entry['target_date']]
                individual.append(entry)
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
    return f"{instrument['name']}: {saved} missing observations checked."


def collect_all(account, ctx, earliest_date, *, stop_on_flat=True):
    """Download each full report once and commit all instrument observations together."""
    from airs_scanner import AirsNoData, download_vermogensoverzicht_sync
    from airs_vermogen import _LOCK, _acquire_session
    from dashboard_daily import daily_lease
    from portfolio import parse_airs_excel

    ctx.check()
    through, snapshots = _snapshots(account)
    if not date(1900, 1, 1) <= earliest_date <= through:
        raise ValueError('Earliest date must be between 1 January 1900 and today.')
    options, _, _, named = _catalogue(snapshots)
    instruments = {row['id']: _with_aliases(row, named) for row in options}
    if not instruments:
        return 'No saved AIRS instruments to fetch.'
    cached = {}
    for row in _pages(supabase.table(TABLE).select('instrument_id,target_date,as_of_date,holdings,status')
                      .eq('portefeuille', account).gte('target_date', earliest_date.isoformat())
                      .lte('target_date', through.isoformat()).order('target_date').order('instrument_id')):
        cached.setdefault(row['target_date'], {})[row['instrument_id']] = row
    reports = {row['as_of_date'][:10]: row['holdings'] for row in snapshots}
    active = set(instruments)
    newer, zeros = {}, {}
    saved = 0
    total = (through - earliest_date).days // 7 + 1

    def observation(target, ids):
        prior = cached.get(target.isoformat(), {})
        selected = [prior[instrument_id] for instrument_id in ids if instrument_id in prior]
        dates = {row['as_of_date'] for row in selected}
        if (len(selected) == len(ids) and len(dates) == 1 and all(row['status'] == 'observed' for row in selected)):
            actual = next(iter(dates))
            if actual and earliest_date <= date.fromisoformat(actual) <= target and (target - date.fromisoformat(actual)).days < 7:
                return actual, {row['instrument_id']: (_quote_for(row['holdings'], instruments[row['instrument_id']]) or {}).get('price')
                                for row in selected}
        holdings, actual = None, None
        for back in range(7):
            ctx.check()
            day = target - timedelta(days=back)
            if day < earliest_date:
                break
            key = day.isoformat()
            if key not in reports:
                try:
                    blob = download_vermogensoverzicht_sync(account, f'{day.year}-01-01', key)
                except AirsNoData:
                    reports[key] = None
                except RuntimeError as error:
                    if not str(error).startswith('Response too small ('):
                        raise
                    reports[key] = None
                else:
                    reports[key] = [asdict(row) for row in parse_airs_excel(blob)]
                    if not reports[key]:
                        raise ValueError('AIRS returned an empty report; cannot confirm the historical boundary.')
            if reports[key] is not None:
                holdings, actual = reports[key], key
                break
        quotes = _quotes(holdings) if holdings is not None else {}
        rows, prices = [], {}
        checked = datetime.now(timezone.utc).isoformat()
        for instrument_id in sorted(ids):
            quote = _instrument_quote(quotes, instruments[instrument_id])
            prices[instrument_id] = quote['price'] if quote else None
            rows.append({'portefeuille': account, 'instrument_id': instrument_id,
                         'target_date': target.isoformat(), 'as_of_date': actual,
                         'status': 'observed' if actual else 'unavailable', 'checked_at': checked,
                         'holdings': [] if quote is None else [{'isin': quote['isin'], 'holding_name': quote['name'],
                                      'currency': quote['currency'], 'current_price_local': quote['price']}]})
        # A complete week's observations become visible before announcing progress.
        for offset in range(0, len(rows), 500):
            ctx.check()
            supabase.table(TABLE).upsert(rows[offset:offset + 500], on_conflict='portefeuille,instrument_id,target_date').execute()
        return actual, prices

    with daily_lease('airs_instrument_history', 'manual'):
        if not _acquire_session(30):
            raise RuntimeError('AIRS is busy with another refresh. Please try again shortly.')
        try:
            target = through
            while target >= earliest_date and active:
                ctx.check()
                actual, prices = observation(target, active)
                saved += 1
                ctx.progress(saved, total, f'{target.isoformat()} · saved weekly snapshot for {len(active)} instruments')
                if actual is None:
                    break
                for instrument_id, price in prices.items():
                    previous = newer.get(instrument_id)
                    zeros[instrument_id] = zeros.get(instrument_id, 0) + 1 if price and previous and abs(previous / price - 1) <= 1e-12 else 0
                    newer[instrument_id] = price
                    if stop_on_flat and zeros[instrument_id] >= 2:
                        active.discard(instrument_id)
                target -= timedelta(weeks=1)
            opening = date(through.year, 1, 1) - timedelta(days=1)
            if earliest_date <= opening <= through:
                ctx.check()
                ctx.progress(saved, total, 'Saving the separate YTD opening snapshot for all instruments…')
                observation(opening, instruments)
        finally:
            _LOCK.release()
    return f'{len(instruments)} instruments checked using {saved} shared weekly snapshots. Saved history is ready to open.'


def collect_backwards(account, instrument_id, ctx, earliest_date=None):
    """One continuous newest-to-oldest sequence. A weekly return needs two quote marks."""
    from airs_scanner import AirsNoData, download_vermogensoverzicht_sync
    from airs_vermogen import _LOCK, _acquire_session
    from dashboard_daily import daily_lease
    from portfolio import parse_airs_excel

    ctx.check()
    through, snapshots, individual, _, instrument = state(account, instrument_id)
    lower = earliest_date or date(1900, 1, 1)
    if not date(1900, 1, 1) <= lower <= through:
        raise ValueError('Earliest date must be between 1 January 1900 and today.')
    instrument_id = instrument['id']
    saved = 0

    def observation(target):
        nonlocal saved
        checked = next((row for row in individual if row['target_date'] == target.isoformat()
                        and row['status'] == 'unavailable'), None)
        if checked and target < through:
            return checked  # A saved historical boundary is resumable; today's missing report can be retried.
        candidates = []
        for row in [*snapshots, *(row for row in individual if row['status'] == 'observed')]:
            actual = date.fromisoformat(str(row['as_of_date'])[:10])
            if lower <= actual <= target and (target - actual).days < 7 and (
                    actual == target or row.get('target_date') == target.isoformat()):
                quote = _quote_for(row['holdings'], instrument)
                if quote is None or quote['price'] is not None:
                    candidates.append((actual, quote))
        entry = {'portefeuille': account, 'instrument_id': instrument_id, 'target_date': target.isoformat(),
                 'as_of_date': None, 'holdings': [], 'status': 'unavailable'}
        if candidates:
            actual, quote = max(candidates, key=lambda item: item[0])
            entry.update(as_of_date=actual.isoformat(), status='observed')
        else:
            for back in range(7):
                ctx.check()
                actual = target - timedelta(days=back)
                if actual < lower:
                    break
                try:
                    blob = download_vermogensoverzicht_sync(account, f'{actual.year}-01-01', actual.isoformat())
                except AirsNoData:
                    continue
                except RuntimeError as error:
                    if str(error).startswith('Response too small ('):
                        continue
                    raise
                holdings = [asdict(row) for row in parse_airs_excel(blob)]
                if not holdings:
                    raise ValueError('AIRS returned an empty report; cannot confirm the historical boundary.')
                quote = _quote_for(holdings, instrument)
                entry.update(as_of_date=actual.isoformat(), status='observed')
                break
        if entry['status'] == 'observed' and quote is not None:
            entry['holdings'] = [{'isin': quote['isin'], 'holding_name': quote['name'],
                                  'currency': quote['currency'], 'current_price_local': quote['price']}]
        entry['checked_at'] = datetime.now(timezone.utc).isoformat()
        supabase.table(TABLE).upsert(entry, on_conflict='portefeuille,instrument_id,target_date').execute()
        individual[:] = [row for row in individual if row['target_date'] != entry['target_date']]
        individual.append(entry)
        saved += 1
        ctx.progress(saved, 0, f"{instrument['name']} · weekly report {target.isoformat()}")
        return entry

    with daily_lease('airs_instrument_history', 'manual'):
        if not _acquire_session(30):
            raise RuntimeError('AIRS is busy with another refresh. Please try again shortly.')
        try:
            target, newer_price, zeros = through, None, 0
            while target >= lower:
                ctx.check()
                entry = observation(target)
                if entry['status'] == 'unavailable':
                    reason = f'No AIRS reports available for the week through {target.isoformat()}.'
                    break
                quote = _quote_for(entry['holdings'], instrument)
                price = quote['price'] if quote else None
                zeros = zeros + 1 if price is not None and newer_price is not None and abs(newer_price / price - 1) <= 1e-12 else 0
                if zeros >= 2:
                    reason = f'Stopped at {target.isoformat()}: two consecutive 0% weeks.'
                    break
                newer_price = price
                target -= timedelta(weeks=1)
            else:
                if earliest_date:
                    reason = f'Reached the requested earliest date {lower.isoformat()}.'
                else:
                    raise RuntimeError('Reached 1900 without a history boundary; earlier history was not checked.')
            # This separate anchor never changes the newest-to-oldest discovery sequence.
            opening = date(through.year, 1, 1) - timedelta(days=1)
            _, missing = observations(instrument, [opening], snapshots, individual, through)
            if missing and opening >= lower:
                ctx.check()
                ctx.progress(saved, 0, 'Checking the separate YTD opening price…')
                observation(opening)
        finally:
            _LOCK.release()
    return f"{instrument['name']}: {saved} weekly observations checked. {reason}"

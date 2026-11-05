"""Read LYNQS Details monthly returns; compare with saved AIRS monthly returns."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from fastapi import HTTPException
from deps import supabase
from leonteq.portfolio_cache import _account_key

TABLE = 'leonteq_performance_snapshot'
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

_READY_PERFORMANCE_TABLES = r"""() => {
    const tables = Array.from(document.querySelectorAll('table'), table =>
        Array.from(table.rows, row => Array.from(row.cells, cell => cell.innerText.trim())));
    const expected = ['year', 'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'total'];
    const ready = tables.some(rows => rows.length > 1 &&
        expected.every((label, index) => rows[0][index]?.toLowerCase() === label) &&
        rows.slice(1).some(cells => /^\d{4}$/.test(cells[0] || '') &&
            cells.length >= 14 && cells.slice(1, 14).every(value =>
                /^(?:[-–—]|[+\-−]?\d+(?:[.,]\d+)?\s*%)$/.test(value))));
    return ready ? tables : false;
}"""


def _refresh_airs(name: str):
    from airs_scanner import download_portfolio_sync
    from airs_vermogen import _acquire_session, _LOCK, latest_completed_market_date
    from routers.airs import _parse_att_excel, _save_performance_to_db
    end = latest_completed_market_date()
    if not _acquire_session(wait=30):
        raise HTTPException(409, 'airs_busy')
    try:
        try:
            raw = download_portfolio_sync(name, f'{end[:4]}-01-01', end)
        except Exception:
            raise HTTPException(502, 'airs_download_failed') from None
        try:
            rows = _parse_att_excel(raw)
        except Exception:
            raise HTTPException(502, 'airs_parse_failed') from None
        if not rows:
            raise HTTPException(502, 'airs_empty_performance')
        try:
            _save_performance_to_db(name, rows, prune_after=end)
        except Exception:
            raise HTTPException(502, 'airs_store_failed') from None
    finally:
        _LOCK.release()


def parse_tables(tables: list[list[list[str]]]) -> list[dict]:
    for table in tables:
        if not table:
            continue
        header = [cell.strip().lower() for cell in table[0]]
        if header[:13] != ['year', *MONTHS]:
            continue
        result = []
        years = set()
        for cells in table[1:]:
            if len(cells) < 13 or not re.fullmatch(r'\d{4}', cells[0].strip()):
                continue
            year = int(cells[0].strip())
            if year in years:
                raise ValueError('duplicate year')
            years.add(year)
            for month, text in enumerate(cells[1:13], 1):
                text = text.strip().replace('−', '-').replace(',', '.')
                if text in ('', '-', '—', '–'):
                    value = None
                elif re.fullmatch(r'[+-]?\d+(?:\.\d+)?\s*%', text):
                    value = float(text.rstrip('%').strip())
                else:
                    raise ValueError('invalid monthly return')
                result.append({'month': f'{year}-{month:02}', 'return_pct': value})
        if result:
            return result
    raise ValueError('monthly performance table missing')


def read_details(page, name):
    from playwright.sync_api import Error, TimeoutError
    from leonteq.portal_login import browser_diagnostic
    import re
    stage = 'portfolio_selection'
    try:
        names = page.locator('.indices-table__index-name .font-medium')
        names.first.wait_for(state='visible', timeout=25000)
        selected = names.filter(has_text=re.compile(r'^\s*' + re.escape(name) + r'\s*$'))
        if selected.count() != 1:
            return {'success': False, 'status': 'portfolio_not_found'}
        selected.click()
        stage = 'details_tab'
        page.get_by_text('Details', exact=True).click()
        stage = 'performance_heading'
        page.get_by_text('Performance Metrics', exact=True).wait_for(state='visible', timeout=25000)
        # Angular paints the header and skeleton rows before the performance
        # request resolves. Wait for values, not merely a visible table.
        stage = 'monthly_rows_loading'
        snapshot = page.wait_for_function(_READY_PERFORMANCE_TABLES, timeout=25000)
        try:
            tables = snapshot.json_value()
        finally:
            snapshot.dispose()
        stage = 'monthly_table_parse'
        months = parse_tables(tables)
        totals = {}
        for table in tables:
            if not table or [cell.strip().lower() for cell in table[0]][:13] != ['year', *MONTHS]:
                continue
            headers = [cell.strip().lower() for cell in table[0]]
            if 'total' not in headers:
                continue
            column = headers.index('total')
            for cells in table[1:]:
                if len(cells) > column and re.fullmatch(r'\d{4}', cells[0].strip()):
                    text = cells[column].strip().replace('−', '-').replace(',', '.')
                    if re.fullmatch(r'[+-]?\d+(?:\.\d+)?\s*%', text):
                        totals[cells[0].strip()] = float(text.rstrip('%').strip())
        return {'success': True, 'status': 'success', 'portfolio_name': name,
                'months': months, 'yearly_totals': totals, 'retrieved_at': datetime.now(timezone.utc).isoformat()}
    except TimeoutError as exc:
        return {'success': False, 'status': 'performance_failed', 'retryable': True, 'diagnostics': browser_diagnostic(exc, stage)}
    except ValueError:
        return {'success': False, 'status': 'performance_failed', 'diagnostics': {'stage': stage, 'reason': 'invalid_table_data'}}
    except Error as exc:
        return {'success': False, 'status': 'browser_error', 'diagnostics': browser_diagnostic(exc, stage)}


def comparison(name: str, portfolio_id: int, refresh: bool = False):
    from leonteq.portfolio_links import options
    from leonteq.portal_login import check_performance
    available = options('dynamic')
    link = next((link for link in available['links'] if link['leonteq_name'] == name and link['airs_portfolio_id'] == portfolio_id), None)
    if not link:
        raise HTTPException(404, 'dynamic_connection_not_found')
    account = _account_key()
    rows = (supabase.table(TABLE).select('payload').eq('account_key', account).eq('portfolio_name', name).limit(1).execute().data or [])
    saved = rows[0]['payload'] if rows else None
    source = saved
    airs_error = None
    refresh_errors = []
    if refresh:
        try:
            _refresh_airs(link['airs_name'])
        except Exception as exc:
            airs_error = 'airs_refresh_failed'
            allowed = {'airs_busy', 'airs_download_failed', 'airs_parse_failed', 'airs_empty_performance', 'airs_store_failed'}
            code = exc.detail if isinstance(exc, HTTPException) and exc.detail in allowed else 'timeout' if 'timeout' in type(exc).__name__.lower() else 'airs_refresh_failed'
            refresh_errors.append({'source': 'AIRS', 'code': code})
    if refresh or not saved or 'yearly_totals' not in saved:
        fresh = check_performance(name)
        if fresh.get('success'):
            supabase.table(TABLE).upsert({'account_key': account, 'portfolio_name': name, 'payload': fresh,
                'retrieved_at': fresh['retrieved_at']}, on_conflict='account_key,portfolio_name').execute()
            source = fresh
        elif saved:
            source = {**saved, 'refresh_error': fresh.get('status')}
            refresh_errors.append({'source': 'Leonteq', 'code': fresh.get('status', 'performance_failed'),
                                   'diagnostics': fresh.get('diagnostics'), 'attempts': fresh.get('attempts')})
        else:
            return {**fresh, 'refresh_errors': [*refresh_errors, {'source': 'Leonteq', 'code': fresh.get('status', 'performance_failed'),
                    'diagnostics': fresh.get('diagnostics'), 'attempts': fresh.get('attempts')}], 'saved_data': False}
    # AIRS is downloaded only on explicit refresh; otherwise read stored reports.
    performance, offset = [], 0
    while True:
        batch = (supabase.table('airs_performance').select('periode,rendement,cumulatief_rendement,fetched_at')
                 .eq('portefeuille', link['airs_name']).order('periode').order('fetched_at')
                 .range(offset, offset + 499).execute().data or [])
        performance.extend(batch)
        if len(batch) < 500:
            break
        offset += len(batch)
    monthly = {}
    for row in sorted(performance, key=lambda r: (r['periode'], r.get('fetched_at') or '')):
        monthly[row['periode'][:7]] = row
    left = {row['month']: row['return_pct'] for row in source['months']}
    result = []
    for month in sorted(set(left) | set(monthly), reverse=True):
        l = left.get(month)
        r = monthly.get(month, {}).get('rendement')
        result.append({'month': month, 'leonteq_pct': l, 'airs_pct': r,
                       'difference_pp': l - r if l is not None and r is not None else None,
                       'airs_as_of': monthly.get(month, {}).get('periode')})
    yearly = {}
    for month, row in sorted(monthly.items()):
        yearly[month[:4]] = row.get('cumulatief_rendement')
    ytd = {}
    for year in set(yearly) | set(source.get('yearly_totals', {})):
        l, r = source.get('yearly_totals', {}).get(year), yearly.get(year)
        ytd[year] = {'leonteq_pct': l, 'airs_pct': r, 'difference_pp': l-r if l is not None and r is not None else None}
    return {'success': True, 'status': 'success', 'months': result, 'ytd': ytd, 'retrieved_at': source['retrieved_at'],
            'refresh_error': source.get('refresh_error') or airs_error, 'refresh_errors': refresh_errors,
            'saved_data': bool(refresh_errors), 'airs_name': link['airs_name']}

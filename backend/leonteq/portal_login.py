"""A bounded, server-side LYNQS login check. Never return credentials or cookies."""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

INDICES_URL = 'https://www.lynqs.com/eportal/#/amc/indices'
_login_lock = threading.Lock()


def browser_diagnostic(exc, stage):
    # Whitelisted classifications only: Playwright messages can include
    # credentials, cookies, login form values and token-bearing URLs.
    text = str(exc)
    network = re.search(r'net::(ERR_[A-Z_]+)', text)
    reason = network.group(1) if network else ('selector_ambiguous' if 'strict mode violation' in text
             else 'browser_closed' if 'closed' in text.lower()
             else 'browser_not_installed' if "Executable doesn't exist" in text
             else 'timeout' if 'timeout' in type(exc).__name__.lower() else 'browser_operation_failed')
    return {'stage': stage, 'error_type': type(exc).__name__, 'reason': reason}

_READ_INDICES = r"""rows => rows.map(row => {
    const cells = Array.from(row.querySelectorAll(':scope > td'));
    const text = node => node?.textContent?.trim() || null;
    return {
        name: text(row.querySelector('.indices-table__index-name .font-medium')),
        sponsor: text(row.querySelector('.indices-table__sponsor')),
        start_date: text(cells[3]),
        value: text(row.querySelector('.indices-cell-valuation__value')),
        change: text(row.querySelector('.indices-cell-valuation__performance')),
        aum: text(cells[5]),
        itd: text(row.querySelector('.indices-cell-performance__itd')),
        ytd: text(row.querySelector('.indices-cell-performance__ytd .text-small')),
        last_7_days_path: row.querySelector('.highcharts-graph')?.getAttribute('d') || null,
    };
})"""

_READ_COMPOSITION = r"""table => {
    const text = node => node?.textContent?.trim() || null;
    const headers = Array.from(table.querySelectorAll('thead th'));
    const column = suffix => headers.findIndex(h => h.classList.contains('composition-table__th--' + suffix));
    const groups = [];
    let group = null;
    for (const row of table.querySelectorAll('tbody tr.composition-table__row')) {
        const cells = Array.from(row.querySelectorAll(':scope > td'));
        const value = suffix => text(cells[column(suffix)]);
        if (!row.querySelector('.composition-table__instrument')) {
            group = {name: value('instrument'), weight: value('weight'), positions: []};
            groups.push(group);
            continue;
        }
        if (!group) { group = {name: 'Other', weight: null, positions: []}; groups.push(group); }
        group.positions.push({
            name: text(row.querySelector('.composition-table__ticker')),
            isin: text(row.querySelector('.composition-table__instrument-isin')),
            total_value: text(row.querySelector('.composition-table__total-value')),
            units: value('total-units'), price: text(row.querySelector('.composition-table__current-price')),
            currency: value('ccy'), performance: value('avg-perf'), weight: value('weight'),
        });
    }
    return groups;
}"""


def _read_composition_export(page, table, total_value: str, total_weight: str) -> list[dict] | None:
    from playwright.sync_api import Error
    from leonteq.composition_export import parse_composition_csv

    try:
        button = page.locator('button').filter(has=page.locator('.ltq-icon-name-file-csv'))
        if button.count() != 1:
            return None
        summaries = table.evaluate("""table => Array.from(table.querySelectorAll('tbody tr.composition-table__row'))
            .filter(row => !row.querySelector('.composition-table__instrument'))
            .map(row => ({name: row.cells[1].textContent.trim(), weight: row.cells[row.cells.length - 1].textContent.trim()}))""")
        expected_count = table.locator('.composition-table__instrument').count()
        with page.expect_download(timeout=10000) as receipt:
            button.click()
        download = receipt.value
        try:
            blob = Path(download.path()).read_bytes()
            return parse_composition_csv(blob, total_value=total_value, total_weight=total_weight,
                                         summaries=summaries, expected_count=expected_count)
        finally:
            download.delete()
    except (Error, ValueError, OSError, UnicodeError):
        # A missing/empty/changed export must not replace a complete composition.
        return None


def _read_composition(page, portfolio_name: str) -> dict:
    from playwright.sync_api import TimeoutError

    try:
        names = page.locator('.indices-table__index-name .font-medium')
        names.first.wait_for(state='visible', timeout=25000)
        selected = names.filter(has_text=re.compile(r'^\s*' + re.escape(portfolio_name) + r'\s*$'))
        if selected.count() != 1:
            return {'success': False, 'status': 'portfolio_not_found'}
        selected.click()
        page.get_by_text('Composition', exact=True).click()
        page.locator('.composition-table__instrument').first.wait_for(state='visible', timeout=25000)
        table = page.locator('table').filter(has=page.locator('.composition-table__th--instrument')).first
        total_value = page.locator('.amc-index-composition__field').filter(
            has=page.get_by_text('Total value', exact=True)).locator('.amc-index-composition__footer-value').inner_text()
        total_weight = page.locator('.amc-index-composition__field').filter(
            has=page.get_by_text('Total weight', exact=True)).locator('.amc-index-composition__footer-value').inner_text()
        groups = _read_composition_export(page, table, total_value, total_weight)
        source = 'csv_export' if groups else 'composition_table'
        if not groups:
            groups = table.evaluate(_READ_COMPOSITION)
        if not groups or any(not position.get('name') for group in groups for position in group['positions']):
            return {'success': False, 'status': 'composition_failed'}
        return {'success': True, 'status': 'success', 'portfolio_name': portfolio_name,
                'total_value': total_value, 'total_weight': total_weight, 'source': source,
                'groups': groups, 'retrieved_at': datetime.now(timezone.utc).isoformat()}
    except TimeoutError:
        return {'success': False, 'status': 'composition_failed'}


def _read_portfolios(page) -> dict:
    from playwright.sync_api import TimeoutError

    try:
        rows = page.locator('table tbody tr.indices-table__row')
        rows.first.wait_for(state='visible', timeout=25000)
        # Optional mini charts must not add a five-second wait to a ready portfolio table.
        portfolios = rows.evaluate_all(_READ_INDICES)
        if not portfolios or any(not row.get('name') for row in portfolios):
            return {'success': False, 'status': 'portfolios_failed'}
        return {'success': True, 'status': 'success', 'portfolios': portfolios,
                'retrieved_at': datetime.now(timezone.utc).isoformat()}
    except TimeoutError:
        return {'success': False, 'status': 'portfolios_failed'}


def _authenticated(context, page) -> bool:
    return (urlparse(page.url).hostname == 'www.lynqs.com'
            and '/eportal/' in page.url
            and any(cookie['name'] == 'lynqs-auth' and cookie.get('value')
                    for cookie in context.cookies(INDICES_URL)))


def _attempt_login(user: str, password: str, portfolio_name: str | None = None, performance: bool = False) -> dict:
    from leonteq.session import on_session_thread
    from leonteq.progress import report
    started = time.perf_counter()
    result = on_session_thread(_read_in_session, user, password, portfolio_name, performance)
    elapsed = time.perf_counter() - started
    report(f'Leonteq source read finished in {elapsed:.1f}s.', f'Leonteq-bron gelezen in {elapsed:.1f}s.')
    return {**result, 'duration_ms': round(elapsed * 1000)}


def _read_in_session(user: str, password: str, portfolio_name: str | None, performance: bool) -> dict:
    from playwright.sync_api import Error, TimeoutError
    from leonteq.progress import report
    from leonteq.session import session

    stage = 'browser_start'
    try:
        context, page, reused = session.get(user, password)
        report('Reusing the active Leonteq connection…' if reused else 'Opening Leonteq LYNQS…',
               'Actieve Leonteq-verbinding hergebruiken…' if reused else 'Leonteq LYNQS openen…')
        if reused and _authenticated(context, page):
            from leonteq.portal_api import read
            try:
                report('Retrieving fresh data directly from Leonteq…', 'Actuele gegevens rechtstreeks ophalen uit Leonteq…')
                return read(page, session.portfolio_ids, portfolio_name, performance)
            except (ValueError, KeyError, TypeError, Error):
                # Schema/auth changes keep the existing verified portal/export reader available.
                report('Direct source unavailable; reading the portal…', 'Rechtstreekse bron niet beschikbaar; portaal lezen…')
        stage = 'login_navigation'
        # Reload the source page on every explicit request. The session is reused, never its data.
        page.goto(INDICES_URL, wait_until='domcontentloaded', timeout=30000)
        if reused:
            # Fragment-only navigation can leave the old Angular component and its data intact.
            page.reload(wait_until='domcontentloaded', timeout=30000)
        if not _authenticated(context, page):
            stage = 'login_form'
            report('Signing in to Leonteq…', 'Aanmelden bij Leonteq…')
            page.locator('input#username, input[name=username]').first.fill(user)
            page.locator('input#password, input[name=password]').first.fill(password)
            page.locator('#kc-login, button[type=submit], input[type=submit]').first.click()
            deadline = time.monotonic() + 25
            stage = 'authentication'
            while time.monotonic() < deadline:
                if _authenticated(context, page):
                    break
                if page.locator('#input-error, #kc-feedback-text, .alert-error').first.is_visible():
                    session.close()
                    return {'success': False, 'status': 'login_rejected'}
                if page.locator('input#otp, input[name=otp], input[autocomplete=one-time-code]').first.is_visible():
                    session.close()
                    return {'success': False, 'status': 'verification_required'}
                page.wait_for_timeout(300)
            else:
                session.close()
                return {'success': False, 'status': 'timeout'}
        if not page.url.endswith('#/amc/indices'):
            stage = 'indices_navigation'
            page.goto(INDICES_URL, wait_until='domcontentloaded', timeout=30000)
        if not _authenticated(context, page):
            session.close()
            return {'success': False, 'status': 'session_failed'}
        if not reused:
            from leonteq.portal_api import read
            try:
                report('Retrieving fresh data directly from Leonteq…', 'Actuele gegevens rechtstreeks ophalen uit Leonteq…')
                return read(page, session.portfolio_ids, portfolio_name, performance)
            except (ValueError, KeyError, TypeError, Error):
                report('Direct source unavailable; reading the portal…', 'Rechtstreekse bron niet beschikbaar; portaal lezen…')
        if portfolio_name is None:
            report('Reading the latest portfolio list…', 'Nieuwste portefeuillelijst lezen…')
            return _read_portfolios(page)
        if performance:
            stage = 'performance_details'
            report(f'Reading monthly returns for {portfolio_name}…', f'Maandrendementen lezen voor {portfolio_name}…')
            from leonteq.monthly_performance import read_details
            return read_details(page, portfolio_name)
        report(f'Reading holdings and weights for {portfolio_name}…', f'Posities en gewichten lezen voor {portfolio_name}…')
        return _read_composition(page, portfolio_name)
    except TimeoutError as exc:
        session.close()
        return {'success': False, 'status': 'timeout', 'diagnostics': browser_diagnostic(exc, stage)}
    except Error as exc:
        session.close()
        return {'success': False, 'status': 'browser_error', 'diagnostics': browser_diagnostic(exc, stage)}


def check_login(portfolio_name: str | None = None) -> dict:
    user = os.environ.get('LEONTEQ_USER') or os.environ.get('LYNQS_USER', '')
    password = os.environ.get('LEONTEQ_PASSWORD') or os.environ.get('LYNQS_PASS', '')
    if not user or not password:
        return {'success': False, 'status': 'missing_credentials'}
    if not _login_lock.acquire(blocking=False):
        return {'success': False, 'status': 'busy'}
    try:
        if portfolio_name is not None:
            return _attempt_login(user, password, portfolio_name)
        return _attempt_login(user, password)
    except ImportError:
        return {'success': False, 'status': 'browser_unavailable'}
    finally:
        _login_lock.release()


def check_performance(name: str) -> dict:
    user = os.environ.get('LEONTEQ_USER') or os.environ.get('LYNQS_USER', '')
    password = os.environ.get('LEONTEQ_PASSWORD') or os.environ.get('LYNQS_PASS', '')
    if not user or not password:
        return {'success': False, 'status': 'missing_credentials'}
    if not _login_lock.acquire(blocking=False):
        return {'success': False, 'status': 'busy'}
    try:
        attempts = []
        for attempt in range(2):
            result = _attempt_login(user, password, name, performance=True)
            if result.get('success'):
                return {**result, 'attempts': attempt + 1}
            attempts.append({'attempt': attempt + 1, 'status': result.get('status'), 'diagnostics': result.get('diagnostics')})
            if result.get('status') not in ('browser_error', 'timeout') and not result.get('retryable'):
                break
            if attempt == 0:
                time.sleep(.5)
        return {**result, 'attempts': attempts}
    except ImportError:
        return {'success': False, 'status': 'browser_unavailable'}
    finally:
        _login_lock.release()

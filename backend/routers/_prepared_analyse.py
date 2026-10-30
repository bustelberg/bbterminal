"""Versioned, durable read models. Missing/stale snapshots always fall back to live reads."""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
import hashlib
import logging
from pathlib import Path
import time

from deps import supabase

log = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parent.parent
_FILES = tuple(sorted({
    *(_ROOT / 'routers').glob('_airs*.py'),
    *(_ROOT / 'index_universe' / 'acwi').glob('*.py'),
    _ROOT / 'routers' / '_active_share.py', _ROOT / 'routers' / '_asset_benchmark.py',
    *(_ROOT / 'routers').glob('_portfolio_*.py'),
    _ROOT / 'routers' / '_risk_read.py', _ROOT / 'routers' / '_tracking_error.py',
    _ROOT / 'routers' / '_annual_portfolio_simulation.py',
    _ROOT / 'routers' / '_risk_history_window.py', _ROOT / 'routers' / '_risk_benchmark_history.py',
    Path(__file__).resolve(),
    _ROOT / 'routers' / '_benchmark_index.py', _ROOT / 'routers' / '_weighted_benchmark.py',
    _ROOT / 'config' / 'airs_strategy_map.json',
    _ROOT / 'index_universe' / 'iShares-MSCI-ACWI-ETF_fund.xls',
    _ROOT / 'index_universe' / 'iShares-MSCI-ACWI-ETF_holdings.csv',
}))
_unavailable_until = 0.0


@lru_cache(maxsize=4)
def _digest(revisions: tuple) -> str:
    value = hashlib.sha256(b'prepared-analyse-v1')
    for name, _mtime, _size in revisions:
        value.update(name.encode())
        value.update(Path(name).read_bytes())
    return value.hexdigest()


# Freeze Python identity with this process's deployed code. An old process finishing during
# a rolling deployment must not stamp its results with newly replaced files' code version.
_PROCESS_CODE = _digest(tuple((str(path), path.stat().st_mtime_ns, path.stat().st_size)
                              for path in _FILES if path.suffix == '.py' and path.is_file()))


def code_version() -> str:
    config = _digest(tuple((str(path), path.stat().st_mtime_ns, path.stat().st_size)
                           for path in _FILES if path.suffix != '.py' and path.is_file()))
    return hashlib.sha256((_PROCESS_CODE + config).encode()).hexdigest()


def _unavailable(exc: Exception) -> None:
    global _unavailable_until
    if time.monotonic() >= _unavailable_until:
        log.warning('[prepared-analyse] unavailable; using live calculations: %s', exc)
    _unavailable_until = time.monotonic() + 60


def source_version(scope: str) -> int | None:
    if time.monotonic() < _unavailable_until:
        return None
    try:
        rows = (supabase.table('analyse_source_revision').select('version').eq('scope', scope)
                .limit(1).execute().data or [])
        return int(rows[0]['version']) if rows else None
    except Exception as exc:  # cache availability must not break the real calculation
        _unavailable(exc)
        return None


def holdings_key(portfolio_id: int, benchmark: str, look_through: bool) -> str:
    return f'holdings:{portfolio_id}:{benchmark.upper()}:{int(look_through)}'


def read(scope: str, key: str) -> tuple[int | None, dict | None]:
    version = source_version(scope)
    if version is None:
        return None, None
    try:
        code = code_version()
        rows = (supabase.table('analyse_prepared_snapshot').select('*').eq('cache_key', key)
                .eq('source_version', version).eq('scope', scope)
                .eq('calculation_day', date.today().isoformat()).eq('code_version', code)
                .limit(1).execute().data or [])
        if (rows and isinstance(rows[0].get('payload'), dict)
                and source_version(scope) == version and code_version() == code):
            return version, deepcopy(rows[0]['payload'])
    except Exception as exc:
        _unavailable(exc)
    return version, None


def save(scope: str, key: str, payload: dict, version: int, code: str) -> bool:
    # If inputs/code changed while calculating, never label mixed data as a new snapshot.
    if source_version(scope) != version or code_version() != code:
        return False
    supabase.table('analyse_prepared_snapshot').upsert({
        'cache_key': key, 'scope': scope, 'source_version': version, 'code_version': code,
        'calculation_day': date.today().isoformat(), 'prepared_at': datetime.now(timezone.utc).isoformat(),
        'payload': payload,
    }, on_conflict='cache_key').execute()
    return True


def overview(payload: dict) -> dict:
    fields = ('portfolio_id', 'name', 'as_of', 'holdings_as_of', 'holdings_fetched_at',
              'book_portefeuille', 'book_note', 'benchmark', 'weight_basis', 'weight_note',
              'allocation', 'returns', 'holdings', 'variant', 'bands', 'risk_profile_policy',
              'looked_through_pct', 'opaque_pct', 'looked_through')
    return {**{key: deepcopy(payload[key]) for key in fields if key in payload},
            'axes': [], 'book_holdings': [], 'realised': None, 'details_pending': True}


def prepare(*, portfolio_ids: list[int] | None = None, benchmark: bool = True,
            holdings: bool = True, should_stop=None, on_step=None,
            max_portfolios: int = 2, max_seconds: float = 20, risk: bool = False) -> dict:
    from common.read_cache import read_cache
    from routers import _airs_portfolio_analysis as analysis
    from routers._active_share import _load_benchmark_weights
    from routers._airs_account_links import list_account_links

    result = {'holdings': 0, 'benchmarks': 0, 'already_current': 0, 'changed_during_run': 0, 'errors': {}}
    if risk:
        result['risk_views'] = 0
    deadline = time.monotonic() + max_seconds
    selected_portfolios: set[int] = set()
    if source_version('holdings') is None:
        return {**result, 'unavailable': True}
    if portfolio_ids is None and holdings:
        portfolio_ids = sorted({int(row['model_portfolio_id']) for row in list_account_links()['accounts']
                                if row.get('model_portfolio_id') is not None})
    work = ([('acwi', 'acwi:ACWI', None, False)] if benchmark else [])
    if holdings:
        work += [('holdings', holdings_key(pid, 'ACWI', look), pid, look)
                 for pid in (portfolio_ids or []) for look in (False, True)]
    for index, (scope, key, pid, look) in enumerate(work):
        if should_stop and should_stop():
            result['cancelled'] = True
            break
        if time.monotonic() >= deadline:
            result['budget_exhausted'] = True
            break
        if on_step:
            on_step(index, len(work), f'Preparing {key}')
        try:
            version = source_version(scope)
            if version is None:
                result['unavailable'] = True
                break
            code = code_version()
            current = (supabase.table('analyse_prepared_snapshot').select('cache_key').eq('cache_key', key)
                       .eq('source_version', version).eq('code_version', code)
                       .eq('calculation_day', date.today().isoformat()).limit(1).execute().data or [])
            if current:
                result['already_current'] += 1
                if risk and pid is not None:
                    _, risk_ready = read(scope, f'risk-views:{key}')
                    if risk_ready is not None:
                        continue
                    if pid not in selected_portfolios and len(selected_portfolios) >= max_portfolios:
                        result['batch_exhausted'] = True
                        break
                    selected_portfolios.add(pid)
                    from routers._risk_read import prepare_views
                    _, saved = read(scope, key)
                    if saved is not None:
                        result['risk_views'] += prepare_views(saved, look)
                        if not save(scope, f'risk-views:{key}', {'prepared': True}, version, code):
                            result['changed_during_run'] += 1
                continue
            if pid is not None:
                if pid not in selected_portfolios and len(selected_portfolios) >= max_portfolios:
                    result['batch_exhausted'] = True
                    break
                selected_portfolios.add(pid)
            if pid is None:
                with read_cache(key):
                    payload = _load_benchmark_weights('ACWI', None)
            else:
                token = analysis._INCLUDE_HOLDING_RISK.set(False)
                overview_token = analysis._OVERVIEW_ONLY.set(False)
                try:
                    with read_cache(key):
                        payload = analysis.compute_portfolio_analysis(pid, 'ACWI', 'book', 'book', None, look, False)
                    payload = {**payload, 'details_pending': False}
                finally:
                    analysis._INCLUDE_HOLDING_RISK.reset(token)
                    analysis._OVERVIEW_ONLY.reset(overview_token)
            if save(scope, key, payload, version, code):
                result['benchmarks' if pid is None else 'holdings'] += 1
                if risk and pid is not None:
                    from routers._risk_read import prepare_views
                    result['risk_views'] += prepare_views(payload, look)
                    if not save(scope, f'risk-views:{key}', {'prepared': True}, version, code):
                        result['changed_during_run'] += 1
            else:
                result['changed_during_run'] += 1
        except Exception as exc:
            result['errors'][key] = f'{type(exc).__name__}: {exc}'
            log.exception('[prepared-analyse] could not prepare %s', key)
    if on_step:
        on_step(len(work), len(work), 'Prepared Analyse views complete')
    return result


def run_controlled(*, busy, should_stop=None, on_step=None,
                   max_portfolios: int = 2, max_seconds: float = 20) -> dict:
    """One bounded serial batch, with a cross-process lock and source-refresh priority."""
    from common.pg import _db_url
    import psycopg

    if busy():
        return {'skipped': 'source refresh is active'}
    url = _db_url()
    if not url:
        return {'unavailable': True, 'reason': 'Direct database connection is required for the preparation lock'}
    with psycopg.connect(url, connect_timeout=3, autocommit=True, options='-c statement_timeout=5000') as conn:
        locked = conn.execute("SELECT pg_try_advisory_lock(hashtext('bbterminal.analyse_prepare'))").fetchone()[0]
        if not locked:
            return {'skipped': 'another preparation worker holds the lock'}
        try:
            return prepare(max_portfolios=max_portfolios, max_seconds=max_seconds, risk=True,
                           should_stop=lambda: bool((should_stop and should_stop()) or busy()), on_step=on_step)
        finally:
            conn.execute("SELECT pg_advisory_unlock(hashtext('bbterminal.analyse_prepare'))")

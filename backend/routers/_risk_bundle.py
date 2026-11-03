"""One prepared, atomic Risk read. Expensive work runs only in preparation workers."""
from datetime import date
import hashlib
import json
import time


class _SourceChanged(Exception):
    pass


def _settle(ctx):
    """Wait briefly for the shared source writer to settle, with cancellation checks."""
    from jobs import UserFacingJobError
    started = stable = time.monotonic()
    from routers._risk_read import source_revision
    revision = source_revision()
    if ctx:
        ctx.progress(0, 6, 'Waiting for the latest data…')
    while time.monotonic() - started < 30:
        if ctx:
            ctx.check()
        time.sleep(.25)
        current = source_revision()
        if current != revision or current[1] is None:
            revision, stable = current, time.monotonic()
        elif time.monotonic() - stable >= .5:
            return
    raise UserFacingJobError('Risk data is still updating. Please try again shortly.')


def identity(holdings, policy):
    from routers._risk_read import _canonical
    rows = sorted((_canonical(row) for row in holdings), key=lambda row: json.dumps(row, sort_keys=True))
    value = json.dumps({'holdings': rows, 'policy': _canonical(policy), 'start': '2021-01-01'}, sort_keys=True)
    return 'risk-bundle:v1:' + hashlib.sha256(value.encode()).hexdigest()


def read(holdings, policy):
    from routers import _prepared_analyse as prepared
    from routers._risk_read import source_revision
    scope, _ = source_revision()
    return prepared.read(scope, identity(holdings, policy))[1]


def _prepare_once(holdings, policy, ctx=None):
    from routers import _prepared_analyse as prepared
    from routers._risk_read import read as calculate, source_revision
    from routers._active_share import compute_active_share
    from routers._portfolio_concentration import compute_concentration
    from routers._annual_portfolio_simulation import compute_simulation
    from routers._portfolio_correlation_risk import compute_risk_correlation
    from routers._portfolio_volatility import compute_volatility
    from routers._portfolio_drawdown import compute_drawdown
    from routers._tracking_error import MIN_PRELIMINARY_DAILY_OBS
    scope, version = source_revision()
    code = prepared.code_version()
    if version is None:
        raise RuntimeError('Risk storage is temporarily unavailable. Please try again shortly.')
    history = {'start_date': '2021-01-01', 'years': max(1, date.today().year - 2021 + 1)}
    weight = sum(row['weight_pct'] for row in holdings)
    normalised = [{**row, 'weight_pct': row['weight_pct'] / weight * 100 if weight > 0 else 0} for row in holdings]
    tasks = [
        ('active-share', compute_active_share, (holdings, 'ACWI', None, {'Stocks': 100, 'Bonds': 0, 'EquityOnly': 1}), {'held_only': True}),
        ('concentration', compute_concentration, (holdings, 'ACWI', None, policy), {}),
        ('tracking-error', compute_simulation, (holdings, 'ACWI'), history),
        ('risk-correlation', compute_risk_correlation, (normalised, 'ACWI', 'weekly'),
         {**history, 'benchmark_weights': {'Stocks': 100, 'Bonds': 0}, 'require_complete_history': True}),
        ('volatility', compute_volatility, (holdings, 'ACWI', 'monthly'),
         {**history, 'benchmark_weights': policy, 'require_complete_history': True}),
        ('drawdown', compute_drawdown, (holdings, 'ACWI', 'daily'),
         {**history, 'benchmark_weights': policy, 'require_complete_history': True}),
    ]
    views = {}
    for index, (name, fn, args, kwargs) in enumerate(tasks):
        if ctx:
            ctx.check()
            ctx.progress(index, len(tasks), 'Loading risk data…')
        answer = calculate(fn, *args, **kwargs)
        if name == 'volatility' and isinstance(answer, dict) and not answer.get('available'):
            daily = calculate(fn, holdings, 'ACWI', 'daily', **kwargs,
                              minimum_observations=MIN_PRELIMINARY_DAILY_OBS, preliminary=True)
            if daily.get('available'):
                answer = daily
        views[name] = answer
        if prepared.source_version(scope) != version or prepared.code_version() != code:
            raise _SourceChanged()
    if ctx:
        ctx.check()
    if all(isinstance(answer, dict) for answer in views.values()):
        if not prepared.save(scope, identity(holdings, policy), {'views': views}, version, code):
            raise _SourceChanged()
    return views


def prepare(holdings, policy, ctx=None):
    """Restart interrupted generations automatically; never publish mixed results."""
    from jobs import UserFacingJobError
    for attempt in range(3):
        try:
            return _prepare_once(holdings, policy, ctx)
        except _SourceChanged:
            if attempt == 2:
                break
            _settle(ctx)
    raise UserFacingJobError('Risk data is still updating. Please try again shortly.')

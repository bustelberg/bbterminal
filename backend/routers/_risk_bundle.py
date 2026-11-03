"""One prepared, atomic Risk read. Expensive work runs only in preparation workers."""
from datetime import date
import hashlib
import json


def identity(holdings, policy):
    from routers._risk_read import _canonical
    rows = sorted((_canonical(row) for row in holdings), key=lambda row: json.dumps(row, sort_keys=True))
    value = json.dumps({'holdings': rows, 'policy': _canonical(policy), 'start': '2021-01-01'}, sort_keys=True)
    return 'risk-bundle:v1:' + hashlib.sha256(value.encode()).hexdigest()


def read(holdings, policy):
    from routers import _prepared_analyse as prepared
    return prepared.read('holdings', identity(holdings, policy))[1]


def prepare(holdings, policy, ctx=None):
    from routers import _prepared_analyse as prepared
    from routers._risk_read import read as calculate
    from routers._active_share import compute_active_share
    from routers._portfolio_concentration import compute_concentration
    from routers._annual_portfolio_simulation import compute_simulation
    from routers._portfolio_correlation_risk import compute_risk_correlation
    from routers._portfolio_volatility import compute_volatility
    from routers._portfolio_drawdown import compute_drawdown
    from routers._tracking_error import MIN_PRELIMINARY_DAILY_OBS
    version, code = prepared.source_version('holdings'), prepared.code_version()
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
            ctx.progress(index, len(tasks), 'Preparing portfolio risk…')
        answer = calculate(fn, *args, **kwargs)
        if name == 'volatility' and isinstance(answer, dict) and not answer.get('available'):
            daily = calculate(fn, holdings, 'ACWI', 'daily', **kwargs,
                              minimum_observations=MIN_PRELIMINARY_DAILY_OBS, preliminary=True)
            if daily.get('available'):
                answer = daily
        views[name] = answer
    if ctx:
        ctx.check()
    if all(isinstance(answer, dict) for answer in views.values()):
        if not prepared.save('holdings', identity(holdings, policy), {'views': views}, version, code):
            raise RuntimeError('Portfolio data changed while preparing risk. Please try again.')
    return views

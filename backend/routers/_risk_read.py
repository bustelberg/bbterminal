"""Versioned risk results shared by workers; interactive reads never repair vendors."""
from contextvars import ContextVar
from copy import deepcopy
from functools import wraps
from datetime import date
import hashlib
import inspect
import json
import logging
from threading import RLock
from index_universe.templates._cache import _LruTtlCache

ALLOW_REPAIR = ContextVar('risk_allow_repair', default=True)
_locks = [RLock() for _ in range(32)]
_series = _LruTtlCache(max_size=8, ttl_seconds=3600)
log = logging.getLogger(__name__)


def _canonical(value):
    if isinstance(value, float):
        rounded = round(value, 12)
        return int(rounded) if rounded.is_integer() else rounded
    if isinstance(value, dict):
        # These equity-risk calculations consume identity and weights, not display euros or FX.
        if 'weight_pct' in value:
            value = {**{key: value.get(key) for key in ('isin', 'name', 'weight_pct', 'bucket')},
                     'is_fund': bool(value.get('is_fund', False))}
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def read(compute, *args, **kwargs):
    from routers import _prepared_analyse as prepared
    from routers._analysis_cache import leg

    bound = inspect.signature(compute).bind(*args, **kwargs)
    bound.apply_defaults()
    identity = json.dumps(_canonical(bound.arguments), sort_keys=True, separators=(',', ':'))
    key = 'risk:' + compute.__name__ + ':' + hashlib.sha256(identity.encode()).hexdigest()

    def load():
        version, saved = prepared.read('holdings', key)
        if saved is not None:
            return saved
        code = prepared.code_version()
        token = ALLOW_REPAIR.set(False)
        try:
            answer = compute(*args, **kwargs)
        finally:
            ALLOW_REPAIR.reset(token)
        if version is not None:
            try:
                prepared.save('holdings', key, answer, version, code)
            except Exception:
                log.exception('Could not save prepared risk result %s', key)
        return answer

    # Collapse concurrent requests for the same result, including cold cache misses.
    with _locks[int(hashlib.sha256(key.encode()).hexdigest(), 16) % len(_locks)]:
        return deepcopy(leg(('risk-result-v1', key, date.today().isoformat(), prepared.code_version()), load))


def shared_series(compute):
    """Reuse expensive aligned histories across volatility, correlation and drawdown."""
    @wraps(compute)
    def wrapped(*args, **kwargs):
        if ALLOW_REPAIR.get():
            return compute(*args, **kwargs)
        bound = inspect.signature(compute).bind(*args, **kwargs)
        bound.apply_defaults()
        if bound.arguments.get('preloaded') is not None:
            return compute(*args, **kwargs)
        from routers._analysis_cache import fingerprint
        key = json.dumps(_canonical(bound.arguments), sort_keys=True, separators=(',', ':'))
        with _locks[0]:
            version = fingerprint()
            if version is None:
                return compute(*args, **kwargs)
            full_key = (version, date.today().isoformat(), key)
            hit = _series.get(full_key)
            if hit is not None:
                return hit
            answer = compute(*args, **kwargs)
            _series.put(full_key, answer)
            return answer
    return wrapped


def prepare_views(payload: dict, look_through: bool) -> int:
    """Prepare the exact default equity-risk requests from a saved Analyse book."""
    import re
    from routers.airs import ActiveShareHolding
    from routers._active_share import compute_active_share
    from routers._portfolio_concentration import compute_concentration
    from routers._annual_portfolio_simulation import compute_simulation
    from routers._portfolio_correlation_risk import compute_risk_correlation
    from routers._portfolio_volatility import compute_volatility
    from routers._portfolio_drawdown import compute_drawdown
    rows = payload.get('book_holdings') or []
    total = sum(row.get('current_value_eur') or 0 for row in rows)
    holdings = []
    for row in rows:
        value = row.get('current_value_eur') or 0
        fallback_weight = row.get('weight_now_pct') or 0
        if not look_through:
            routes = row.get('sources') or []
            via = [route for route in routes if route.get('label')]
            direct = [route for route in routes if not route.get('label')]
            if len(routes) >= 2 and via and sum(route.get('start_value_eur') or 0 for route in routes) > 0:
                if not direct:
                    continue
                value = sum(route.get('value_eur') or 0 for route in direct)
                fallback_weight = sum(route.get('weight_now_pct') or 0 for route in direct)
            elif (len(routes) == 1 and routes[0].get('label')) or (not routes and len(row.get('via_names') or []) == 1):
                continue
        name = row.get('name') or ''
        if row.get('is_fund') or row.get('bucket') not in (None, '', 'Equity') or re.search(
                r'\b(etf|etp|etn|ucits|fund|fonds|certificate|certificaten|index|perp|bond|obligat|coupon|treasur|gilt|bund|govie|sovereign|floating\s*rate|frn|high\s*yield|fixed\s*income)\b|\d[.,]?\d*\s*%', name, re.I):
            continue
        holdings.append(ActiveShareHolding(isin=row.get('isin'), name=row.get('name'),
            weight_pct=value / total * 100 if total > 0 else fallback_weight,
            is_fund=False, bucket=row.get('bucket'), value_eur=value, currency=row.get('currency')).model_dump())
    policy = {**payload['risk_profile_policy'], 'EquityOnly': 1} if payload.get('risk_profile_policy') else None
    years = max(1, date.today().year - 2021 + 1)
    history = {'start_date': '2021-01-01', 'years': years}
    read(compute_active_share, holdings, 'ACWI', None, {'Stocks': 100, 'Bonds': 0, 'EquityOnly': 1}, held_only=True)
    read(compute_concentration, holdings, 'ACWI', None, policy)
    read(compute_simulation, holdings, 'ACWI', **history)
    stock_weight = sum(row['weight_pct'] for row in holdings)
    normalised = [{**row, 'weight_pct': row['weight_pct'] / stock_weight * 100 if stock_weight > 0 else 0} for row in holdings]
    read(compute_risk_correlation, normalised, 'ACWI', 'weekly', benchmark_weights={'Stocks': 100, 'Bonds': 0},
         require_complete_history=True, **history)
    read(compute_volatility, holdings, 'ACWI', 'monthly', benchmark_weights=policy,
         require_complete_history=True, **history)
    read(compute_drawdown, holdings, 'ACWI', 'daily', benchmark_weights=policy,
         require_complete_history=True, **history)
    return 6

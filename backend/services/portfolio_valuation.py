"""Daily, saved historical-multiple estimates. No provider work on HTTP reads.

This is a screening estimate, not intrinsic value or a point-in-time backtest.
The vendor supplies restatable fiscal-period EPS, not original publication dates.
Historical EPS is therefore delayed 120 days, explicitly disclosed in the payload.
Only exact company ISINs are price identities; notes/issuer aliases are not used.
"""
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from statistics import median
from threading import Lock, RLock

from deps import paginate, supabase

TABLE = 'portfolio_valuation_bundle'
_locks = {}
_locks_guard = Lock()
MODEL_VERSION = 'historical-pe-v2'
EPS_CODES = tuple(f'quarterly__{section}__EPS without NRI' for section in
                  ('Per Share Data', 'per_share_data', 'per_share_data_array'))
_EPS_PREFERENCE = tuple(reversed(EPS_CODES))  # Current array schema before legacy spellings.
METHOD = {
    'version': MODEL_VERSION, 'lookback_years': 5, 'minimum_observations': 1000,
    'minimum_span_days': 1460, 'reporting_lag_days': 120,
    'description': 'Median daily positive P/E over five years × latest reported trailing-year EPS without NRI. '
                   'Requires 1,000 observations spanning at least four years. Fiscal earnings are delayed 120 days '
                   'for historical comparisons because original publication dates are unavailable. '
                   'Vendor history may be restated. This is historical relative value, not intrinsic fair value.',
}


def number(value):
    try:
        result = float(value)
        return result if isfinite(result) else None
    except (TypeError, ValueError):
        return None


def select_eps_series(rows, today):
    """Select one vendor schema for the whole history, never splice restated bases.

    Prefer the series with the newest actual fiscal period. Ties use the current
    vendor array spelling, then older spellings. Ingestion timestamps cannot
    establish freshness because unchanged observations are intentionally not rewritten.
    """
    by_code = {}
    for row in rows:
        when = str(row.get('target_date') or '')[:10]
        if row.get('is_prediction') or not when or when > today or number(row.get('numeric_value')) is None:
            continue
        by_code.setdefault(row.get('metric_code') or '', []).append(row)
    if not by_code:
        return [], None
    def rank(code):
        priority = len(_EPS_PREFERENCE) - _EPS_PREFERENCE.index(code) if code in _EPS_PREFERENCE else 0
        return max(str(row['target_date'])[:10] for row in by_code[code]), priority, code
    selected = max(by_code, key=rank)
    return by_code[selected], selected or None


def ttm_earnings(rows, today):
    """Four consecutive actual quarters; never fill a missing quarter or use estimates."""
    selected, code = select_eps_series(rows, today)
    quarters = {}
    for row in selected:
        when = str(row.get('target_date') or '')[:10]
        value = number(row.get('numeric_value'))
        if row.get('is_prediction') or value is None or not when or when > today:
            continue
        # A conflict inside the chosen series is genuine; aliases were excluded above.
        previous = quarters.get(when)
        if previous is not None and previous != value:
            raise ValueError(f'Conflicting EPS values within {code or "the selected series"} for {when}: '
                             f'{previous:g} versus {value:g}; review that source period.')
        quarters[when] = value
    dates = sorted(quarters)
    result = []
    for i in range(3, len(dates)):
        window = dates[i - 3:i + 1]
        gaps = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(window, window[1:])]
        if all(60 <= gap <= 120 for gap in gaps):
            result.append((window[-1], sum(quarters[day] for day in window)))
    return result


def estimate(prices, earnings, financial_currency, today=None):
    today = today or date.today()
    result = {'status': 'unavailable', 'reason': None, 'price': None, 'price_date': None,
              'currency': None, 'eps_ttm': None, 'eps_date': None, 'current_pe': None,
              'median_pe': None, 'estimated_value': None, 'discount_pct': None,
              'observations': 0, 'history_from': None, 'history_to': None}

    def unavailable(reason):
        return {**result, 'reason': reason}

    # Currency is metadata for this exact listing; unknown/mixed units cannot be guessed.
    from asset_pipeline.fx import SUBUNIT
    by_date, currencies = {}, set()
    for row in prices:
        when = str(row.get('target_date') or '')[:10]
        value = number(row.get('close'))
        currency = row.get('currency')
        if not when or when > today.isoformat() or value is None or value <= 0:
            continue
        currency, scale = SUBUNIT.get(currency, (currency, 1))
        currencies.add(currency)
        value /= scale
        if when in by_date and by_date[when] != value:
            return unavailable(f'Conflicting prices for {when}; review the exact ISIN listing.')
        by_date[when] = value
    if not by_date:
        return unavailable('No saved Yahoo closing prices for this exact company ISIN.')
    latest = max(by_date)
    result.update(price=by_date[latest], price_date=latest, currency=next(iter(currencies)) if len(currencies) == 1 else None)
    if len(currencies) != 1 or None in currencies or not financial_currency:
        return unavailable('Quote currency or earnings currency is missing or inconsistent.')
    if result['currency'] != financial_currency:
        return unavailable(f"Quote currency {result['currency']} differs from earnings currency {financial_currency}; no FX or ADR ratio assumed.")
    if (today - date.fromisoformat(latest)).days > 7:
        return unavailable(f'Latest saved close is {latest}, more than seven days old; refresh prices.')
    _, result['eps_metric_code'] = select_eps_series(earnings, today.isoformat())
    try:
        ttm = ttm_earnings(earnings, today.isoformat())
    except ValueError as exc:
        return unavailable(str(exc))
    if not ttm:
        return unavailable('Fewer than four consecutive reported EPS quarters; quarterly EPS without NRI is required.')
    # Do not use earnings newer than the displayed valuation close.
    ttm = [(when, value) for when, value in ttm if when <= latest]
    if not ttm:
        return unavailable('No trailing-year EPS on or before the latest close.')
    fiscal_date, current_eps = ttm[-1]
    result.update(eps_ttm=current_eps, eps_date=fiscal_date)
    if (today - date.fromisoformat(fiscal_date)).days > 220:
        return unavailable(f'Latest trailing-year EPS ends {fiscal_date}; refresh reported fundamentals.')
    if current_eps <= 0:
        return unavailable('Latest trailing-year EPS is zero or negative; P/E valuation is not meaningful.')
    result['current_pe'] = result['price'] / current_eps
    available = [(date.fromisoformat(when) + timedelta(days=120)).isoformat() for when, _ in ttm]
    start = (today - timedelta(days=round(365.25 * 5))).isoformat()
    samples = []
    for when, price in sorted(by_date.items()):
        if when < start:
            continue
        index = bisect_right(available, when) - 1
        if index < 0:
            continue
        period, eps = ttm[index]
        if eps > 0 and (date.fromisoformat(when) - date.fromisoformat(period)).days <= 220:
            samples.append((when, price / eps))
    result.update(observations=len(samples), history_from=samples[0][0] if samples else None,
                  history_to=samples[-1][0] if samples else None)
    span = (date.fromisoformat(samples[-1][0]) - date.fromisoformat(samples[0][0])).days if samples else 0
    if len(samples) < 1000 or span < 1460:
        return unavailable(f'Insufficient positive-earnings history: {len(samples)} daily observations over {span} days; requires 1,000 observations spanning four years within the five-year window.')
    pe = median(value for _, value in samples)
    value = current_eps * pe
    return {**result, 'status': 'ok', 'median_pe': pe, 'estimated_value': value,
            'discount_pct': (1 - result['price'] / value) * 100}


def portfolios():
    from routers._airs_overview import list_overview
    from routers._airs_ref import models
    # Dashboard accounts own the display names. The linked Fixed model is only
    # the valuation data key, never a second roster/nickname source.
    model_rows = {row['id']: row for row in models()}
    groups = ('bustelberg', 'toppenberg', 'topselecties', 'imagine_fund', 'test_topselecties')
    result = []
    for row in list_overview():
        if row.get('management_group') not in groups:
            continue
        model_id = row.get('fixed_portfolio_id')
        result.append({'id': model_id, 'name': row['name'], 'account': row['dynamic_portefeuille'],
                       'management_group': row['management_group'],
                       'composition_date': model_rows.get(model_id, {}).get('positions_datum'),
                       'unavailable_reason': None if model_id is not None else
                       'No linked AIRS Fixed model; connect this account on the Management Dashboard first.'})
    return result


def company_holdings(portfolio_id):
    from routers._airs_ref import model, positions_for
    from routers._airs_lookthrough import expand_positions
    from routers._airs_portfolio_analysis import _grid, _is_fund, _class_from_categorie
    from routers._airs_holding_isin import classify_bucket
    portfolio = model(portfolio_id)
    if not portfolio:
        raise ValueError(f'Unknown model portfolio {portfolio_id}')
    positions = positions_for(portfolio_id)
    if portfolio.get('positions_datum'):
        positions = [row for row in positions if row.get('datum') == portfolio['positions_datum']]
    if not positions:
        raise ValueError(f'No saved composition for portfolio {portfolio_id}; refresh AIRS Fixed positions first.')
    expanded, evidence = expand_positions(portfolio_id, portfolio.get('positions_datum'), positions)
    grid = _grid(sorted({row['isin'] for row in expanded if row.get('isin')}))
    companies = {}
    excluded = 0
    for i, row in enumerate(expanded):
        isin, name = row.get('isin'), row.get('fonds')
        asset = grid.get(isin) or {}
        bucket = classify_bucket(_class_from_categorie(row.get('categorie')), _is_fund(asset, name or ''), isin, name, asset)
        if bucket in ('Cash', 'Bonds') or _is_fund(asset, name or ''):
            excluded += 1
            continue
        # Unknown classifications remain visible with their data limitation.
        key = isin or f'missing:{i}'
        entry = companies.setdefault(key, {'isin': isin, 'name': name or isin or 'Unnamed holding', 'weight_pct': 0})
        entry['weight_pct'] += number(row.get('percentage')) or 0
    return list(companies.values()), {'composition_date': portfolio.get('positions_datum'),
                                     'excluded_funds_cash_bonds': excluded, 'lookthrough': evidence}


def load_inputs(isin, today):
    # Exact ISIN only. An issuer/ADR match sufficient for notes or overlap is not sufficient for EPS.
    companies = (supabase.table('company').select('company_id,isin,gurufocus_exchange:gurufocus_exchange(currency_code)')
                 .eq('isin', isin).is_('out_of_scope_at', 'null').execute().data or [])
    if len(companies) != 1:
        return None, [], [], f'Expected one active fundamentals company for ISIN {isin}; found {len(companies)}. Review its company mapping.'
    company = companies[0]
    cid = company['company_id']
    start = (today - timedelta(days=round(365.25 * 7))).isoformat()
    prices = list(paginate(lambda lo, hi: supabase.table('company_yahoo_price')
                          .select('target_date,close,currency').eq('company_id', cid)
                          .gte('target_date', start).lte('target_date', today.isoformat())
                          .order('target_date').order('analysis_id').range(lo, hi).execute()))
    earnings = list(paginate(lambda lo, hi: supabase.table('metric_data')
                            .select('target_date,numeric_value,is_prediction,metric_code').eq('company_id', cid)
                            .eq('source_code', 'gurufocus').eq('is_prediction', False).in_('metric_code', EPS_CODES)
                            .gte('target_date', start).lte('target_date', today.isoformat())
                            .order('target_date').order('metric_code').range(lo, hi).execute()))
    return company, prices, earnings, None


def _publication_lock(portfolio_id):
    with _locks_guard:
        return _locks.setdefault(portfolio_id, RLock())


def publish(portfolio_id, *, today=None, check_cancel=lambda: None, input_cache=None):
    with _publication_lock(portfolio_id):
        check_cancel()
        return _publish(portfolio_id, today=today, check_cancel=check_cancel, input_cache=input_cache)


def _publish(portfolio_id, *, today=None, check_cancel=lambda: None, input_cache=None):
    today = today or date.today()
    holdings, evidence = company_holdings(portfolio_id)
    rows, errors = [], {}
    calculations = input_cache if input_cache is not None else {}
    for holding in holdings:
        check_cancel()
        try:
            isin = holding.get('isin')
            if not isin:
                calculated = {'status': 'unavailable', 'reason': 'Holding has no ISIN; connect it to an exact company security.'}
            else:
                if isin not in calculations:
                    company, prices, earnings, reason = load_inputs(isin, today)
                    calculations[isin] = ({'status': 'unavailable', 'reason': reason} if reason else
                                         estimate(prices, earnings, (company.get('gurufocus_exchange') or {}).get('currency_code'), today))
                # Cache only the small result across portfolios, not years of raw bars.
                calculated = calculations[isin]
        except Exception as exc:
            errors[holding.get('isin') or holding['name']] = f'{type(exc).__name__}: {exc}'
            calculated = {'status': 'error', 'reason': errors[holding.get('isin') or holding['name']]}
        rows.append({**holding, **calculated})
    check_cancel()
    payload = {'portfolio_id': portfolio_id, 'calculation_date': today.isoformat(), 'method': METHOD,
               'rows': sorted(rows, key=lambda row: row['name'].casefold()), 'evidence': evidence,
               'errors': errors}
    stamp = datetime.now(timezone.utc).isoformat()
    # A source outage must not overwrite the last valid publication with empty/error values.
    if errors:
        raise RuntimeError(f'Valuation {portfolio_id} not published; previous results retained. Instrument evidence: {errors}')
    supabase.table(TABLE).upsert({'portfolio_id': portfolio_id, 'prepared_at': stamp,
                                 'payload': payload}, on_conflict='portfolio_id').execute()
    return {**payload, 'prepared_at': stamp, 'ready': True}


def refresh(portfolio_id, *, force=False):
    """Prepare one selected portfolio from existing DB inputs, never a provider refresh."""
    from fastapi import HTTPException
    with _publication_lock(portfolio_id):
        existing = read(portfolio_id)  # Validates the dashboard scope and storage migration.
        if existing.get('ready') and not existing.get('stale') and not force:
            return existing
        try:
            return publish(portfolio_id)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).exception('Valuation preparation failed for portfolio %s; saved results retained', portfolio_id)
            raise HTTPException(503, f'Could not prepare valuation for portfolio {portfolio_id}; previous results remain saved. '
                                f'{type(exc).__name__}. Detailed source evidence is in the server log.') from exc


def read(portfolio_id):
    from fastapi import HTTPException
    selected = next((row for row in portfolios() if row['id'] == portfolio_id), None)
    if selected is None:
        raise HTTPException(404, 'Dashboard portfolio not found')
    try:
        rows = (supabase.table(TABLE).select('payload,prepared_at').eq('portfolio_id', portfolio_id)
                .limit(1).execute().data or [])
    except Exception as exc:
        raise HTTPException(503, 'Saved valuations could not be read. Apply migration '
                            '20261113190000_portfolio_valuation.sql and run portfolio_valuation_daily. '
                            f'Database error: {type(exc).__name__}') from exc
    if not rows or rows[0]['payload'].get('method', {}).get('version') != MODEL_VERSION:
        # The stock list is useful before the first daily valuation publication.
        # This reads the saved composition only; no prices/fundamentals are fetched or calculated.
        try:
            holdings, evidence = company_holdings(portfolio_id)
        except ValueError as exc:
            return {'portfolio_id': portfolio_id, 'ready': False, 'rows': [], 'method': METHOD, 'detail': str(exc)}
        return {'portfolio_id': portfolio_id, 'ready': False,
                'rows': [{**holding, 'status': 'pending', 'reason': 'Valuation has not been prepared yet.'}
                         for holding in holdings], 'method': METHOD, 'evidence': evidence}
    row = rows[0]
    return {**row['payload'], 'ready': True, 'prepared_at': row['prepared_at'],
            'stale': (row['payload'].get('calculation_date') != date.today().isoformat()
                      or row['payload'].get('evidence', {}).get('composition_date') != selected.get('composition_date'))}

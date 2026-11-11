"""Read the same authenticated AMC endpoints as LYNQS, without navigating its SPA."""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote
import calendar

BASE = 'https://www.lynqs.com/eportal/amc-gateway/api/v2'


class ApiUnavailable(ValueError):
    pass


def _number(value):
    while isinstance(value, dict):
        value = value.get('value', value.get('data'))
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ApiUnavailable('Missing numeric source field') from None
    if not result.is_finite():
        raise ApiUnavailable('Non-finite source field')
    return result


def _text(value, precision=2):
    while isinstance(value, dict):
        precision = value.get('precision', precision)
        value = value.get('value', value.get('data'))
    if not isinstance(precision, int) or not 0 <= precision <= 8:
        raise ApiUnavailable('Unknown source precision')
    return format(_number(value), f',.{precision}f').replace(',', '’')


def _percent(value):
    return None if value is None or isinstance(value, dict) and value.get('value') is None else _text(value) + '%'


def _optional_text(value):
    if value is None or isinstance(value, dict) and value.get('value', value.get('data')) is None:
        return None
    return _text(value)


def _sparkline(points):
    values = [float(_number(point['value'])) for point in points]
    if not values:
        return None
    low, high = min(values), max(values)
    return ' '.join(f'{"M" if i == 0 else "L"} {i * 120 / max(1, len(values) - 1):.2f} '
                    f'{(36 - (value - low) * 32 / (high - low)) if high > low else 20:.2f}'
                    for i, value in enumerate(values))


def catalogue(payload):
    if not isinstance(payload, dict):
        raise ApiUnavailable('Unknown catalogue response')
    rows = payload.get('rows')
    if not isinstance(rows, list) or not rows or payload.get('totalRows') != len(rows):
        raise ApiUnavailable('Incomplete portfolio catalogue')
    result = []
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get('name'), str)
                or not row['name'].strip() or not isinstance(row.get('ruleBookId'), str)
                or not row['ruleBookId']):
            raise ApiUnavailable('Missing portfolio identity')
        absolute = row.get('performance1DAbsolute')
        change = _percent(row.get('performance1D'))
        result.append({
            'name': row['name'].strip(), 'sponsor': (row.get('indexSponsor') or '').strip() or None,
            'start_date': date.fromisoformat(row['inceptionDate'][:10]).strftime('%d %b %Y'),
            'value': _text(row['currentValuation']), 'aum': _text(row['assetsUnderManagement'], 0),
            'change': f'{_text(absolute)} ({change})' if absolute is not None and change is not None else None,
            'itd': _percent(row.get('performanceInception')), 'ytd': _percent(row.get('performanceYTD')),
            'last_7_days_path': _sparkline(row.get('recentValuations') or []),
        })
    if len({row['name'] for row in result}) != len(result):
        raise ApiUnavailable('Ambiguous portfolio names')
    return result


def composition(payload, name):
    if not isinstance(payload, dict):
        raise ApiUnavailable('Unknown composition response')
    rows, totals = payload.get('rows'), payload.get('totals')
    if not isinstance(rows, list) or not rows or not isinstance(totals, dict):
        raise ApiUnavailable('Incomplete composition')
    currencies = {row.get('amcCurrency') for row in rows}
    if len(currencies) != 1 or not next(iter(currencies)):
        raise ApiUnavailable('Unknown base currency')
    currency = next(iter(currencies))
    groups, total_value, total_weight = {}, Decimal(0), Decimal(0)
    for row in rows:
        category = row.get('instrumentCategory')
        if not row.get('name') or not category or not row.get('currency'):
            raise ApiUnavailable('Incomplete holding')
        group = groups.setdefault(category, {'name': category, 'positions': [], '_weight': Decimal(0)})
        value, weight = _number(row['totalValueAmcCcy']), _number(row['weightPercentAmcCcy'])
        total_value += value
        total_weight += weight
        group['_weight'] += weight
        group['positions'].append({
            'name': row['name'], 'isin': row.get('instrumentIsin') or None,
            'total_value': f'{_text(row["totalValueAmcCcy"])} {currency}',
            'units': _text(row['totalUnits']), 'price': _text(row['currentPrice']),
            'price_date': (row['currentPrice'].get('date') if isinstance(row['currentPrice'].get('date'), str)
                           else payload.get('closingPriceDate')),
            'currency': row['currency'], 'performance': _percent(row.get('averagePerformance')) or '-',
            'weight': _text(row['weightPercentAmcCcy']) + '%',
            'identifier': row.get('instrumentMappingId'),
            'entry_price': _optional_text(row.get('averageEntryPrice')), 'fx': _optional_text(row.get('fx')),
        })
    tolerance = Decimal('.01') * len(rows)
    if (abs(total_value - _number(totals['totalValueAmcCcy'])) > tolerance
            or abs(total_weight - _number(totals['weightPercentAmcCcy'])) > tolerance):
        raise ApiUnavailable('Composition totals do not reconcile')
    for group in groups.values():
        group['weight'] = _text(group.pop('_weight')) + '%'
    return {'portfolio_name': name, 'groups': list(groups.values()),
            'total_value': f'{_text(totals["totalValueAmcCcy"])} {currency}',
            'total_weight': _text(totals['weightPercentAmcCcy']) + '%'}


def monthly(payload, name):
    if not isinstance(payload, list) or not payload:
        raise ApiUnavailable('Missing monthly returns')
    months, totals, years = [], {}, set()
    for row in payload:
        year = row['year']
        if not isinstance(year, int) or not 1900 <= year <= 2200 or year in years:
            raise ApiUnavailable('Invalid return year')
        years.add(year)
        for month in range(1, 13):
            value = row.get(calendar.month_name[month].lower())
            number = _percent(value)
            months.append({'month': f'{year}-{month:02}',
                           'return_pct': float(number[:-1].replace('’', '')) if number is not None else None})
        if (total := _percent(row.get('total'))) is not None:
            totals[str(year)] = float(total[:-1].replace('’', ''))
    return {'portfolio_name': name, 'months': months, 'yearly_totals': totals}


def read(page, ids, name=None, performance=False, transactions=False):
    def get(path):
        response = page.request.get(f'{BASE}/{path}', headers={'Cache-Control': 'no-cache'}, timeout=10000)
        if not response.ok:
            raise ApiUnavailable('The authenticated source API is unavailable')
        return response.json()

    if name is None or name not in ids:
        payload = get('amc?sort=%2Bname')
        portfolios = catalogue(payload)
        ids.clear()
        ids.update({row['name'].strip(): row['ruleBookId'] for row in payload['rows']})
        if name is None:
            return {'success': True, 'status': 'success', 'source': 'lynqs_api',
                    'retrieved_at': datetime.now(timezone.utc).isoformat(), 'portfolios': portfolios}
    if name not in ids:
        raise ApiUnavailable('Portfolio not found in the current catalogue')
    path = f'amc/{quote(ids[name], safe="")}'
    if transactions:
        from leonteq.recent_transactions import parse_transactions
        from leonteq.progress import report
        rows, total, seen = [], None, set()
        while True:
            report(f'Reading booked transactions · {len(rows)}/{total if total is not None else "…"}')
            page_data = get(path + f'/index-transactions?offset={len(rows)}&limit=100&sort=-transactionTime')
            if (not isinstance(page_data, dict) or not isinstance(page_data.get('rows'), list)
                    or type(page_data.get('totalRows')) is not int or not 0 <= page_data['totalRows'] <= 20000):
                raise ApiUnavailable('Unknown or oversized transaction feed')
            if total is not None and page_data['totalRows'] != total:
                raise ApiUnavailable('Transactions changed during pagination. Fetch again.')
            total = page_data['totalRows']
            batch = page_data['rows']
            identities = {(row.get('reportId'), row.get('transactionNumber')) for row in batch
                          if isinstance(row, dict) and row.get('reportId') is not None and row.get('transactionNumber') is not None}
            if seen & identities:
                raise ApiUnavailable('Overlapping transaction pages. Fetch again.')
            seen.update(identities)
            rows.extend(batch)
            if len(rows) >= total:
                break
            if not batch:
                raise ApiUnavailable('Incomplete transaction pagination')
        return {'success': True, 'transactions': parse_transactions({'rows': rows, 'totalRows': total}),
                'retrieved_at': datetime.now(timezone.utc).isoformat()}
    result = monthly(get(path + '/monthly-performance'), name) if performance else composition(get(path + '/composition?sort=%2Bname'), name)
    return {'success': True, 'status': 'success', 'source': 'lynqs_api',
            'retrieved_at': datetime.now(timezone.utc).isoformat(), **result}

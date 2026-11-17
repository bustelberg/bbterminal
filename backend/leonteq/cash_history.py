"""Certificate cash movements and fees exposed by the LYNQS reporting client."""
from urllib.parse import quote
from decimal import Decimal

from leonteq.portal_api import ApiUnavailable, _number


def _pages(get, path, sort):
    rows, expected, seen = [], None, set()
    while expected is None or len(rows) < expected:
        payload = get(f'{path}?offset={len(rows)}&limit=100&sort={sort}')
        batch, total = payload.get('rows'), payload.get('totalRows')
        if (not isinstance(batch, list) or type(total) is not int or not 0 <= total <= 20000
                or expected is not None and total != expected):
            raise ApiUnavailable('Incomplete or changing Leonteq certificate history')
        expected = total
        for row in batch:
            stamp = row.get('date')
            if not stamp or stamp in seen:
                raise ApiUnavailable('Overlapping Leonteq certificate history dates')
            seen.add(stamp)
        rows.extend(batch)
        if len(rows) > total or not batch and len(rows) < total:
            raise ApiUnavailable('Incomplete Leonteq certificate history pagination')
    return rows


def collect(get, index_path, previous=None):
    certificates = get(index_path + '/certificates')
    if not isinstance(certificates, list) or not certificates:
        raise ApiUnavailable('No linked Leonteq certificate cash history')
    evidence = []
    previous_certificates = {item['isin']: item for item in (previous or {}).get('certificates') or []}
    for certificate in certificates:
        isin = certificate.get('isin')
        if not isin or certificate.get('currency') != 'EUR':
            raise ApiUnavailable('Certificate cash history needs a verified EUR identity')
        path = 'amc/certificate/' + quote(isin, safe='')
        summaries = _pages(get, path + '/secondary-market-transactions', '-date')
        cached = previous_certificates.get(isin) or {}
        old_summaries = {row['date']: row for row in cached.get('cash_summaries') or []}
        old_end = max(old_summaries, default='')
        details = []
        for summary in summaries:
            if summary.get('currency') != 'EUR':
                raise ApiUnavailable('Certificate cash movements are not in EUR')
            stamp = summary['date']
            if stamp < old_end and old_summaries.get(stamp) == summary:
                records = [row for row in cached.get('cash_movements') or [] if row['dateTime'][:10] == stamp]
            else:
                records = get(path + '/secondary-market-transaction-details?date=' + quote(stamp, safe=''))
            if not isinstance(records, list):
                raise ApiUnavailable('Incomplete Leonteq certificate cash details')
            if any(str(row.get('dateTime') or '')[:10] != stamp for row in records):
                raise ApiUnavailable('Leonteq certificate cash dates do not match')
            net = sum((_number(row.get('netFlows')) for row in records), start=0)
            if abs(net - _number(summary.get('totalNetFlows'))) > Decimal('.02'):
                raise ApiUnavailable('Certificate cash details do not reconcile to their daily total')
            details.extend(records)
        evidence.append({'isin': isin, 'certificate': certificate, 'cash_summaries': summaries,
            'cash_movements': details,
            'valuation_fees': _pages(get, path + '/valuation-fee-history', '-date')})
    return {'complete': True, 'source': 'Leonteq certificate reporting', 'certificates': evidence}

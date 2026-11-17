"""Recover a source-checked cash inception from booked initial instructions.

Orders are execution evidence only when BOOKED. They supplement missing trades,
never dividends, and never duplicate transactions already in the source ledger.
"""
from collections import defaultdict
from decimal import Decimal as D

from leonteq.portal_api import ApiUnavailable, _number


def initial_records(records, orders, composition, start, end, *, prefer_transactions=False):
    identities = {r.get('instrumentRic'): r.get('instrumentIsin') for r in composition['rows']
                  if r.get('instrumentRic') and r.get('instrumentIsin')}
    combined = list(records)
    consumed = set()
    for order in orders:
        if order.get('status') != 'BOOKED' or order.get('orderDate') != end:
            continue
        side = order.get('side')
        if side not in ('BUY', 'SELL'):
            raise ApiUnavailable('Unsupported initial Leonteq instruction side')
        isin = order.get('instrumentIsin') or identities.get(order.get('instrumentRic'))
        quantity = _number(order.get('filledQuantity'))
        net, fx = _number(order.get('netAmount')), _number(order.get('fxRate'))
        if (not isin or quantity <= 0 or quantity != _number(order.get('quantity'))
                or net <= 0 or fx <= 0 or order.get('conversionCurrency') != 'EUR'
                or order.get('amcCurrency') != 'EUR' or not order.get('id')):
            raise ApiUnavailable('Incomplete booked initial Leonteq execution')
        event = side.title()
        cash = net * fx * (-1 if side == 'BUY' else 1)
        matches = [(i, r) for i, r in enumerate(records) if i not in consumed
                   and r['date'] == end and r['isin'] == isin and r['event'] == event
                   and r['quantity'] is not None and abs(D(r['quantity'])) == quantity]
        if matches:
            exact = [(i, r) for i, r in matches if r['cash_eur'] is not None and abs(D(r['cash_eur']) - cash) <= D('.02')]
            if len(exact) != 1:
                if prefer_transactions and len(matches) == 1:
                    consumed.add(matches[0][0])
                    continue
                raise ApiUnavailable('Initial Leonteq instruction and transaction amounts conflict')
            consumed.add(exact[0][0])
            continue
        combined.append({'id': 'instruction:' + order['id'], 'date': end, 'event': event,
            'name': order['instrumentName'], 'isin': isin, 'booked': True,
            'quantity': str(quantity), 'cash_eur': str(cash), 'net_amount': str(net),
            'net_price': str(_number(order.get('netPrice'))), 'booking_fx': str(fx),
            'currency': order.get('underlyingCurrency'), 'booking_currency': 'EUR',
            'source': 'leonteq_booked_instruction'})
    if any(r['booked'] and r['date'] <= start for r in combined):
        raise ApiUnavailable('Leonteq initial ledger has activity before its opening boundary')
    return combined


def reconstruct(first, records, start, points, *, opening_cash=None):
    """Reverse all initial fills and cash, then independently verify index return.

    This is limited to an all-cash launch: zero residual securities, every initial
    holding explained, no first-day transfers/dividends/sales, and an index of 100.
    An unexplained funding flow or price mismatch fails the index reconciliation.
    """
    if points[0][0] != start or points[0][1] != 100 or points[1][0] != first['date']:
        raise ApiUnavailable('Leonteq opening is not a verified index inception')
    trades = [r for r in records if r['booked'] and start < r['date'] <= first['date']]
    if not trades or any(r['event'] not in ('Buy', 'AMC Fees') or r['cash_eur'] is None for r in trades):
        raise ApiUnavailable('Leonteq opening requires complete initial buys and net cash')
    quantities = defaultdict(lambda: D(0))
    for r in trades:
        if r['event'] == 'Buy':
            if not r['isin'] or r['quantity'] is None:
                raise ApiUnavailable('Leonteq initial fill has no security quantity')
            quantities[r['isin']] += abs(D(r['quantity']))
    for identity in set(quantities) | (set(first['positions']) - {'cash:EUR'}):
        if abs(first['positions'].get(identity, {}).get('quantity', D(0)) - quantities[identity]) > D('.000001'):
            raise ApiUnavailable('Leonteq initial executions do not explain every opening holding')
    cash = first['positions'].get('cash:EUR', {}).get('value', D(0)) - sum(D(r['cash_eur']) for r in trades)
    if opening_cash is not None:
        cash = D(str(opening_cash))
    if cash <= 0:
        raise ApiUnavailable('Leonteq reconstructed opening cash is invalid')
    tolerance = D('.01') * (len(first['positions']) + len(trades))
    if abs(first['nav'] - cash * points[1][1] / points[0][1]) > tolerance:
        raise ApiUnavailable('Leonteq reconstructed opening cash does not reconcile to its first return')
    return {'date': start, 'nav': cash, 'positions': {'cash:EUR': {
        'name': 'EUR', 'isin': None, 'cash': True, 'value': cash, 'quantity': cash}}}

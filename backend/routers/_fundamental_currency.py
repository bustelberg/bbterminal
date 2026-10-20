"""One quote-currency boundary shared by every portfolio Fundamental model."""
import math

from routers._benchmark_index import _rate


def valuation_price(value, quote_currency, financial_currency, when, fx):
    if value is None or not math.isfinite(float(value)) or float(value) <= 0:
        return None
    if not quote_currency or not financial_currency:
        return None
    if quote_currency == financial_currency:
        return float(value)
    source = _rate(fx, quote_currency, when)
    target = _rate(fx, financial_currency, when)
    if not source or not target or not math.isfinite(source) or not math.isfinite(target) or source <= 0 or target <= 0:
        return None
    return float(value) / source * target

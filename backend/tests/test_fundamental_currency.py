import pytest

from routers._fundamental_currency import valuation_price


FX = {currency: {'2026-10-16': rate} for currency, rate in
      {'USD': 1.1, 'CAD': 1.5, 'GBP': 0.8, 'AUD': 1.6, 'HKD': 8.5, 'MXN': 20,
       'SGD': 1.4, 'ZAR': 19, 'ILS': 4}.items()}


@pytest.mark.parametrize('quote,target,value,expected', [
    ('USD', 'EUR', 110, 100), ('EUR', 'USD', 100, 110),
    ('CAD', 'USD', 150, 110), ('USD', 'CAD', 110, 150),
    ('USD', 'AUD', 110, 160), ('AUD', 'USD', 160, 110),
    ('EUR', 'HKD', 100, 850), ('MXN', 'HKD', 2000, 850),
    ('USD', 'SGD', 110, 140), ('GBp', 'GBP', 8000, 80),
    ('GBp', 'EUR', 8000, 100), ('ZAc', 'EUR', 190000, 100),
    ('ILA', 'EUR', 40000, 100), ('USD', 'USD', 110, 110),
])
def test_all_cross_listing_and_minor_unit_currency_paths(quote, target, value, expected):
    assert valuation_price(value, quote, target, '2026-10-19', FX) == pytest.approx(expected)


@pytest.mark.parametrize('quote,target,value,fx', [
    (None, 'EUR', 100, FX), ('USD', None, 100, FX), ('USD', 'EUR', 100, {}),
    ('USD', 'EUR', float('nan'), FX), ('USD', 'EUR', 0, FX),
    ('USD', 'EUR', 100, {'USD': {'2026-10-20': 1.1}}),
])
def test_unknown_or_invalid_inputs_never_become_a_mixed_currency_price(quote, target, value, fx):
    assert valuation_price(value, quote, target, '2026-10-19', fx) is None

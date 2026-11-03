import pytest

from tests._fake_supabase import FakeSupabase
from routers import _airs_price_check as prices


@pytest.mark.parametrize('latest', ['2026-10-30', '2026-11-02'])
def test_refreshes_sold_company_prices_and_euro_conversion(monkeypatch, latest):
    from asset_pipeline import store
    from routers import _airs_portfolio_refresh as refresh
    fake = FakeSupabase({
        'asset_post_close_watch': [{'isin': 'SOLD', 'last_sale_date': '2026-02-04'}],
        'asset_execution': [{'isin': 'SOLD', 'analysis_id': 42, 'yahoo_symbol': 'COMPANY', 'currency': 'USD'}],
        'asset_price': [{'analysis_id': 42, 'target_date': '2026-10-30', 'close': 100}],
    })
    monkeypatch.setattr(prices, 'supabase', fake)
    calls = []
    def extend(aid, symbol, since):
        calls.append((aid, symbol, since))
        fake.tables['asset_price'].append({'analysis_id': aid, 'target_date': latest, 'close': 101})
        return 1
    monkeypatch.setattr(store, 'extend_series', extend)
    fx = []
    monkeypatch.setattr(refresh, '_fx', lambda currencies, anchor, emit: fx.append((currencies, anchor)) or {'failed': 0})

    summary = prices.refresh_sold_holding_price('SOLD')

    assert calls == [(42, 'COMPANY', '2026-02-04')]
    assert fx == [({'USD'}, '2026-02-04')]
    assert latest in summary
    assert ('No newer closing price' in summary) == (latest == '2026-10-30')


def test_unknown_sold_company_cannot_start_price_work(monkeypatch):
    monkeypatch.setattr(prices, 'supabase', FakeSupabase({'asset_post_close_watch': []}))
    with pytest.raises(ValueError, match='no saved sale history'):
        prices.refresh_sold_holding_price('UNKNOWN')

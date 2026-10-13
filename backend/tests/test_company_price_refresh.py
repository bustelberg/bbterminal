"""A manual company-price refresh fetches the live tail, not its whole history."""
from __future__ import annotations

from asset_pipeline import company_prices


class _Reply:
    def __init__(self, data=None, count=None):
        self.data = data or []
        self.count = count


class _Query:
    def __init__(self, reply):
        self.reply = reply

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return self.reply


class _Supabase:
    def __init__(self):
        self.company = _Reply([{"isin": "FR0000120321", "company_name": "L'Oreal"}])
        self.execution = _Reply([{"analysis_id": 42, "yahoo_symbol": "OR.PA", "exchange": "PAR"}])
        self.prices = _Reply(count=12345)

    def table(self, name):
        return _Query({"company": self.company, "asset_execution": self.execution,
                       "asset_price": self.prices}[name])


def test_forced_refresh_of_mapped_company_extends_current_tail(monkeypatch):
    """Force bypasses freshness, but must not turn an existing series into a history reload."""
    from asset_pipeline import price_refresh, store

    monkeypatch.setattr(company_prices, "supabase", _Supabase())
    monkeypatch.setattr(price_refresh, "latest_close_by_analysis", lambda _ids: {42: "2026-10-10"})
    calls = []
    monkeypatch.setattr(store, "extend_series", lambda *args: calls.append(args) or 2)
    monkeypatch.setattr(store, "store_series", lambda *_args: (_ for _ in ()).throw(
        AssertionError("a mapped manual refresh must not reload full history")))

    result = company_prices.refresh_company_prices(7, force_refresh=True)

    assert calls == [(42, "OR.PA", "2026-10-10")]
    assert result.rows_loaded == 2


def test_unpriced_company_still_gets_its_first_full_history(monkeypatch):
    """The optimisation never leaves a newly resolved company with only a short tail."""
    from asset_pipeline import price_refresh, store

    monkeypatch.setattr(company_prices, "supabase", _Supabase())
    monkeypatch.setattr(price_refresh, "latest_close_by_analysis", lambda _ids: {})
    monkeypatch.setattr(store, "extend_series", lambda *_args: (_ for _ in ()).throw(
        AssertionError("there is no existing series to extend")))
    monkeypatch.setattr(store, "store_series", lambda *args: 12345)

    result = company_prices.refresh_company_prices(7, force_refresh=True)

    assert result.rows_loaded == 12345

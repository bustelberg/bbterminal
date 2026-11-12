from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase


def test_benchmark_response_preserves_dated_fx_for_eur_price_display():
    from routers.airs import WeightedBenchmarkComponent

    payload = WeightedBenchmarkComponent.model_validate({
        'name': 'Equity', 'weight_pct': 100, 'currency': 'USD',
        'start_price': 100, 'end_price': 120,
        'fx_start': 1.25, 'fx_end': 1.2, 'return_pct': 25,
    }).model_dump(mode='json')

    opening_eur = payload['start_price'] / payload['fx_start']
    closing_eur = payload['end_price'] / payload['fx_end']
    assert opening_eur == pytest.approx(80)
    assert closing_eur == pytest.approx(100)
    assert (closing_eur / opening_eur - 1) * 100 == pytest.approx(payload['return_pct'])


def test_headline_etf_return_stops_at_airs_date_even_when_newer_prices_exist(monkeypatch):
    from routers import _benchmark_etf as etf, _benchmark_index as index

    monkeypatch.setattr(etf, 'ensure_fresh', lambda _: (7, ('2026-11-09', 150)))
    monkeypatch.setattr(etf, '_proxy_fetched_at', lambda _: None)
    monkeypatch.setattr(etf, 'supabase', FakeSupabase({'benchmark_price': [
        {'benchmark_id': 7, 'target_date': '2025-12-31', 'price': 100},
        {'benchmark_id': 7, 'target_date': '2026-11-06', 'price': 110},
        {'benchmark_id': 7, 'target_date': '2026-11-09', 'price': 150},
    ]}))
    monkeypatch.setattr(index, '_fx_to_eur', lambda *_: {})
    monkeypatch.setattr(index, '_rate', lambda *_: 1)
    result = etf.etf_returns('ACWI', ['2026-01-01'], end='2026-11-06')['2026-01-01']
    assert result['as_of'] == '2026-11-06'
    assert result['eur_pct'] == pytest.approx(10)


@pytest.mark.parametrize('read_only', [False, True])
def test_proxy_curve_uses_the_portfolios_exact_dates_and_prior_market_closes(monkeypatch, read_only):
    from routers import _benchmark_etf as etf
    from routers import _benchmark_index as index

    def ensure(_label):
        assert not read_only, 'opening bundle must not fetch vendor history'
        return (7, ("2026-02-02", 120.0))
    monkeypatch.setattr(etf, "ensure_fresh", ensure)
    monkeypatch.setattr(etf, '_latest', lambda _bid: ("2026-02-02", 120.0))
    monkeypatch.setattr(etf, "_at_or_before", lambda _bid, _date: ("2025-12-31", 100.0))
    monkeypatch.setattr(etf, "supabase", FakeSupabase(tables={
        'benchmark': [{'benchmark_id': 7, 'ticker': etf.PROXY['SP500']}], "benchmark_price": [
        {"benchmark_id": 7, "target_date": "2025-12-31", "price": 100.0},
        {"benchmark_id": 7, "target_date": "2026-01-30", "price": 110.0},
        {"benchmark_id": 7, "target_date": "2026-02-02", "price": 120.0},
    ]}))
    monkeypatch.setattr(index, "_fx_to_eur", lambda *_args: {})
    monkeypatch.setattr(index, "_rate", lambda *_args: 1.0)

    got = etf.etf_return_series(
        "SP500", "2026-01-01", ["2026-01-01", "2026-01-31", "2026-02-02"], read_only=read_only)

    assert [p["date"] for p in got["points"]] == [
        "2026-01-01", "2026-01-31", "2026-02-02"]
    # 31 January is a weekend: the point stays on the portfolio date but uses Friday's close.
    assert got["points"][1]["price_date"] == "2026-01-30"
    assert [p["cum_pct"] for p in got["points"]] == pytest.approx([0.0, 10.0, 20.0])
    assert got["return_pct"] == pytest.approx(20.0)


def test_scheduled_proxy_refresh_fetches_a_gap_once_per_day(monkeypatch):
    """A restart after the 06:00 job must not re-fetch (or rewrite) ETF history."""
    from routers import _benchmark_etf as etf

    calls: list[dict] = []
    previous = dict(etf._fetched_today)
    etf._fetched_today.clear()
    monkeypatch.setattr(etf, "_benchmark_id", lambda _ticker: 7)
    monkeypatch.setattr(etf, "_latest", lambda _bid: ("2026-10-12", 100.0))
    monkeypatch.setattr(etf, "_behind_the_market", lambda _last: False)
    monkeypatch.setattr(etf, "_proxy_fetched_at", lambda _bid: None)
    monkeypatch.setattr(etf, "_stamp_fetched", lambda _bid: None)
    monkeypatch.setattr(
        etf, "_refresh",
        lambda _ticker, _bid, _last, *, full_history: calls.append(
            {"last": _last, "full_history": full_history}) or 0,
    )
    try:
        etf.ensure_fresh("ACWI", force=True)
        etf.ensure_fresh("ACWI", force=True)  # simulated startup after the scheduled refresh
        etf.ensure_fresh("ACWI", force=True, bypass_daily_guard=True)
    finally:
        etf._fetched_today.clear()
        etf._fetched_today.update(previous)

    assert calls == [
        {"last": "2026-10-12", "full_history": False},
        {"last": "2026-10-12", "full_history": False},
    ]


def test_persisted_proxy_stamp_prevents_a_duplicate_after_restart(monkeypatch):
    from datetime import datetime, timezone
    from routers import _benchmark_etf as etf

    previous = dict(etf._fetched_today)
    etf._fetched_today.clear()  # a fresh process has no in-memory guard
    monkeypatch.setattr(etf, "_benchmark_id", lambda _ticker: 7)
    monkeypatch.setattr(etf, "_latest", lambda _bid: ("2026-10-12", 100.0))
    monkeypatch.setattr(etf, "_behind_the_market", lambda _last: False)
    monkeypatch.setattr(etf, "_proxy_fetched_at",
                        lambda _bid: datetime.now(timezone.utc).isoformat())
    monkeypatch.setattr(etf, "_refresh", lambda *_a, **_kw: pytest.fail("duplicate refresh"))
    try:
        etf.ensure_fresh("ACWI", force=True)
    finally:
        etf._fetched_today.clear()
        etf._fetched_today.update(previous)


def test_proxy_history_repair_is_an_explicit_full_rebuild(monkeypatch):
    from routers import _benchmark_etf as etf

    calls: list[dict] = []
    monkeypatch.setattr(etf, "refresh_index_proxies", lambda **kw: calls.append(kw) or 4)

    assert etf.repair_index_proxy_history() == 4
    assert calls == [{"force": True, "bypass_daily_guard": True, "full_history": True}]

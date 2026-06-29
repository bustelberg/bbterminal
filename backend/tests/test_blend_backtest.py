"""Unit tests for momentum.blend_backtest — the pure builder that blends a
saved momentum backtest with a fixed-weight ETF overlay into a
BacktestResult-shaped blob (the seed a 'diversified variant' is scheduled
from). No DB: the caller hands in the source result blob + ETF price series.
"""
from __future__ import annotations

from momentum import blend_backtest as bb


def _source_result() -> dict:
    """A tiny 3-period monthly source backtest, each period one stock at 100%."""
    def period(month, entry_d, exit_d, ret_pct, cum_pct, holdings_ret):
        return {
            "date": month,
            "holdings": [{
                "company_id": 42, "ticker": "AAA", "company_name": "Aaa Inc",
                "sector": "Tech", "score": 80.0, "category_scores": {"price": 80.0},
                "weight": 1.0, "forward_return_pct": holdings_ret,
                "entry_price_local": 100.0, "exit_price_local": 100.0 * (1 + holdings_ret / 100),
                "entry_price_eur": 100.0, "exit_price_eur": 100.0 * (1 + holdings_ret / 100),
                "entry_date": entry_d, "exit_date": exit_d, "side": "long",
            }],
            "portfolio_return_pct": ret_pct,
            "cumulative_return_pct": cum_pct,
        }
    return {
        "monthly_records": [
            period("2020-01", "2020-01-01", "2020-02-01", 2.0, 2.0, 2.0),
            period("2020-02", "2020-02-01", "2020-03-01", -1.0, 0.98, -1.0),
            period("2020-03", "2020-03-01", "2020-04-01", 3.0, 4.0094, 3.0),
        ],
        "daily_records": [
            {"date": "2020-01-01", "cumulative_return_pct": 0.0},
            {"date": "2020-02-01", "cumulative_return_pct": 2.0},
            {"date": "2020-03-01", "cumulative_return_pct": 0.98},
            {"date": "2020-04-01", "cumulative_return_pct": 4.0094},
        ],
        "summary": {"sharpe_ratio": 1.0},
        "universe": [{"company_id": 42, "ticker": "AAA"}],
    }


def _gld_prices() -> list[tuple[str, float]]:
    # Monthly closes spanning the whole source window.
    return [
        ("2019-12-31", 100.0),
        ("2020-01-31", 110.0),   # +10% in Jan
        ("2020-02-28", 104.5),   # -5% in Feb
        ("2020-03-31", 104.5),   # flat in Mar
    ]


def test_blended_appends_etf_holdings_with_negative_company_id():
    src = _source_result()
    gld = bb.OverlayEtf(benchmark_id=7, ticker="GLD", name="Gold", sector="Commodity",
                        weight=0.2, band=0.1, prices=_gld_prices())
    out = bb.build_blended_result(src, [gld], strategy_weight=0.8)

    assert out["monthly_records"], "expected blended periods"
    first = out["monthly_records"][0]
    # One scaled stock + one ETF holding.
    stock = [h for h in first["holdings"] if h["company_id"] > 0]
    etf = [h for h in first["holdings"] if h["company_id"] < 0]
    assert len(stock) == 1 and len(etf) == 1
    # Stock weight scaled to the strategy sleeve.
    assert abs(stock[0]["weight"] - 0.8) < 1e-9
    # ETF holding uses the negative-benchmark-id convention + its target weight.
    assert etf[0]["company_id"] == -7
    assert abs(etf[0]["weight"] - 0.2) < 1e-9
    assert etf[0]["ticker"] == "GLD"


def test_blended_period_return_is_weighted_blend():
    src = _source_result()
    gld = bb.OverlayEtf(benchmark_id=7, ticker="GLD", name="Gold", sector="Commodity",
                        weight=0.2, band=0.1, prices=_gld_prices())
    out = bb.build_blended_result(src, [gld], strategy_weight=0.8)
    # Jan: strat +2% @0.8, GLD +10% @0.2 → 0.8*2 + 0.2*10 = 3.6%
    jan = out["monthly_records"][0]
    assert abs(jan["portfolio_return_pct"] - 3.6) < 1e-6


def test_summary_has_risk_metrics_and_daily_curve():
    src = _source_result()
    gld = bb.OverlayEtf(benchmark_id=7, ticker="GLD", name="Gold", sector="Commodity",
                        weight=0.2, band=0.1, prices=_gld_prices())
    out = bb.build_blended_result(src, [gld], strategy_weight=0.8)
    s = out["summary"]
    for k in ("sharpe_ratio", "sortino_ratio", "annualized_return_pct", "max_drawdown_pct",
              "total_return_pct", "total_months"):
        assert k in s
    assert out["daily_records"], "expected a blended daily curve"
    assert all("date" in d and "cumulative_return_pct" in d for d in out["daily_records"])


def test_window_limited_by_late_etf_history():
    """An ETF whose history starts after the first source period drops the
    pre-history periods (mirrors the diversifier's common-window limiting)."""
    src = _source_result()
    late = bb.OverlayEtf(benchmark_id=9, ticker="LATE", name="Late ETF", sector=None,
                         weight=0.2, band=0.1,
                         prices=[("2020-02-15", 50.0), ("2020-02-28", 51.0), ("2020-03-31", 52.0)])
    out = bb.build_blended_result(src, [late], strategy_weight=0.8)
    months = [r["date"] for r in out["monthly_records"]]
    # Jan period (entry 2020-01-01) predates LATE's first price → dropped.
    assert "2020-01" not in months
    assert "2020-03" in months


def test_make_etf_holding_and_rebalance_assembly():
    h = bb.make_etf_holding(7, "GLD", "Gold", "Commodity", 0.2, 100.0, 110.0,
                            "2020-01-01", "2020-02-01")
    assert h["company_id"] == -7
    assert abs(h["forward_return_pct"] - 10.0) < 1e-9

    stocks = [{"company_id": 1, "ticker": "X", "weight": 0.5},
              {"company_id": 2, "ticker": "Y", "weight": 0.5}]
    etf = bb.OverlayEtf(benchmark_id=7, ticker="GLD", name="Gold", sector=None,
                        weight=0.2, band=0.1, prices=[])
    merged = bb.assemble_blended_rebalance_holdings(
        stocks, [etf], {7: 105.0}, "2020-05-01", strategy_weight=0.8,
    )
    assert len(merged) == 3
    assert abs(merged[0]["weight"] - 0.4) < 1e-9  # 0.5 * 0.8
    etf_row = [h for h in merged if h["company_id"] == -7][0]
    assert etf_row["entry_price_local"] == 105.0
    assert etf_row["forward_return_pct"] is None  # freshly entered, no exit yet

"""A BOOK converts its MONEY weights into SHARES with ONE price, and that price does not move.

⚠⚠ A PORTFOLIO WEIGHT IS A MONEY WEIGHT AND A FUNDAMENTAL IS A PER-SHARE FACT, so exactly one
conversion bridges them — the market cap. Owning `w_i` of a book worth `B`:

    n_i = w_i·B / price_i = w_i·B · shares_i / cap_i(T)
    claim = n_i · fcf_per_share_i(t) = w_i·B · F_i(t) / cap_i(T)

`B` cancels in every ratio, so the contribution is `w_i · F_i(t) / cap_i(T)`.

⚠⚠ `T`, NOT `t`. The share count is fixed when you buy, so the cap that converts it is fixed too.
Divided by each period's own cap the sum stops being a portfolio and becomes a fresh purchase at
every year's valuation: its growth is then `growth(FCF) − growth(price)`, i.e. a YIELD series. A
company that doubles its cash flow while its cap doubles reads **0%** against the benchmark's
**+100%**, in the same chart — and that series is, up to SBC and a constant, the FCF-SBC yield card
four rows below it, so the tab drew one quantity twice and called one of them growth.

⚠⚠ AND BEFORE THAT IT DID NOT EVEN FAIL AS AN ABSENCE. The per-period lookup used the FILING DATE
(`2015-12-31`) against caps keyed by PERIOD (`2015`), so every filed period was dropped — while the
LTM branch, which fell back to the newest cap, survived alone. One period is enough to put
`blend_series` on the aggregate path with no step it can span: a single invisible point beside a
benchmark that drew perfectly. Reported 2026-08-26 as "FCF per share shows empty data and only the
benchmark" on Bustelberg Offensief. One basis per member removes the whole class — there is no
per-period lookup left to miss.

Unit-only: `deps.supabase` is a `FakeSupabase` and the FX/LTM helpers are stubbed.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase

FCF_PS = "annuals__Per Share Data__Free Cash Flow per Share"
SHARES = "annuals__Income Statement__Shares Outstanding (Diluted Average)"

#: ⚠ THE SHAPE THAT TELLS THE TWO CONSTRUCTIONS APART, and the one a real book has: cash flow
#: DOUBLES (1.0 → 2.0 per share on a flat share count) and so does the market cap. On the euro sum
#: that is +100%; on a per-period cap it is 0%, because the yield never moved.
_ROWS = [
    {"company_id": 1, "metric_code": FCF_PS, "target_date": "2024-12-31", "numeric_value": 1.0},
    {"company_id": 1, "metric_code": FCF_PS, "target_date": "2025-12-31", "numeric_value": 2.0},
    {"company_id": 1, "metric_code": SHARES, "target_date": "2024-12-31", "numeric_value": 100.0},
    {"company_id": 1, "metric_code": SHARES, "target_date": "2025-12-31", "numeric_value": 100.0},
]
_CAPS_DOUBLING = {1: {"2024": 1_000.0, "2025": 2_000.0}}
_CAPS_QUARTERLY = {1: {f"{y}-Q{q}": 1_000.0 for y in (2024, 2025) for q in (1, 2, 3, 4)}}


@pytest.fixture
def earnings(monkeypatch):
    """`routers.earnings` reading a fake `metric_data`, with FX and LTM neutralised."""
    from routers import earnings as e

    fake = FakeSupabase({
        "metric_data": _ROWS,
        "company": [{"company_id": 1, "company_name": "Acme", "isin": "US0000000001",
                     "market_cap_eur": 1_000.0,
                     "gurufocus_exchange": {"currency_code": "EUR"}}],
        "asset_grid": [{"isin": "US0000000001", "sector": "Technology"}],
    })
    monkeypatch.setattr(e, "supabase", fake)
    # ⚠ FX OUT OF THE WAY: 1.0 EUR per EUR, so every figure below is exact.
    import routers._benchmark_index as bi

    monkeypatch.setattr(bi, "_fx_to_eur", lambda *_a, **_k: {})
    monkeypatch.setattr(bi, "_rate", lambda *_a, **_k: 1.0)
    monkeypatch.setattr(e, "_ltm_by_company", lambda *_a, **_k: {})
    return e


def _series(out: dict, code: str = FCF_PS) -> dict[str, float]:
    return (out.get(code) or {}).get(1) or {}


def _growth(s: dict[str, float]) -> float:
    return s["2025-12-31"] / s["2024-12-31"] - 1


class TestTheCapIsOneDate:
    def test_a_one_company_book_grows_exactly_like_that_company_in_an_index(self, earnings):
        """⚠⚠ THE INVARIANT. With one member, `w·F/cap(T)` and `F` differ by a CONSTANT, so the two
        constructions must agree to the last bit. They did not before: the book read 0% where the
        index read +100%. Verified on eight live companies at 8.9e-16."""
        index = _series(earnings.fundamental_totals([1], ["fcf_ps"]))
        book = _series(earnings.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps=_CAPS_DOUBLING))
        assert _growth(index) == pytest.approx(1.0)          # cash flow doubled
        assert _growth(book) == pytest.approx(_growth(index))

    def test_the_period_s_own_cap_would_have_read_zero(self, earnings):
        """The defect, stated as the number it produced — a rerating cancelling the growth."""
        book = _series(earnings.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps=_CAPS_DOUBLING))
        per_period = {p: 5.0 * v / _CAPS_DOUBLING[1][p[:4]]
                      for p, v in _series(earnings.fundamental_totals([1], ["fcf_ps"])).items()}
        assert _growth(per_period) == pytest.approx(0.0)     # what the tab drew
        assert _growth(book) == pytest.approx(1.0)           # what it draws now

    def test_the_level_is_the_weight_times_the_fundamental_per_euro_of_cap(self, earnings):
        # w · F / cap(T) = 5 × (2.0 × 100 × 1e6) / 2_000 — the LATEST cap, both years.
        book = _series(earnings.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps=_CAPS_DOUBLING))
        assert book["2025-12-31"] == pytest.approx(5.0 * (2.0 * 100 * 1e6) / 2_000.0)
        assert book["2024-12-31"] == pytest.approx(5.0 * (1.0 * 100 * 1e6) / 2_000.0)

    def test_every_filed_period_survives_on_both_cadences(self, earnings):
        """⚠ THE OUTAGE ITSELF: a per-period lookup keyed by FILING DATE against caps keyed by
        PERIOD misses every time. There is no such lookup left, on either vocabulary."""
        for caps, cadence in ((_CAPS_DOUBLING, "annual"), (_CAPS_QUARTERLY, "quarterly")):
            book = _series(earnings.fundamental_totals(
                [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps=caps, cadence=cadence))
            assert sorted(book) == ["2024-12-31", "2025-12-31"], cadence

    def test_a_member_with_no_cap_at_all_is_left_out_entirely(self, earnings):
        # ⚠ NOT A ZERO, and no longer a period-by-period drop: there is no price at which to turn
        # this book's money weight into shares, so the member has no claim to contribute.
        assert _series(earnings.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps={})) == {}


class TestLtmRidesOnTheSameBasis:
    """⚠ IT USED TO HAVE A FALLBACK OF ITS OWN — the newest cap, where the filed periods each
    demanded their own — which is what let it survive alone when every other period was dropped."""

    @pytest.fixture
    def with_ltm(self, earnings, monkeypatch):
        monkeypatch.setattr(earnings, "_ltm_by_company",
                            lambda *_a, **_k: {1: ("2026-03-31", 3.0)})
        return earnings

    def test_ltm_uses_the_member_s_one_cap_like_every_other_period(self, with_ltm):
        book = _series(with_ltm.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps=_CAPS_DOUBLING))
        assert book["LTM"] == pytest.approx(5.0 * (3.0 * 100 * 1e6) / 2_000.0)

    def test_it_cannot_be_the_only_surviving_period(self, with_ltm):
        # ⚠⚠ THE BLANK CHART, PINNED. No cap ⇒ no member ⇒ no euros at all, which falls cleanly
        # back to the growth chain — rather than one member on one period, which puts
        # `blend_series` on the aggregate path with no step it can span.
        assert _series(with_ltm.fundamental_totals(
            [1], ["fcf_ps"], weight_by_cid={1: 5.0}, caps={})) == {}

    def test_and_an_index_keeps_its_LTM_with_no_caps_at_all(self, with_ltm):
        assert "LTM" in _series(with_ltm.fundamental_totals([1], ["fcf_ps"]))

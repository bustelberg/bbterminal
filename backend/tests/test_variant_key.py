"""`_variant_key` must mirror the frontend's `makeVariantKey`
(`lib/stores/momentum.ts`) byte-for-byte. The UI pre-seeds each variant
table row under this key and routes `variant_result` SSE events back to
it — if the two encoders drift, results land in a "ghost" row the user
never selected while the pre-seeded row stays empty (the exact symptom
the weekday-sweep bug produced when run against a stale backend).
"""
from __future__ import annotations

from routers.momentum.backtest_stream.models import VariantSpec
from routers.momentum.backtest_stream.variants import _variant_key


def _spec(**kw) -> VariantSpec:
    base = {"frequency": "monthly", "strategy_type": "long_only"}
    base.update(kw)
    return VariantSpec(**base)


class TestVariantKey:
    def test_legacy_two_axis_key(self):
        assert _variant_key(_spec()) == "monthly__long_only"

    def test_full_axis_ordering(self):
        # Order must be frequency, strategy, s, p, m, u, g, w.
        k = _variant_key(_spec(
            top_n_sectors=4, top_n_per_sector=6, min_price_score=30.0,
            index_universe="LEONTEQ", grouping="sector", rebalance_weekday=0,
        ))
        assert k == "monthly__long_only__s4__p6__m30__uLEONTEQ__gsector__w0"

    def test_weekday_variants_are_distinct(self):
        # The reported bug: two weekday variants must NOT collapse to one key.
        common = dict(min_price_score=30.0, index_universe="LEONTEQ", grouping="sector")
        mon = _variant_key(_spec(**common, rebalance_weekday=0))
        tue = _variant_key(_spec(**common, rebalance_weekday=1))
        assert mon == "monthly__long_only__m30__uLEONTEQ__gsector__w0"
        assert tue == "monthly__long_only__m30__uLEONTEQ__gsector__w1"
        assert mon != tue

    def test_weekday_zero_is_tagged_not_omitted(self):
        # weekday=0 (Monday) is an explicit value, distinct from "inherit".
        assert _variant_key(_spec(rebalance_weekday=0)) == "monthly__long_only__w0"
        assert _variant_key(_spec()) == "monthly__long_only"  # inherit → no tag

    def test_min_price_score_float_formats_without_trailing_zero(self):
        # Pydantic parses min_price_score as float; `:g` must drop `.0` so
        # the key matches the frontend's `m${number}` (30, not 30.0).
        assert _variant_key(_spec(min_price_score=30.0)) == "monthly__long_only__m30"

    def test_index_universe_wins_over_universe_label(self):
        k = _variant_key(_spec(index_universe="ACWI", universe_label="custom"))
        assert "uACWI" in k and "ucustom" not in k

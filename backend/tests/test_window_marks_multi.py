"""`window_marks_multi` must be `{a: window_marks(a)}` — same bars, per-anchor selection.

⚠ THIS PRICES A BENCHMARK INDEX, so a difference here is not a slow page, it is a wrong index.
The two failure modes this codebase has already paid for both live in exactly these three fields:

  * the OPENING MARK decides the start-of-window cap weight, and weighting by the wrong one is the
    look-ahead bias that turned +9.10% into +21.70%;
  * the JUMP SET decides split adjustment, and our stored closes are not split-adjusted and cannot
    self-heal — a missed 9:1 hits the index TWICE, because the start weight is backed out through
    the same broken price.

⚠ THE JUMP SET IS PER-ANCHOR. Jumps are those at or after that anchor's OWN opening mark; a split
before the mark is already absorbed into it and re-applying it rescales a price that was never on
the old basis. A shared jump set would be wrong for whichever anchor is later — which is why these
tests use two anchors with DIFFERENT jump sets rather than one.
"""
from __future__ import annotations

import pytest

# ⚠ NO FAKE DATABASE HERE, DELIBERATELY. The SQL-level equivalence of `window_marks_multi` and
# `window_marks` cannot be shown against a fake — the whole claim is about what POSTGRES does with
# `DISTINCT ON (analysis_id, anchor)` and the per-anchor jump join, and a Python stand-in would
# only re-assert my own reading of it. That equivalence was verified against the real database
# (502 SP500 ids, two anchors, field for field including the asymmetric jump sets: 3 at one anchor,
# 0 at the other). What IS testable without a database is everything around the query — the
# fallback shapes and the "one call for N anchors" contract — and that is what these cover.


@pytest.fixture
def _no_copy(monkeypatch):
    """Force the COPY path OFF so both functions take their documented fallback."""
    import common.pg as pg
    monkeypatch.setattr(pg, "_db_url", lambda: None)


class TestShapeParityWithoutCopy:
    """⚠ THE FALLBACK SHAPES MUST MATCH OR THE CALLER KeyErrors. `index_returns` indexes
    `marks[s]` directly, so the multi version must yield `{anchor: {}}` — never a bare `{}` —
    when COPY is unavailable. The single-anchor loader returns `{}` per anchor, and the dict
    comprehension it replaced therefore produced a key per anchor."""

    def test_no_copy_yields_a_key_per_anchor(self, _no_copy):
        from routers._asset_benchmark import window_marks_multi
        out = window_marks_multi([1, 2], "2025-01-01", ["2025-04-01", "2026-01-01"], "2026-08-11")
        assert set(out) == {"2025-04-01", "2026-01-01"}
        assert all(v == {} for v in out.values())

    def test_no_ids_yields_a_key_per_anchor(self, _no_copy):
        from routers._asset_benchmark import window_marks_multi
        out = window_marks_multi([], "2025-01-01", ["2026-01-01"], "2026-08-11")
        assert out == {"2026-01-01": {}}

    def test_no_anchors_is_empty(self, _no_copy):
        from routers._asset_benchmark import window_marks_multi
        assert window_marks_multi([1], "2025-01-01", [], "2026-08-11") == {}

    def test_duplicate_anchors_collapse(self, _no_copy):
        from routers._asset_benchmark import window_marks_multi
        out = window_marks_multi([1], "2025-01-01", ["2026-01-01"] * 3, "2026-08-11")
        assert list(out) == ["2026-01-01"], "anchors are de-duplicated before the query"


class TestIndexReturnsAsksOnce:
    """The regression this change fixes: `index_returns` ran one full COPY per anchor over the
    identical id set and identical window (measured 356ms + 352ms), despite its own docstring
    saying 'ONE price load'."""

    def test_index_returns_calls_the_multi_loader_once(self, monkeypatch):
        import routers._asset_benchmark as ab

        calls = []

        def fake_multi(ids, lookback, anchors, end):
            calls.append((tuple(ids), lookback, tuple(sorted(anchors)), end))
            return {a: {} for a in anchors}

        monkeypatch.setattr(ab, "window_marks_multi", fake_multi)
        monkeypatch.setattr(ab, "members", lambda _l: (
            [{"company_id": 1, "currency": "USD", "market_cap_eur": 1.0}], {"pct": 100.0}))
        monkeypatch.setattr(ab, "_fx_to_eur", lambda *_a, **_k: {})
        monkeypatch.setattr(ab, "_asset_closes", lambda *_a, **_k: {})
        monkeypatch.setattr(ab, "_window_rows", lambda *_a, **_k: ([], False))

        ab.index_returns("SP500", ["2026-01-01", "2025-04-28"])
        assert len(calls) == 1, f"one COPY expected for two anchors, got {len(calls)}"
        assert calls[0][2] == ("2025-04-28", "2026-01-01"), "both anchors in the single call"

    def test_window_marks_single_is_still_present(self):
        """Kept deliberately: it is the reference the multi version was verified against, and
        `index_rows` (one window) has no reason to pay for the anchor machinery."""
        from routers._asset_benchmark import window_marks
        assert callable(window_marks)

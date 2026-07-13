"""Adding a row by ISIN must never RE-RESOLVE one that already exists.

THE INCIDENT (2026-07-13, on the live local grid): calling `store_one` on
US02079K3059 — Alphabet Class A, already resolved to GOOGL — silently repointed it:

    before   GOOGL     EUR 8,785,142,579 median daily traded value   5,502 bars from 2004
    after    GOOA.VI   EUR        76,634                              2,302 bars from 2017
                       ^ VIENNA. A 75,000x thinner listing. No error, no warning.

WHY IT HAPPENS. `resolve()` ranks Yahoo's candidates by median traded value and would pick
GOOGL every time — it scores EUR 8.79bn against GOOA.VI's EUR 76k. But it can only rank
what it is given, and Yahoo answers a search with an EMPTY LIST under load rather than a
429 (the trap `asset_pipeline/fast_resolve.py` was written for). Drop the real listing from
the candidate set and the thin foreign line wins by default. This is the NVDA-on-Stuttgart
failure mode reached by a different road.

So the guard is not about idempotency or politeness. Re-resolution is DESTRUCTIVE when it
goes wrong, and "add a row" must never be able to destroy one. Re-resolving is a deliberate
act with its own control (the per-row Resolve action).
"""
from __future__ import annotations


class TestTheListingsAreNotInterchangeable:
    """The numbers that make the swap a data-loss event rather than a cosmetic one."""

    GOOGL = {"symbol": "GOOGL", "med_adv_eur": 8_785_142_579.0, "bars": 5502, "years": 21.9}
    GOOA_VI = {"symbol": "GOOA.VI", "med_adv_eur": 76_634.0, "bars": 2302, "years": 9.1}

    def test_the_vienna_line_is_four_orders_of_magnitude_thinner(self):
        ratio = self.GOOGL["med_adv_eur"] / self.GOOA_VI["med_adv_eur"]
        assert ratio > 10_000        # ~75,000x

    def test_ranking_by_traded_value_prefers_the_real_listing(self):
        """`resolve()` sorts by -med_adv_eur, so it picks GOOGL — WHEN GOOGL IS PRESENT.
        The bug is never the ranking; it is the candidate set."""
        best = max([self.GOOGL, self.GOOA_VI], key=lambda s: s["med_adv_eur"])
        assert best["symbol"] == "GOOGL"

    def test_an_empty_candidate_set_hands_the_win_to_the_thin_line(self):
        """Yahoo returns [] under load instead of erroring. With GOOGL missing, the very
        same ranking now 'correctly' selects a Vienna listing — silently."""
        candidates = [self.GOOA_VI]                      # GOOGL dropped by an empty search
        best = max(candidates, key=lambda s: s["med_adv_eur"])
        assert best["symbol"] == "GOOA.VI"               # no error, no warning, wrong answer


class TestAResolutionWithoutPricesIsNotAResolution:
    """The second incident, from the same bulk run (2026-07-13).

    TEN distinct Leonteq structured products — CH1369849273, CH1381833321, CH1550438902,
    … all issued by Leonteq Securities AG (Guernsey Branch) — each resolved to the SAME
    symbol, GODE.DE, and each was written as `status='ok'`:

        GODE.DE   bars=None   price_from=None   price_to=None

    Zero price data. Yahoo has no listing for a structured product, so its search returned
    a name-alike; the ranker took it because nothing better was on offer; and ten unrelated
    instruments ended up pointing at one empty series. Ten confident rows, no data behind
    any of them, and nothing errored.

    ZERO BARS IS THE TELL, and it is unambiguous: this grid exists to price instruments, so
    an instrument with no prices was not found. `store_one` now records the ISIN as unmapped
    (the honest answer for a structured product) rather than keeping a mapping that only
    looks like one.
    """

    def test_store_one_rejects_a_zero_bar_resolution(self):
        import inspect

        from asset_pipeline import store

        src = inspect.getsource(store.store_one)
        assert "if not rows:" in src, "a zero-bar resolution must not be stored as a mapping"
        # It must RECORD the ISIN (so the row exists, unmapped) and then raise.
        after = src.split("if not rows:", 1)[1]
        assert "upsert_unmapped" in after, "the ISIN must still be recorded, just unmapped"
        assert "raise ValueError" in after

    def test_the_check_happens_AFTER_the_series_is_fetched(self):
        """It has to: `rows` is the count `store_series` actually wrote. Checking anything
        earlier would only be guessing at whether prices exist."""
        import inspect

        from asset_pipeline import store

        src = inspect.getsource(store.store_one)
        assert src.index("store_series") < src.index("if not rows:")

    def test_ten_isins_collapsing_to_one_symbol_is_the_signature(self):
        """What the failure looked like in the data — kept as the description of the bug,
        so a future reader recognises it. One symbol, ten ISINs, no bars."""
        rows = [
            {"isin": i, "symbol": "GODE.DE", "bars": None}
            for i in ("CH1369849273", "CH1381833321", "CH1550438902", "CH1550438910",
                      "CH1550438928", "CH1550438936", "CH1550442797", "CH1571717235",
                      "CH1571725139", "CH1525090200")
        ]
        assert len({r["symbol"] for r in rows}) == 1        # ten ISINs, ONE symbol
        assert all(not r["bars"] for r in rows)             # and not a single price bar


class TestTheGuard:
    def test_the_store_route_refuses_an_existing_isin(self):
        """A source-level check: the handler must look the ISIN up and 409 BEFORE calling
        store_one. If someone deletes this, the incident above comes straight back."""
        import inspect

        from routers import asset_pipeline

        src = inspect.getsource(asset_pipeline.store_one)
        assert "asset_grid" in src, "the route no longer checks whether the ISIN exists"
        assert "409" in src, "an existing ISIN must be refused, not re-resolved"
        # The lookup has to happen BEFORE the resolve, or the guard is decorative.
        assert src.index("asset_grid") < src.index("store.store_one")

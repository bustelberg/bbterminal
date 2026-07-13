"""Repointing an ETF off a thin listing must be anchored on the ISIN, never on the NAME.

THE HAZARD, straight out of the grid (2026-07-13):

    IE00BNDS1P30   V3GF.MI   Vanguard ESG Global All Cap UCITS ETF
    IE00BNDS1Q47   V3GE.DE   Vanguard ESG Global All Cap UCITS ETF

Two rows. Two ISINs. One name. They are different SHARE CLASSES of one fund — accumulating
vs distributing (and/or a currency variant). An accumulating class rolls dividends back into
the price; a distributing one pays them out. The two therefore compound differently: swapping
one for the other is not a cosmetic venue change, it is a WRONG PRICE SERIES.

`same_company()` — the right tool for an operating company, because it strips corporate forms
so "NVIDIA Corporation" matches "NVIDIA CORP" — cannot see the difference between these two.
Nothing name-based can. So the safety cannot be a name heuristic; it has to be structural:
candidates come from OpenFIGI's listings OF THE ONE ISIN, which by construction contains only
the venues that single share class trades on.
"""
from __future__ import annotations

import inspect


class TestTwoShareClassesOneName:
    A = {"isin": "IE00BNDS1P30", "symbol": "V3GF.MI",
         "name": "Vanguard ESG Global All Cap UCITS ETF"}
    B = {"isin": "IE00BNDS1Q47", "symbol": "V3GE.DE",
         "name": "Vanguard ESG Global All Cap UCITS ETF"}

    def test_a_name_check_cannot_tell_them_apart(self):
        """The premise of the whole file. If this ever fails, a name gate became viable
        and this test should be revisited — but do not assume it silently."""
        from asset_pipeline.resolve import same_company

        assert same_company(self.A["name"], self.B["name"]) is True
        assert self.A["isin"] != self.B["isin"]          # ...yet they are different securities

    def test_they_are_distinct_rows_in_the_grid(self):
        """Both are held by the AIRS portfolios, so this is not hypothetical: a name-based
        re-resolve of one has the other sitting right there to be swapped in."""
        assert self.A["symbol"] != self.B["symbol"]


class TestTheRepointerIsIsinAnchored:
    def test_candidates_come_from_openfigi_isin_lookup_not_a_name_search(self):
        """The structural guarantee. `lookup_isin` returns the venues of ONE ISIN, so a
        sibling share class (a different ISIN) can never enter the candidate set."""
        from scripts import repoint_etf_listing as r

        src = inspect.getsource(r.main)
        assert "lookup_isin" in src, "candidates must be enumerated FROM THE ISIN"
        assert "build_candidates" in src
        # The name-search resolver must not appear: it is what crosses share classes.
        assert "resolve(" not in src.replace("resolve_analysis_instrument(", "")

    def test_a_zero_bar_winner_is_rejected(self):
        """Same rule as `store_one`: a resolution with no price series is not a resolution.
        Here it is sharper — we'd be trading a working thin row for an empty one."""
        from scripts import repoint_etf_listing as r

        src = inspect.getsource(r.main)
        assert "if not rows:" in src

    def test_it_only_swaps_for_a_materially_more_liquid_venue(self):
        from scripts import repoint_etf_listing as r

        src = inspect.getsource(r.main)
        assert "min_gain" in src


class TestFastResolveUsesSameCompanyNotARawFloor:
    """`fast_resolve` was the last place still gating on a raw `_name_score >= 80` floor —
    the anti-pattern that put NVIDIA on Stuttgart. Here a false reject cannot corrupt a row
    (candidates are ISIN-anchored), but it drops the LIQUID listing and strands the name on
    the thin cross-listing, which is precisely the bug the function exists to prevent."""

    def test_the_raw_floor_is_gone(self):
        from asset_pipeline import fast_resolve

        src = inspect.getsource(fast_resolve.fast_resolve)
        assert "_NAME_MATCH" not in src, "a raw token_set_ratio floor is banned — use same_company"
        assert "same_company" in src

    def test_the_floor_would_have_rejected_a_real_match(self):
        """Why the floor is wrong, in numbers, so nobody restores it."""
        from asset_pipeline.resolve import _name_score, _NAME_MATCH, same_company

        assert _name_score("NVIDIA Corporation", "NVIDIA CORP") < _NAME_MATCH   # 75.9
        assert same_company("NVIDIA Corporation", "NVIDIA CORP") is True

"""Repointing a thin listing resolves BY NAME — so it may only ever touch an operating company.

THE HAZARD, produced by this script's own candidate list (2026-07-14):

    JE00BN7KB557   COFF.L   WisdomTree Coffee      an ETC on coffee FUTURES
                -> LKNCY             Luckin Coffee Inc — a Chinese coffee RETAILER

    same_company("Luckin Coffee Inc", "WT COFFEE") -> True    (token_set_ratio = 80.0)

The stored OpenFIGI name is "WT COFFEE", and the word "coffee" alone carries it over the floor.
The identity gate — the thing that exists to stop exactly this — waved it through, and nothing
downstream would have caught it either: LKNCY is a real, liquid, priced listing. The ETC would
simply have started reporting the share price of an unrelated company.

WHY IT HAD NEVER FIRED
    The script required a market cap (to compute its ADV/cap ratio), and a fund does not report
    one — all 29 ETP rows in the grid have none. So the cap check was, BY ACCIDENT, the fence
    keeping funds away from a name-based resolver. The moment `--no-cap-max-adv` swept in the
    rows with no cap — which is where the genuinely broken listings live, because a dead listing
    is precisely what fails to report a cap — the hole opened.

    That is the lesson worth keeping: the protection was incidental, so it was invisible, so it
    was removable without anyone noticing. It is now explicit and structural.
"""
from __future__ import annotations

import inspect

# The line that IS the `--isin` bypass. Anchoring on it rather than on `"if isin:"`, which also
# appears in the query builder above the loop.
_ISIN_BYPASS = 'r["_ratio"] = (float(adv) / float(cap)) if priced else None'


class TestTheCoffeeSwap:
    def test_the_identity_gate_does_NOT_catch_it(self):
        """The premise. If `same_company` rejected this pair there would be no bug — and the
        fence below would be redundant. It does not: 'coffee' is enough."""
        from asset_pipeline.resolve import same_company

        assert same_company("Luckin Coffee Inc", "WT COFFEE") is True
        # ...and the operating-company case it exists for still works, so it cannot just be
        # tightened: this is the same function that must match NVIDIA to "NVIDIA CORP".
        assert same_company("NVIDIA Corporation", "NVIDIA CORP") is True

    def test_a_fund_is_not_name_resolvable(self):
        from scripts import repoint_primary_listing as r

        assert "ETP" not in r._NAME_RESOLVABLE_TYPES
        assert "Closed-End Fund" not in r._NAME_RESOLVABLE_TYPES
        # An operating company still is — including the shapes an ADR/REIT takes.
        assert "Common Stock" in r._NAME_RESOLVABLE_TYPES
        assert "ADR" in r._NAME_RESOLVABLE_TYPES
        assert "REIT" in r._NAME_RESOLVABLE_TYPES

    def test_the_fence_holds_even_for_an_explicit_isin(self):
        """`--isin` bypasses the RATIO. It must not bypass the identity model — a human naming a
        fund's ISIN is exactly as wrong as the sweep finding it, and more confident."""
        from scripts import repoint_primary_listing as r

        src = inspect.getsource(r._candidates)
        fence = src.index('r.get("openfigi_type") not in _NAME_RESOLVABLE_TYPES')
        bypass = src.index(_ISIN_BYPASS)
        assert fence < bypass, "the type fence must precede the --isin bypass"

    def test_refusals_are_reported_not_silent(self):
        """A skipped row is not a healthy row — it is one that needs `repoint_etf_listing.py`.
        Saying nothing reads as 'nothing to do'."""
        from scripts import repoint_primary_listing as r

        src = inspect.getsource(r._candidates)
        assert "skipped_funds" in src
        assert "repoint_etf_listing.py" in src


class TestTheBlindSpot:
    """Brown & Brown (`US1152361010`) sat on BTW.DE — Xetra, EUR, €7,836/day, no market cap, and
    NOT ONE PRICE BAR — while its real market, NYSE `BRO`, does €154m/day. 19,698x.

    The detector could not see it BECAUSE it was broken: no cap means no ratio, and a row with no
    ratio was skipped. The rows most likely to be wrong were the ones it never looked at.
    """

    def test_an_explicit_isin_bypasses_the_cap_requirement(self):
        """`--isin` is documented as bypassing the ratio filter. It has to bypass the market cap
        that filter is COMPUTED FROM, or it silently reports '0 candidates' for the worst rows —
        which is what it did for Brown & Brown."""
        from scripts import repoint_primary_listing as r

        src = inspect.getsource(r._candidates)
        after = src.split(_ISIN_BYPASS, 1)[1].split("continue", 1)[0]
        assert "out.append(r)" in after      # appended WITHOUT requiring `priced`

    def test_a_missing_cap_is_a_signal_not_a_skip(self):
        from scripts import repoint_primary_listing as r

        src = inspect.getsource(r._candidates)
        assert "no_cap_max_adv" in src

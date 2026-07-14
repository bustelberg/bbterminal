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


class TestResolvingAQueuedFund:
    """A `queued` ETF has NEVER been resolved: no symbol, no prices, invisible everywhere. It is
    the row that most needs an ISIN-anchored resolve — and the one the tool could not touch.

        IE00BP3QZ825   iShares Edge MSCI World Momentum   status=queued, no Yahoo symbol

    THE ANCHOR PROBLEM
        `_same_fund` gates a candidate against the INCUMBENT'S YAHOO NAME. A queued row has no
        incumbent, so there is no such name — and the two names it does have are both unusable,
        for the identical reason documented on `_same_fund`: they are vowel-crushed.

            OpenFIGI:  ISHR EDGE MSCI WRLD MOMENTUM
            Leonteq:   ISHR EDGE MSCI WRLD MOMENTUM
            Yahoo:     iShares Edge MSCI World Momentum Factor UCITS ETF

        Anchoring on either rejects every candidate, including the right one.

    THE ANSWER IS STRUCTURAL, NOT A LOOSER MATCHER
        The candidates all come from OpenFIGI's listings OF THIS ONE ISIN, and a fund's venues
        report near-identical names *to Yahoo* — same source, same convention. So the anchor is
        their AGREEMENT. A constructed ticker that collided with an unrelated instrument on some
        venue is the outlier that agrees with nobody. A lone candidate is NOT a consensus.
    """

    def test_venues_of_one_fund_agree_and_the_consensus_is_the_anchor(self):
        from scripts.repoint_etf_listing import _consensus_anchor

        names = [
            "iShares Edge MSCI World Momentum Factor UCITS ETF",       # IS3R.DE
            "iShares Edge MSCI World Momentum Factor UCITS ETF USD",   # IWMO.L
            "iShares Edge MSCI World Momentum Factor UCITS ETF",       # IWFM.L
            "Some Unrelated Mining Corp",                              # a ticker collision
        ]
        anchor = _consensus_anchor(names)
        assert anchor is not None
        assert "Momentum" in anchor
        assert "Mining" not in anchor          # the outlier can never BE the anchor

    def test_a_lone_candidate_is_not_a_consensus(self):
        """One name agrees with itself. That is not corroboration, it is an unchecked guess —
        and an unchecked swap is the one thing this file exists to refuse."""
        from scripts.repoint_etf_listing import _consensus_anchor

        assert _consensus_anchor(["iShares Edge MSCI World Momentum Factor UCITS ETF"]) is None
        assert _consensus_anchor([]) is None

    def test_disagreeing_candidates_yield_no_anchor(self):
        from scripts.repoint_etf_listing import _consensus_anchor

        assert _consensus_anchor(["Alpha Mining Corp", "Beta Software Inc"]) is None

    def test_an_explicit_isin_reaches_a_queued_row(self):
        """The sweep only looks at `status='ok'` rows with an incumbent — correctly, since a
        queued row has no thinness to measure. Named explicitly, it must be judged anyway."""
        from scripts import repoint_etf_listing as r

        src = inspect.getsource(r._thin_etfs)
        assert 'q = q.eq("isin", isin)' in src
        assert 'q.eq("status", "ok")' in src
        # ...and the status filter must NOT be applied on the explicit-ISIN branch. (Match the
        # CALL, not the word — the branch's comment says "whatever its status".)
        explicit = src.split("if isin:", 1)[1].split("else:", 1)[0]
        assert '.eq("status"' not in explicit

    def test_the_incumbent_checks_are_skipped_when_there_is_no_incumbent(self):
        """`old` is None for a queued row. Guarding on it is what stops "the incumbent failed its
        own name gate" firing against a row that never had one."""
        from scripts import repoint_etf_listing as r

        src = inspect.getsource(r.main)
        assert "if old and old not in symbols:" in src
        assert "if old and not any(" in src


class TestTheConstructedSymbolCannotReachEveryVenue:
    """The hole `yahoo_isin` closes, measured on DE000A0F5UH1 (iShares STOXX Global Select
    Dividend 100), which sat QUEUED — unresolved, unpriced, invisible.

    Every other candidate source CONSTRUCTS a Yahoo symbol as `ticker + venue suffix`. That
    quietly assumes OpenFIGI and Yahoo agree on the ticker. On the German venues they do not:

        OpenFIGI, exchCode GR/GF/GD/GS/... :  SDGPEX     -> we build SDGPEX.DE  (does not exist)
        Yahoo, XETRA                       :  ISPA.DE    <- EUR 5,483,388/day

    So Xetra — the fund's most liquid line by 58x — was UNREACHABLE BY CONSTRUCTION, and the
    only candidates on offer were Vienna (EUR 31,824/day) and Zurich (EUR 94,776/day). No name
    gate, liquidity rank or consensus anchor can rescue a candidate set that never contained
    the right answer; this is the same shape as the empty-search that put Alphabet on Vienna.

    Asking Yahoo to resolve the ISIN returns ISPA.DE with no ticker guess in the middle.
    """

    def test_yahoo_isin_is_a_candidate_source_never_an_answer(self):
        """⚠ THE LOAD-BEARING ONE. Yahoo resolves an ISIN to *a* listing, not the *liquid* one,
        and its pick is routinely the wrong end of that — it answers Alphabet's ISIN with
        `1GOOGL.MI` (Milan) and this momentum ETF with the thinner London line:

            IE00BP3QZ825  ->  IWFM.L   EUR 1,442,631/day     (Yahoo's ISIN pick)
                              IS3R.DE  EUR 5,627,530/day     (the incumbent; 3.9x more liquid)

        Taking it as the answer would rebuild the exact wrong-listing bug this file exists to
        prevent. It may only ever ADD to the pool: the ranker and the name gate still decide.
        """
        from asset_pipeline import fast_resolve

        src = inspect.getsource(fast_resolve.build_candidates)
        # It appends into the same list the other sources feed, and returns that list — it does
        # not short-circuit to Yahoo's answer.
        assert "cands.append(s)" in src
        assert "return cands[:limit]" in src
        assert "_from_yahoo_isin" in src

        # And the repointer still ranks by traded value and gates on the name afterwards.
        from scripts import repoint_etf_listing as r
        main = inspect.getsource(r.main)
        assert "yahoo_isin=True" in main
        assert 'max(scored, key=lambda s: float(s.get("med_adv_eur") or 0))' in main
        assert "_same_fund(sc.get(\"name\"), anchor)" in main

    def test_it_is_off_by_default_so_the_bulk_path_stays_one_call_per_isin(self):
        """`fast_resolve` exists to be ~1 Yahoo call/ISIN on a bulk run. An extra search per
        ISIN is how you get throttled — and Yahoo answers a throttled caller with an EMPTY
        list, not a 429, which is precisely how a row lands on a thin foreign listing. Only the
        deliberate, per-row repointers opt in."""
        import inspect as _i

        from asset_pipeline import fast_resolve

        sig = _i.signature(fast_resolve.build_candidates)
        assert sig.parameters["yahoo_isin"].default is False

    def test_an_empty_search_costs_the_improvement_never_the_correctness(self):
        """Yahoo returns [] under load rather than a 429. Since this source only ADDS
        candidates, an empty (or raising) search degrades to the constructed set — the previous
        behaviour — instead of corrupting the pick or claiming 'no listing'."""
        from asset_pipeline import fast_resolve, yahoo

        orig = yahoo.search
        try:
            yahoo.search = lambda *a, **k: []          # the throttled-empty response
            assert fast_resolve._from_yahoo_isin("DE000A0F5UH1") == []

            def _boom(*a, **k):
                raise RuntimeError("yahoo is down")

            yahoo.search = _boom
            assert fast_resolve._from_yahoo_isin("DE000A0F5UH1") == []
        finally:
            yahoo.search = orig

    def test_the_isin_pseudo_symbol_is_dropped(self):
        """For some funds Yahoo hands back the ISIN ITSELF plus a venue suffix — a Stuttgart
        placeholder, not a listing we can price:

            IE00BNDS1P30  ->  IE00BNDS1P30.SG   (quoteType MUTUALFUND)

        Left in, it is a candidate that can only fail a probe. Dropped here."""
        from asset_pipeline import fast_resolve, yahoo

        orig = yahoo.search
        try:
            yahoo.search = lambda *a, **k: [
                {"symbol": "IE00BNDS1P30.SG", "quoteType": "MUTUALFUND"},
                {"symbol": "V3GF.MI", "quoteType": "ETF"},
                {"symbol": "EURUSD=X", "quoteType": "CURRENCY"},
            ]
            assert fast_resolve._from_yahoo_isin("IE00BNDS1P30") == ["V3GF.MI"]
        finally:
            yahoo.search = orig

    def test_the_yahoo_candidate_outranks_the_constructed_ones(self):
        """Order matters because `build_candidates` truncates at `limit`. Yahoo resolved the
        ISIN itself — no ticker guess — so it must not be the candidate that falls off the
        cliff. (A UCITS ETF genuinely lists on 6-10 venues; the repointer probes 12.)"""
        from asset_pipeline import fast_resolve, yahoo

        figi = [{"ticker": "SDGPEX", "exchCode": "GR", "securityType": "ETP"},
                {"ticker": "EX46", "exchCode": "AV", "securityType": "ETP"}]
        orig = yahoo.search
        try:
            yahoo.search = lambda *a, **k: [{"symbol": "ISPA.DE", "quoteType": "ETF"}]
            got = fast_resolve.build_candidates(
                "DE000A0F5UH1", figi, None, limit=12, yahoo_isin=True)
        finally:
            yahoo.search = orig

        assert got[0] == "ISPA.DE"
        # ...and it did not REPLACE the constructed set, only lead it.
        assert "SDGPEX.DE" in got and "EX46.VI" in got

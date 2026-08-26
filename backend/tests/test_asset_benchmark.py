"""A benchmark rebuilt in the ASSET world (yfinance), joined by ISIN.

WHY IT EXISTS
    `_benchmark_index` prices off GuruFocus, and GuruFocus sells a subscription WITH HOLES: no
    UK, no India, no Ireland, no Australia/NZ, no Africa, no LatAm. Invisible for the S&P (a US
    index). DISQUALIFYING for ACWI:

        LSE (UK)     72 ACWI members    GuruFocus prices  0
        NSE (India) 160 ACWI members    GuruFocus prices  0

    ~7.8% of ACWI's published weight is in countries GuruFocus will never price. A cap-weighted
    index renormalised over the other 92% does not LOSE that weight — it redistributes it into
    everything else. That is a bias, and no care inside the arithmetic removes it.

    yfinance has no such holes, and we already hold those prices in `asset_price`.
"""
from __future__ import annotations

import inspect

import pytest

from routers import _asset_benchmark as ab
from tests._fake_supabase import FakeSupabase


@pytest.fixture
def index(monkeypatch):
    """A three-company index of which TWO cross the bridge into the asset world.

    ⚠ THE THIRD COMPANY IS THE POINT OF THE FIXTURE, not padding. `universe_membership` (the
    company world, where membership is authored) holds three; `universe_asset_membership` (the
    bridge view) holds two. That gap IS the India/UK loss in miniature, and it is the only reason
    `covered_pct` can be checked against something other than 100%.
    """
    fake = FakeSupabase({
        "universe": [{"universe_id": 9, "label": "SP500"}],
        "universe_membership": [{"universe_id": 9, "company_id": c} for c in (1, 2, 3)],
        "universe_asset_membership": [{"universe_id": 9, "analysis_id": a} for a in (10, 20)],
        "asset_grid": [
            {"analysis_id": 10, "isin": "US-A", "yahoo_symbol": "AAA", "name": "Alpha Inc",
             "gf_company_name": "Alpha Inc", "currency": "USD", "market_cap_eur": 1_000.0,
             "market_cap_currency": "USD", "status": "ok", "bars": 900, "is_default": True,
             "delisted_at": None, "out_of_scope_at": None},
            {"analysis_id": 20, "isin": "US-B", "yahoo_symbol": "BBB", "name": "Beta Inc",
             "gf_company_name": "Beta Inc", "currency": "USD", "market_cap_eur": 500.0,
             "market_cap_currency": "USD", "status": "ok", "bars": 800, "is_default": True,
             "delisted_at": None, "out_of_scope_at": None},
        ],
        "asset_analysis": [],
    })
    monkeypatch.setattr(ab, "supabase", fake)
    return fake, ab


def _add_share_classes(fake, *classes) -> None:
    """Add N share classes of ONE company — different assets, different ISINs, SAME company, and
    each carrying the FULL company cap, which is what Yahoo actually reports.

    ⚠ A HELPER BECAUSE A ONE-ROW VERSION OF THIS TEST PASSES WITHOUT THE DEDUPE. Asserting "exactly
    one Alphabet survives" against a fixture holding one Alphabet is true whatever the code does;
    the fixture has to contain the collision for the assertion to mean anything.
    """
    for i, (symbol, cap) in enumerate(classes):
        aid = 30 + i
        fake.tables["universe_asset_membership"].append({"universe_id": 9, "analysis_id": aid})
        fake.tables["asset_grid"].append(
            {"analysis_id": aid, "isin": f"US-ALPHABET-{i}", "yahoo_symbol": symbol,
             "name": f"Alphabet Inc {'AC'[i]}", "gf_company_name": "Alphabet Inc",
             "currency": "USD", "market_cap_eur": cap, "market_cap_currency": "USD",
             "status": "ok", "bars": 5000, "is_default": True,
             "delisted_at": None, "out_of_scope_at": None})


class TestTheBridgeIsAJoinNotAColumn:
    """A membership FLAG on `asset_execution` was the obvious alternative, and it is a trap: the
    ACWI universe is RECONSTRUCTED on a schedule, so the flag would need re-syncing on every
    refresh — and the day it drifts, the benchmark is quietly wrong with no error anywhere.

    Same rule the holdings count already follows: *the count is a VIEW, never a column.* A join
    cannot drift, because it has nothing to keep in sync.
    """

    def test_membership_is_resolved_through_the_isin(self, index):
        """⚠ REWRITTEN FROM A SOURCE GREP TO A BEHAVIOURAL TEST (2026-08-10). It asserted
        `'table("company")' in inspect.getsource(members)`, and went red when the three-hop join
        moved INTO the database as the `universe_asset_membership` view (migration 20260806060000)
        — i.e. it failed at the exact moment the rule it names got stronger, because a view cannot
        drift from the membership it mirrors at all.

        That is the failure mode of asserting on spelling: it cannot tell a refactor from a
        regression, and the cheap way to green is to paste in whatever string the code now
        contains, which pins nothing. So it now asserts the RULE — a constituent reaches the index
        only by being in the bridge — which survives the join moving to SQL, to a helper, or back.
        """
        fake, ab_mod = index
        out, coverage = ab_mod.members("SP500")

        assert {m["isin"] for m in out} == {"US-A", "US-B"}
        # ⚠ THE DENOMINATOR IS THE COMPANY-WORLD COUNT, NOT THE BRIDGE'S. Three companies are in
        # the index; only two cross into the asset world. Taking the denominator from the bridge
        # would report 100% coverage while a third of the index was missing — the loss would
        # vanish into the number that exists to report it.
        #
        # ⚠ THE THREE COVERAGE KEYS THIS TEST IS ABOUT, NOT THE WHOLE DICT. `coverage` also carries
        # the cap-stamp range (`caps_from`/`caps_to`/`caps_unstamped`), which is a different report
        # about a different thing; an exact-dict assertion here went red the day those were added
        # and said "the bridge is broken", which is the one thing that had not changed. A test
        # named for a rule asserts that rule.
        assert coverage["universe_members"] == 3
        assert coverage["priced"] == 2
        assert coverage["covered_pct"] == pytest.approx(200 / 3)

        # Drop the bridge row and the constituent leaves the index, though its `asset_grid` row is
        # untouched. A stored membership FLAG could not behave this way without being re-synced.
        fake.tables["universe_asset_membership"] = [
            r for r in fake.tables["universe_asset_membership"] if r["analysis_id"] != 20]
        out2, _ = ab_mod.members("SP500")
        assert {m["isin"] for m in out2} == {"US-A"}

    def test_no_membership_column_is_read(self):
        """If someone adds `asset_execution.acwi`, this catches the moment it gets read."""
        src = inspect.getsource(ab)
        for flag in ('"acwi"', "'acwi'", '"in_sp500"', '"universe_label"'):
            assert flag not in src.lower(), f"membership must be a join, not a stored flag ({flag})"


class TestTheWeightingIsREUSEDNotCopied:
    """⚠ START-OF-WINDOW CAP WEIGHTS. Weighting by TODAY's cap is look-ahead bias — it turned
    +9.10% into +21.70%. A second copy of that loop is a second place for the bias to grow back,
    so this module supplies `members` + `closes` from a different source and calls the SAME
    `_benchmark_index._window_rows` that /benchmarks uses."""

    def test_every_surface_calls_the_shared_window_rows(self):
        """Not pinned to an argument list — `marks=` was added there and the point is the CALL."""
        for fn in (ab.index_returns, ab.index_rows, ab.compute_index):
            assert "_window_rows(" in inspect.getsource(fn), fn.__name__

    def test_the_narrow_loader_selects_marks_and_does_not_price_them(self):
        """⚠ `window_marks` may fetch less, never compute differently. The moment it grows a
        return, a weight or an FX conversion there are two definitions of an index return and the
        cheap one is the one nobody cross-checks."""
        # The docstring EXPLAINS the weighting it must not do, so scan the code only.
        src = inspect.getsource(ab.window_marks)
        code = src.split('"""')[2] if src.count('"""') >= 2 else src
        for forbidden in ("return_eur_pct", "return_local_pct", "weight_pct", "start_cap_eur",
                          "market_cap", "_rate(", "index_weights"):
            assert forbidden not in code, f"window_marks must only SELECT ({forbidden})"

    def test_it_does_not_reimplement_the_cap_rollback(self):
        src = inspect.getsource(ab)
        assert "cap_start_eur" not in src, "the weighting lives in _window_rows, not here"


class TestOneCompanyOneRow:
    def test_share_classes_are_deduped(self, index):
        """Yahoo, like GuruFocus, puts the FULL company cap on EVERY share class. Alphabet is
        GOOGL *and* GOOG, each carrying the whole cap — a naive sum counts it twice (11.3% of the
        S&P's weight, fictional).

        ⚠ AND THE ASSET WORLD DOES NOT MAKE THIS MOOT, which is the trap worth pinning: keying on
        `analysis_id` collapses a company's LISTINGS, not its SHARE CLASSES. Those carry different
        ISINs, so they are different assets — two rows, each with the full cap, exactly as before
        the repoint. (Formerly a grep for `'c.get("company_name") or ""'`, which went red when the
        key learned to prefer `gf_company_name`; the dedupe itself never changed.)
        """
        fake, ab_mod = index
        _add_share_classes(fake, ("GOOGL", 3.9e12), ("GOOG", 3.9e12))

        out, _ = ab_mod.members("SP500")

        alphabet = [m for m in out if m["company_name"] == "Alphabet Inc"]
        assert len(alphabet) == 1, "both share classes survived — the index gained a phantom 3.9tn"
        assert alphabet[0]["market_cap_eur"] == pytest.approx(3.9e12)   # kept, NOT summed to 7.8tn
        assert sum(m["market_cap_eur"] for m in out) == pytest.approx(1_500.0 + 3.9e12)

    def test_the_dedupe_keys_on_the_company_not_the_listing_name(self, index):
        """The key prefers `gf_company_name` — the company-world name — so two share classes whose
        ASSET names differ ("Alphabet Inc A" / "Alphabet Inc C") still collapse. Keying on the
        asset name would let them both through, which is the bug in its original form."""
        fake, ab_mod = index
        # ⚠ THE TWO ASSET NAMES DIFFER — "Alphabet Inc A" vs "Alphabet Inc C" — so a key built from
        # `name` would let both through. Only the company-world name collapses them.
        _add_share_classes(fake, ("GOOGL", 3.9e12), ("GOOG", 4.1e12))

        out, _ = ab_mod.members("SP500")

        alphabet = [m for m in out if m["company_name"] == "Alphabet Inc"]
        assert len(alphabet) == 1
        # The LARGEST cap wins, so the row kept is deterministic rather than whichever the
        # database happened to return first.
        assert alphabet[0]["market_cap_eur"] == pytest.approx(4.1e12)


class TestCoverageIsNeverAssumed:
    """ACWI's missing names go A WHOLE COUNTRY AT A TIME. Reporting a renormalised index without
    saying what it was renormalised over is the same invention the portfolio returns refuse to
    make — and here it is systematic, not random."""

    def test_members_returns_its_own_coverage(self):
        src = inspect.getsource(ab.members)
        assert '"covered_pct"' in src
        assert '"universe_members"' in src and '"priced"' in src

    def test_coverage_rides_along_with_every_window(self):
        assert "**coverage" in inspect.getsource(ab.index_returns)

    def test_a_row_with_no_cap_cannot_be_weighted_and_is_dropped(self, index):
        """A freshly ingested row has BARS but no market cap. Weighting it as zero would silently
        delete it from the index; keeping it needs a cap. It is excluded and COUNTED — the
        difference being that an exclusion shows up in `covered_pct` and a zero weight does not.

        (Formerly a grep for the literal `if not g or cap <= 0:`, which went red when the `not g`
        half became unreachable. The behaviour is the point, not the branch.)
        """
        fake, ab_mod = index
        for r in fake.tables["asset_grid"]:
            if r["analysis_id"] == 20:
                r["market_cap_eur"] = None      # priced, has bars, no cap yet

        out, coverage = ab_mod.members("SP500")

        assert {m["isin"] for m in out} == {"US-A"}
        assert coverage["priced"] == 1
        assert coverage["covered_pct"] == pytest.approx(100 / 3)


class TestThePortfolioAndTheBenchmarkSharePriceUniverse:
    """⚠ The portfolio's return comes from `asset_price` (yfinance). Pricing the index off
    GuruFocus would compare two price universes — different adjustment conventions, different FX
    — and call the difference alpha."""

    def test_the_analysis_prices_its_benchmark_in_the_asset_world(self):
        from routers import _airs_portfolio_analysis as pa

        src = inspect.getsource(pa)
        assert "from routers._asset_benchmark import index_returns" in src
        assert "from routers._asset_benchmark import members as _members" in src

"""Refreshing a book must also refresh the books it is BUILT FROM.

⚠ A HOLDING CAN BE ANOTHER BOOK. Some positions are Leonteq AMCs wrapping another strategy, and
everything shown through one — the looked-through holdings, their returns, the whole attribution —
is read from the WRAPPED book's own scan. Re-scanning the parent alone re-reads the twelve lines it
stores and leaves the forty instruments behind them dated to whenever those books were last
touched. Measured 2026-08-05: BUS_Offensief_Dyn is built on ONE other account,
TOPS_BEOFF_BEH_DYN on NINE.

The chain is: holding -> linked model portfolio -> the ACCOUNT paired with that model. Nothing here
decides a link; it only follows the three hops that already exist.
"""
from __future__ import annotations

import pytest

import airs_vermogen as V


@pytest.fixture
def graph(monkeypatch):
    """A shaped fleet. `PARENT` holds a certificate of model 1 (=CHILD), which holds model 2
    (=GRANDCHILD). `LOOP` holds its own certificate — the real cycle, recorded in
    `_airs_portfolio_links`: TOPS_STS_L holds the certificate of the strategy it IS."""
    links = {
        "PARENT": [{"linked_portfolio_id": 1}, {"linked_portfolio_id": None}],
        "CHILD": [{"linked_portfolio_id": 2}],
        "GRANDCHILD": [{"linked_portfolio_id": None}],
        "LOOP": [{"linked_portfolio_id": 9}],          # 9 == LOOP itself
        "A": [{"linked_portfolio_id": 4}],             # A <-> B, a two-step cycle
        "B": [{"linked_portfolio_id": 3}],
        "BROKEN": [{"linked_portfolio_id": 1}],
    }
    accounts = [{"portefeuille": "CHILD", "model_portfolio_id": 1},
                {"portefeuille": "GRANDCHILD", "model_portfolio_id": 2},
                {"portefeuille": "A", "model_portfolio_id": 3},
                {"portefeuille": "B", "model_portfolio_id": 4},
                {"portefeuille": "LOOP", "model_portfolio_id": 9},
                # ⚠ Paired with no model — it can never be a dependency, and must not crash the
                # lookup either.
                {"portefeuille": "ORPHAN", "model_portfolio_id": None}]
    monkeypatch.setattr("routers._airs_account_links.list_account_links",
                        lambda: {"accounts": accounts})
    monkeypatch.setattr("routers._airs_holding_isin.resolve_account_isins",
                        lambda p, **_k: {"rows": links[p]})
    return links


class TestItFollowsTheChain:
    def test_a_certificate_pulls_in_the_book_behind_it(self, graph):
        assert V.dependent_accounts("PARENT") == ["CHILD", "GRANDCHILD"]

    def test_it_is_transitive(self, graph):
        """A book inside a book inside a book. Stopping at depth 1 would leave the grandchild —
        whose instruments the parent actually displays — untouched."""
        assert "GRANDCHILD" in V.dependent_accounts("PARENT")

    def test_a_book_with_no_certificates_pulls_in_nothing(self, graph):
        assert V.dependent_accounts("GRANDCHILD") == []

    def test_a_holding_that_links_to_nothing_is_ignored(self, graph):
        # `linked_portfolio_id: None` is an ordinary instrument — the common case.
        assert V.dependent_accounts("CHILD") == ["GRANDCHILD"]


class TestCycles:
    """⚠ THE CYCLE IS REAL, NOT DEFENSIVE PROGRAMMING. `_airs_portfolio_links` documents it:
    TOPS_STS_L's best name match is the wrapper of the strategy it IS, and following that link
    walks back to the row you started from. Unguarded, a refresh recurses until the session dies."""

    def test_a_book_holding_its_own_certificate_does_not_recurse(self, graph):
        assert V.dependent_accounts("LOOP") == []

    def test_a_two_step_cycle_terminates_and_lists_each_book_once(self, graph):
        assert V.dependent_accounts("A") == ["B"]
        assert V.dependent_accounts("B") == ["A"]

    def test_the_target_is_never_its_own_dependency(self, graph):
        for name in ("PARENT", "CHILD", "LOOP", "A"):
            assert name not in V.dependent_accounts(name)


class TestItNeverBreaksTheRefresh:
    """Working out WHAT to refresh must not be able to stop the refresh happening."""

    def test_an_unreadable_book_is_skipped_not_raised(self, graph, monkeypatch):
        def _boom(p, **_k):
            if p == "CHILD":
                raise RuntimeError("no snapshot stored")
            return {"rows": graph[p]}

        monkeypatch.setattr("routers._airs_holding_isin.resolve_account_isins", _boom)
        # CHILD is still named (the parent's own rows named it); we simply cannot see THROUGH it,
        # so GRANDCHILD is lost. Reported in the log, never raised.
        assert V.dependent_accounts("PARENT") == ["CHILD"]

    def test_an_unreadable_account_map_yields_no_dependencies(self, graph, monkeypatch):
        def _boom():
            raise RuntimeError("links unavailable")

        monkeypatch.setattr("routers._airs_account_links.list_account_links", _boom)
        assert V.dependent_accounts("PARENT") == []

    def test_it_does_not_freshen(self, graph):
        """⚠ `freshen=False`. Deciding what to scrape must not itself scrape — that would put a
        download in front of every download, on the very session the refresh is about to use."""
        import inspect

        assert "freshen=False" in inspect.getsource(V.dependent_accounts)

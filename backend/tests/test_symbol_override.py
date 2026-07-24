"""Manual symbol overrides: the Yahoo listing an ISIN MUST resolve to.

THE MEASURED CASE (2026-07-24). `IE00BJSFQW37` — iShares Global Corp Bond UCITS ETF, EUR-hedged
Dist — was resolved onto `IS0X.DE`, the USD UNHEDGED Dist share class. The names differ by "EUR"
vs "USD (Dist)", which a name-anchored resolver accepts; OpenFIGI lists 36 venues for that ISIN
and `IS0X` is not one of them. Measured: AIRS implied EUR 4.1523/unit, `IS0X.DE` closed at
EUR 77.55, the correct `36B7.DE` at EUR 4.1523. Held in 5 model portfolios, up to 30% weight.

⚠ AND THE WRONG LISTING WAS THE MORE LIQUID ONE — EUR 222k/day against 36B7's EUR 110k. Every
automatic repointer ranks by liquidity, so none of them can ever choose correctly here. Only a
human can, which is precisely why the answer needs somewhere durable to live.
"""
from __future__ import annotations

import pytest

from asset_pipeline import symbol_override
from tests._fake_supabase import FakeSupabase

ISIN, RIGHT, WRONG = "IE00BJSFQW37", "36B7.DE", "IS0X.DE"


def _tables(override=True, symbol=RIGHT, current=WRONG, alias=False):
    return {
        "asset_symbol_override": ([{"isin": ISIN, "yahoo_symbol": symbol,
                                    "note": "EUR-hedged Dist; IS0X.DE is the USD class"}]
                                  if override else []),
        "asset_isin_alias": ([{"isin": ISIN, "canonical_isin": "IE00B7J7TB45"}]
                             if alias else []),
        "asset_execution": [
            {"execution_id": 41539, "isin": ISIN, "analysis_id": 8657,
             "yahoo_symbol": current, "name": "iShares Global Corp Bond UCITS ETF",
             "exchange": "XETRA", "currency": "EUR", "status": "ok"},
        ],
        "asset_grid": [{"isin": ISIN, "asset_class": "etf"}],
    }


def _patch(monkeypatch, **kw):
    fake = FakeSupabase(_tables(**kw))
    monkeypatch.setattr(symbol_override, "supabase", fake)
    # `isin_alias` is consulted for the both-claim-one-ISIN guard and reads its own handle.
    from asset_pipeline import isin_alias
    monkeypatch.setattr(isin_alias, "supabase", fake)
    return fake


class TestItIsIdempotent:
    """The property that makes it safe to run after EVERY resolution."""

    def test_a_row_already_naming_the_pinned_symbol_is_left_alone(self, monkeypatch):
        _patch(monkeypatch, current=RIGHT)
        called = []
        monkeypatch.setattr(symbol_override, "_repoint",
                            lambda *a: called.append(a) or True)
        assert symbol_override.apply_symbol_overrides() == 0
        assert called == [], "a correct row must not be repointed again"

    def test_and_it_costs_no_yahoo_call_to_decide_that(self, monkeypatch):
        """⚠ THE ORDER MATTERS. This runs after every resolution slice; probing Yahoo to discover
        that nothing changed would add a call per override to every tick, and Yahoo answers an
        overloaded caller with an EMPTY result rather than a 429 — which is how a resolution lands
        on a thin foreign listing. The stored row is compared FIRST."""
        _patch(monkeypatch, current=RIGHT)

        def _boom(*_a, **_k):
            raise AssertionError("probed the network before checking the stored row")

        monkeypatch.setattr(symbol_override, "_repoint", _boom)
        assert symbol_override.apply_symbol_overrides() == 0


class TestItRepointsADriftedRow:
    def test_a_row_naming_the_wrong_symbol_is_repointed(self, monkeypatch):
        _patch(monkeypatch)
        seen = []
        monkeypatch.setattr(symbol_override, "_repoint",
                            lambda i, s: seen.append((i, s)) or True)
        assert symbol_override.apply_symbol_overrides() == 1
        assert seen == [(ISIN, RIGHT)]

    def test_one_bad_override_does_not_stop_the_others(self, monkeypatch):
        """A fleet-wide re-assert that aborts on the first failure leaves every later override
        un-applied, silently — the failure mode is indistinguishable from having none."""
        fake = _patch(monkeypatch)
        fake.tables["asset_symbol_override"].append(
            {"isin": "IE00B4L5Y983", "yahoo_symbol": "SWDA.L"})
        fake.tables["asset_execution"].append(
            {"execution_id": 2, "isin": "IE00B4L5Y983", "yahoo_symbol": "OTHER.L"})
        fake.tables["asset_grid"].append({"isin": "IE00B4L5Y983", "asset_class": "etf"})

        def _flaky(isin, _sym):
            if isin == ISIN:
                raise RuntimeError("Yahoo throttled")
            return True

        monkeypatch.setattr(symbol_override, "_repoint", _flaky)
        assert symbol_override.apply_symbol_overrides() == 1   # the other one still landed


class TestItRefusesToFightTheAliasTable:
    """⚠ TWO OVERRIDES CANNOT BOTH OWN ONE ISIN.

    An alias points the row at ANOTHER ISIN's instrument; a symbol override gives it its OWN. If
    both claim an ISIN they overwrite each other on every pass, and which one survives depends on
    call order rather than on anyone's intent — a row that silently changes meaning between two
    runs of the same pipeline.
    """

    def test_an_aliased_isin_is_refused_and_named(self, monkeypatch, caplog):
        _patch(monkeypatch, alias=True)
        monkeypatch.setattr(symbol_override, "_repoint",
                            lambda *_a: pytest.fail("applied an override on an aliased ISIN"))
        with caplog.at_level("ERROR"):
            assert symbol_override.apply_symbol_overrides() == 0
        assert ISIN in caplog.text, "a refusal nobody can see is a silent drop"


class TestScoping:
    def test_only_isin_applies_just_that_one(self, monkeypatch):
        fake = _patch(monkeypatch)
        fake.tables["asset_symbol_override"].append(
            {"isin": "IE00B4L5Y983", "yahoo_symbol": "SWDA.L"})
        fake.tables["asset_execution"].append(
            {"execution_id": 2, "isin": "IE00B4L5Y983", "yahoo_symbol": "OTHER.L"})
        seen = []
        monkeypatch.setattr(symbol_override, "_repoint",
                            lambda i, s: seen.append(i) or True)
        symbol_override.apply_symbol_overrides(only_isin=ISIN)
        assert seen == [ISIN]

    def test_an_isin_with_no_execution_row_is_skipped_not_crashed(self, monkeypatch):
        fake = _patch(monkeypatch)
        fake.tables["asset_execution"].clear()
        monkeypatch.setattr(symbol_override, "_repoint",
                            lambda *_a: pytest.fail("repointed a row that does not exist"))
        assert symbol_override.apply_symbol_overrides() == 0


class TestItIsWiredIntoTheResolutionPaths:
    """An override that is not re-asserted after a resolution is not an override — it is a value
    the next by-name resolve discards. `asset_isin_alias`'s docstring says exactly this, and its
    own wiring is the precedent."""

    def test_the_queue_worker_reapplies_it_after_a_slice(self):
        import inspect

        from asset_pipeline import queue

        src = inspect.getsource(queue.process_slice)
        assert "_reapply_symbol_overrides" in src
        assert "apply_symbol_overrides" in inspect.getsource(queue._reapply_symbol_overrides)
        # ...and after the resolutions, not before them — order is the whole mechanism.
        assert src.index("ThreadPoolExecutor") < src.index("_reapply_symbol_overrides")

    def test_the_hand_repointer_reapplies_it_too(self):
        src = (__import__("pathlib").Path(__file__).resolve().parent.parent
               / "scripts" / "repoint_to_symbol.py").read_text(encoding="utf-8")
        assert "apply_symbol_overrides()" in src
        # ⚠ And it must TELL the operator the repoint is otherwise unrecorded, or the durable
        # store stays empty precisely because the manual path appears to have worked.
        assert "asset_symbol_override" in src

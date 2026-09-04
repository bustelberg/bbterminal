"""THE NAME-MISMATCH SWEEP DOES NOT RE-RESOLVE ANYTHING UNLESS A PERSON NAMES IT.

⚠⚠ IT USED TO RE-QUEUE EVERY ROW THAT FAILED `same_company`, AND THAT IS THE DESTRUCTIVE
RE-RESOLVE THE WHOLE ASSET PIPELINE IS BUILT AROUND AVOIDING. Measured 2026-09-04 on the live
grid: 110 rows fail the test and only ~15 are genuinely the wrong company. The other ~95 are
OpenFIGI's own spelling of a CORRECT mapping — `MUENCHENER RUECKVER AG-REG` for Münchener
Rückversicherungs-Gesellschaft, `IND & COMM BK OF CHINA-H` for ICBC, `SAMSUNG ELECTRO-REGS GDR
PFD`, `DHL GROUP` for Deutsche Post (a rename), `VANG FTSE JPN USDA` for a Vanguard ETF. Yahoo
answers an overloaded caller with an EMPTY list rather than a 429, so re-resolving a row that was
already right can only move it to a thinner listing (Alphabet -> GOOA.VI, 75,000x thinner). The
old default made that bet 95 times to fix 15.

⚠ AND NO AUTOMATIC RULE REPLACES THE PERSON — three were scored against 15 hand-checked errors:
the OpenFIGI-type allowlist catches all 15 and would re-resolve 38 correct rows; type AND a
country mismatch leaves 11 false positives and misses 3 real ones; "a bare US ticker for a non-US
ISIN" is structural and clean but catches only 4 of 15. Hence: list by default, act on names.
"""
from __future__ import annotations

import pytest

from tests._fake_supabase import FakeSupabase


def _row(isin: str, name: str, figi: str, identity: str = "mismatch", adv: float = 1.0) -> dict:
    return {"isin": isin, "name": name, "openfigi_name": figi, "openfigi_type": "Common Stock",
            "yahoo_symbol": isin[:4], "med_adv_eur": adv, "status": "ok",
            "identity_status": identity}


@pytest.fixture()
def q(monkeypatch):
    """`asset_pipeline.queue` bound to an in-memory database."""
    from asset_pipeline import queue as q_mod

    fake = FakeSupabase()
    fake.tables["asset_execution"] = [
        # genuinely wrong — a French ISIN priced off Akamai
        _row("FR0004180537", "Akamai Technologies, Inc.", "AKKA TECHNOLOGIES", adv=4.8e8),
        # correct, and the name test hates it anyway
        _row("DE0008430026", "Münchener Rückversicherungs-Gesellschaft", "MUENCHENER RUECKVER AG",
             adv=1.4e8),
        # correct AND stamped verified — must never be swept, whatever the names look like
        _row("US0378331005", "Apple Inc.", "APPLE INC LOOKS DIFFERENT", identity="verified"),
    ]
    fake.tables["asset_ingest_queue"] = []
    monkeypatch.setattr(q_mod, "supabase", fake)
    return q_mod, fake


class TestItListsRatherThanActs:
    def test_the_default_queues_nothing(self, q):
        q_mod, fake = q
        res = q_mod.requeue_suspects()
        assert res["applied"] is False
        assert res["queued"] == 0
        assert fake.tables["asset_ingest_queue"] == []

    def test_and_hands_back_the_rows_so_the_caller_can_print_them(self, q):
        q_mod, _fake = q
        res = q_mod.requeue_suspects()
        assert res["suspects"] == 2
        assert {r["isin"] for r in res["rows"]} == {"FR0004180537", "DE0008430026"}


class TestItActsOnNamedRows:
    def test_only_queues_exactly_what_was_named(self, q):
        q_mod, fake = q
        res = q_mod.requeue_suspects(only=["FR0004180537"])
        assert res["applied"] is True
        assert [r["isin"] for r in fake.tables["asset_ingest_queue"]] == ["FR0004180537"]

    def test_an_isin_that_is_not_flagged_is_reported_not_queued(self, q):
        """⚠ A TYPO MUST NOT SILENTLY RE-RESOLVE NOTHING AND SAY IT WORKED — nor should naming a
        healthy row be a way to re-resolve it through this door."""
        q_mod, fake = q
        res = q_mod.requeue_suspects(only=["US0378331005", "XX0000000000"])
        assert fake.tables["asset_ingest_queue"] == []
        assert res["unknown"] == ["US0378331005", "XX0000000000"]

    def test_the_blanket_sweep_still_exists_but_has_to_be_asked_for(self, q):
        q_mod, fake = q
        res = q_mod.requeue_suspects(apply=True)
        assert res["applied"] is True
        assert {r["isin"] for r in fake.tables["asset_ingest_queue"]} == {
            "FR0004180537", "DE0008430026"}


class TestTheVerdictIsReadNotReDerived:
    def test_a_verified_row_is_never_a_suspect_however_its_names_read(self, q):
        """⚠⚠ THE DETECTOR EXISTED TWICE. This function re-ran `same_company` inline while
        `resolve.identity_status` stamped the row at resolve time, so the sweep and the grid's
        Match badge were free to disagree — two answers to one question, and the sweep's was the
        one that re-resolved things. Apple's stored name here would fail any name test; the stored
        verdict says verified, and that is what governs."""
        q_mod, _fake = q
        res = q_mod.requeue_suspects()
        assert "US0378331005" not in {r["isin"] for r in res["rows"]}

    def test_a_row_that_is_not_ok_is_not_swept(self, q):
        """An unmapped row has nothing to re-resolve away from."""
        q_mod, fake = q
        fake.tables["asset_execution"].append(
            {**_row("NL0000000000", "Something", "SOMETHING ELSE"), "status": "not_found"})
        res = q_mod.requeue_suspects()
        assert "NL0000000000" not in {r["isin"] for r in res["rows"]}


class TestADeliberateUnmapSurvivesTheRetrySweep:
    """⚠⚠ THE OTHER DOOR, AND THE ONE THAT WOULD HAVE UNDONE THE FIX QUIETLY.

    `requeue_unmapped()` re-queues every `not_found` row OpenFIGI identified, because almost all of
    them failed only while Yahoo was throttled. That premise is false for a row a person unmapped
    BECAUSE the resolver had it on a different instrument: re-queueing runs the same resolver over
    the same candidates and can restore the same wrong ticker. First Abu Dhabi Bank is the measured
    case — OpenFIGI offers only Bloomberg composite codes, Yahoo has no Abu Dhabi coverage, and the
    bare ticker `FAB` is a US First Trust ETF carrying 4,822 bars.
    """

    @pytest.fixture()
    def unmapped(self, monkeypatch):
        from asset_pipeline import queue as q_mod

        fake = FakeSupabase()
        fake.tables["asset_execution"] = [
            {"isin": "AEN000101016", "openfigi_figi": "BBG000DHGTK4",
             "openfigi_type": "Common Stock", "status": "not_found",
             "reason": "unmapped by hand: the stored listing is a different instrument"},
            {"isin": "US1111111111", "openfigi_figi": "BBG000000001",
             "openfigi_type": "Common Stock", "status": "not_found",
             "reason": "fast path: no Yahoo symbol candidate"},
        ]
        fake.tables["asset_ingest_queue"] = []
        monkeypatch.setattr(q_mod, "supabase", fake)
        return q_mod, fake

    def test_the_throttle_casualty_is_retried(self, unmapped):
        q_mod, fake = unmapped
        q_mod.requeue_unmapped()
        assert "US1111111111" in {r["isin"] for r in fake.tables["asset_ingest_queue"]}

    def test_and_the_deliberate_unmap_is_left_alone(self, unmapped):
        q_mod, fake = unmapped
        res = q_mod.requeue_unmapped()
        assert "AEN000101016" not in {r["isin"] for r in fake.tables["asset_ingest_queue"]}
        assert res["retryable"] == 1

    def test_the_marker_is_one_declaration_both_sides_read(self):
        """⚠ THE SCRIPT WRITES IT AND THE QUEUE READS IT — a second copy of the string is a fix
        that stops working the day somebody rewords one of them."""
        import importlib.util
        from pathlib import Path

        from asset_pipeline.store import MANUAL_UNMAP_PREFIX

        path = Path(__file__).resolve().parent.parent / "scripts" / "unmap_asset_row.py"
        src = path.read_text(encoding="utf-8")
        assert "MANUAL_UNMAP_PREFIX" in src
        assert f'"{MANUAL_UNMAP_PREFIX}"' not in src.split("MANUAL_UNMAP_PREFIX", 1)[1]
        assert importlib.util.find_spec("asset_pipeline.store") is not None

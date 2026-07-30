"""The AIRS scrape decides which accounts exist — not `airs_performance`.

MEASURED 2026-07-24. AIRS's Front-Office list returned 44 portfolios; our table showed 50. The
six extras (TOPS_AZTS_L, TOPS_MOTS_L, WTS test 1-4 DYN) had simply stopped being scraped, and
nothing in the data said so: `airs_performance` is append-only, so an account AIRS deactivated
keeps every row it ever wrote and stays in the list for ever.

⚠ AND IT DOES NOT LOOK LIKE A STALE ROW — IT LOOKS LIKE A BROKEN FEATURE. TOPS_NEU_BEH_DYN's
holdings were frozen at its last scan (2026-07-16, before ISIN capture), so its Class column was
read-only and 9 of its 12 rows sat Unclassified. Three separate "bugs", one dead account.
"""
from __future__ import annotations

from routers import _airs_accounts
from tests._fake_supabase import FakeSupabase

OLD, NEW = "2026-07-16T08:00:00+00:00", "2026-07-24T08:00:00+00:00"


def _patch(monkeypatch, roster, hidden=()):
    fake = FakeSupabase({
        "airs_account_roster": list(roster),
        "airs_account_hidden": [{"portefeuille": h} for h in hidden],
    })
    monkeypatch.setattr(_airs_accounts, "supabase", fake)
    return fake


class TestTheLiveSetIsTheNEWESTDiscovery:
    """Not every account ever seen — the one pass that ran most recently."""

    def test_only_the_newest_batch_counts(self, monkeypatch):
        _patch(monkeypatch, [
            {"portefeuille": "LIVE_A", "last_seen_at": NEW},
            {"portefeuille": "LIVE_B", "last_seen_at": NEW},
            # Seen once, then never again — AIRS stopped listing it.
            {"portefeuille": "RETIRED", "last_seen_at": OLD},
        ])
        assert _airs_accounts._live_accounts() == {"live_a", "live_b"}

    def test_it_is_case_and_padding_insensitive(self, monkeypatch):
        _patch(monkeypatch, [{"portefeuille": "  Tops_Def_Beh_Dyn  ", "last_seen_at": NEW}])
        assert _airs_accounts._live_accounts() == {"tops_def_beh_dyn"}


class TestAnUnknownRosterMustNotBlankThePage:
    """⚠ `None` MEANS "DO NOT FILTER" AND IS NOT AN EMPTY SET.

    Before the first discovery — a fresh database, or the moment this table was added — the
    roster is empty. Reading that as "no account exists" empties the portfolios page completely,
    which is a far worse failure than showing six rows too many: it looks like total data loss.
    """

    def test_an_empty_roster_does_not_filter(self, monkeypatch):
        _patch(monkeypatch, [])
        assert _airs_accounts._live_accounts() is None

    def test_a_missing_table_does_not_filter(self, monkeypatch):
        class _Broken(FakeSupabase):
            def table(self, name):
                if name == "airs_account_roster":
                    raise RuntimeError('relation "airs_account_roster" does not exist')
                return super().table(name)

        monkeypatch.setattr(_airs_accounts, "supabase", _Broken({}))
        assert _airs_accounts._live_accounts() is None


class TestAFailedScrapeMustNotRetireEveryAccount:
    """⚠ A LOGIN FAILURE RETURNS FEW ROWS, NOT AN ERROR.

    A changed selector or an expired session yields a handful of rows and no exception. Writing
    that as the roster would retire the entire table in one pass — and the next page load would
    show almost nothing, with the data itself intact and no error anywhere to explain it.
    """

    def test_a_short_discovery_is_refused(self, monkeypatch, caplog):
        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": []})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)
        with caplog.at_level("WARNING"):
            airs_vermogen._record_roster(["ONLY_ONE", "AND_TWO"])
        assert fake.tables["airs_account_roster"] == [], "a failed scrape must not become the roster"
        assert "roster NOT updated" in caplog.text, "a silent refusal is indistinguishable from a write"

    def test_a_full_discovery_is_recorded_with_ONE_stamp(self, monkeypatch):
        """One timestamp for the batch, or `last_seen_at = max(...)` becomes a race."""
        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": []})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)
        airs_vermogen._record_roster([f"ACC_{i}" for i in range(44)])
        rows = fake.tables["airs_account_roster"]
        assert len(rows) == 44
        assert len({r["last_seen_at"] for r in rows}) == 1


class TestTheTwoFiltersAreIndependent:
    def test_hidden_removes_a_LIVE_account(self, monkeypatch):
        """`airs_account_hidden` is editorial — for an account AIRS does list but you do not want
        shown. The roster answers a different question and neither substitutes for the other."""
        _patch(monkeypatch,
               [{"portefeuille": "LIVE_A", "last_seen_at": NEW},
                {"portefeuille": "LIVE_B", "last_seen_at": NEW}],
               hidden=["LIVE_B"])
        assert _airs_accounts._live_accounts() == {"live_a", "live_b"}
        assert _airs_accounts._hidden_accounts() == {"live_b"}


class TestRecordingReportsMustNotRedefineTheLiveSet:
    """⚠ ROWS VANISHED FROM THE PORTFOLIOS PAGE MID-SCAN, AND THIS IS WHY.

    `_live_accounts` means "the accounts AIRS listed on the most recent discovery", computed as
    `last_seen_at == max(last_seen_at)`. `_record_reports` runs PER ACCOUNT as the scan progresses,
    and it used to stamp `last_seen_at` too — which silently re-defined the live set to mean "the
    accounts scanned so far". Measured 2026-07-30: the table filled with all 44 and then collapsed
    to the single book that had just been scanned.

    It outlived the run, too. The scan is INCREMENTAL: a pass that scanned 14 and skipped 30 left
    only those 14 carrying the newest stamp, so 30 healthy books were filtered off their own page
    until the next discovery re-stamped them.

    "AIRS listed this account" and "we scanned this account" are facts about different sets, and
    the incremental scan is exactly what makes them differ.
    """

    def test_it_writes_reports_at_and_not_last_seen_at(self):
        import inspect

        import airs_vermogen

        src = inspect.getsource(airs_vermogen._record_reports)
        rows = src.split("rows = [")[1].split("]")[0]
        assert "reports_at" in rows
        assert "reports_ok" in rows
        assert "last_seen_at" not in rows, "discovery owns last_seen_at — see the docstring"

    def test_discovery_still_owns_last_seen_at(self):
        """The other half of the invariant: something must still stamp it, or nothing is ever live."""
        import inspect

        import airs_vermogen

        assert "last_seen_at" in inspect.getsource(airs_vermogen._record_roster)

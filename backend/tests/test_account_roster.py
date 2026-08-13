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

    def test_it_writes_reports_at_and_not_last_seen_at(self, monkeypatch):
        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": [
            {"portefeuille": "LIVE_A", "last_seen_at": NEW},
        ]})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)

        airs_vermogen._record_reports({"LIVE_A": ["att", "volk"]}, NEW)

        # ⚠ AN `update`, NOT AN `insert` — the write stopped being an upsert on 2026-08-13 because
        # an upsert omitting the NOT NULL `last_seen_at` fails whether or not the row exists. See
        # `TestTheWriteIsAnUPDATEAndNeverAnUpsert`.
        written = [w for w in fake.writes if w[0] == "update"]
        assert written, "the outcome was not recorded at all"
        assert "last_seen_at" not in written[-1][2], "discovery owns last_seen_at — see the docstring"

        row = next(r for r in fake.tables["airs_account_roster"] if r["portefeuille"] == "LIVE_A")
        assert row["reports_ok"] == ["att", "volk"]
        assert row["reports_at"] == NEW
        # ⚠ AND THE EXISTING VALUE SURVIVES UNTOUCHED. Under the old upsert the row was replaced,
        # so "absent" was the only way to express "not written"; an UPDATE leaves the discovery
        # stamp in place, which is the stronger and more literal form of the same rule.
        assert row["last_seen_at"] == NEW

    def test_discovery_still_owns_last_seen_at(self):
        """The other half of the invariant: something must still stamp it, or nothing is ever live."""
        import inspect

        import airs_vermogen

        assert "last_seen_at" in inspect.getsource(airs_vermogen._record_roster)


class TestTheWriteIsAnUPDATEAndNeverAnUpsert:
    """⚠⚠ THE 2026-08-03 DIAGNOSIS WAS WRONG AND THESE TESTS PASSED ANYWAY (corrected 2026-08-13).

    The symptom was real:

        null value in column "last_seen_at" of relation "airs_account_roster" violates not-null
        constraint — Failing row contains (AITopSelectie OFF DYN, null, ...)

    and the cause was believed to be "an account discovery has never seen took the INSERT branch",
    fixed by filtering to rows already in the roster. It did not work. Postgres forms and VALIDATES
    the candidate tuple BEFORE it arbitrates the conflict, so an upsert omitting a NOT NULL column
    with no default fails **whether or not the row exists**. Measured directly against the database
    on 2026-08-13:

        select count(*) filter (where portefeuille='BUS_WTS_Dividend_Dyn')  ->  1   (it exists)
        insert ... on conflict (portefeuille) do update ...                 ->  23502

    ⚠ AND EVERY TEST BELOW WENT ON PASSING, WHICH IS THE LESSON. `FakeSupabase` has no NOT NULL
    constraint, so it cannot reproduce this class of failure at all — the behavioural tests were
    asserting on a store that accepts anything. The only check that would have caught it is the
    structural one: this function must not call `upsert`.

    ⚠ THE DAMAGE WAS NOT THE MISSING ROW. Every account failed, on every scan, silently — and this
    table is what marks a row "att did not arrive", so the failure suppressed exactly the warning it
    should have raised. `AITopSelectie OFF DYN` went on showing +55.20% (June's
    `cumulatief_rendement`) while July's −11.96% sat unfetched, and the row looked perfectly
    healthy.
    """

    def test_it_never_calls_upsert(self):
        """⚠ STRUCTURAL, BECAUSE THE FAKE CANNOT ENFORCE `NOT NULL`. `upsert` here is an INSERT
        whose tuple omits `last_seen_at`, and that is rejected before the ON CONFLICT clause is
        ever consulted. The only safe write is a plain UPDATE."""
        import inspect

        import airs_vermogen

        src = inspect.getsource(airs_vermogen._record_reports)
        assert ".upsert(" not in src, (
            "an upsert omitting last_seen_at fails 23502 even when the row exists — use update()")
        assert ".update(" in src

    def test_it_never_writes_last_seen_at(self):
        """The other half of the rule: the fix must not be "just include last_seen_at". That field
        defines the live set, and stamping it here re-defines it as "the accounts scanned so far"
        — the bug this class's first test exists for."""
        import inspect

        import airs_vermogen

        body = inspect.getsource(airs_vermogen._record_reports)
        writes = body[body.index("by_outcome"):]
        assert "last_seen_at" not in writes

    def test_an_unknown_account_does_not_take_the_batch_down_with_it(self, monkeypatch):
        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": [
            {"portefeuille": "KNOWN_A", "last_seen_at": NEW},
            {"portefeuille": "KNOWN_B", "last_seen_at": NEW},
        ]})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)

        airs_vermogen._record_reports(
            {"KNOWN_A": ["att"], "UNSEEN": ["att"], "KNOWN_B": ["att", "volk"]}, NEW)

        recorded = {r["portefeuille"] for r in fake.tables["airs_account_roster"]
                    if r.get("reports_at")}
        assert recorded == {"KNOWN_A", "KNOWN_B"}, (
            "the two known accounts must still get their outcomes — one unseen account used to "
            "fail the whole batch")

    def test_it_never_creates_a_roster_row(self, monkeypatch):
        """Existence is discovery's fact to state. Inserting here would also make the new row the
        newest `last_seen_at` and collapse the live set to it."""
        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": []})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)

        airs_vermogen._record_reports({"UNSEEN": ["att"]}, NEW)

        assert fake.tables["airs_account_roster"] == []

    def test_it_names_what_it_skipped(self, monkeypatch, caplog):
        """A silent skip is how the missing report goes unnoticed a second time. The operator has
        to be told which accounts, and that only a full discovery can fix it."""
        import logging

        import airs_vermogen

        fake = FakeSupabase({"airs_account_roster": []})
        monkeypatch.setattr(airs_vermogen, "supabase", fake)

        with caplog.at_level(logging.WARNING):
            airs_vermogen._record_reports({"AITopSelectie OFF DYN": ["att"]}, NEW)

        assert "AITopSelectie OFF DYN" in caplog.text
        assert "discovery" in caplog.text.lower()

    def test_a_roster_read_failure_records_nothing_rather_than_guessing(self, monkeypatch):
        """If we cannot tell which accounts exist, writing anyway is how the INSERT happens."""
        import airs_vermogen

        class _Boom:
            def table(self, _n):
                raise RuntimeError("roster unreadable")

        monkeypatch.setattr(airs_vermogen, "supabase", _Boom())
        airs_vermogen._record_reports({"KNOWN_A": ["att"]}, NEW)   # must not raise

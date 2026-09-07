"""`common.sequences.is_sequence_drift` — the test that decides whether an insert is retried.

⚠ THE ONLY PART OF THIS MODULE THAT CAN BE UNIT-TESTED IS THE ONE THAT MATTERS. `repair_sequence`
needs a live Postgres and `insert_repairing_sequence` needs both that and PostgREST, so neither is
touched here (unit tests only — see CLAUDE.md). What is pinned is the predicate: get it too WIDE
and a real uniqueness conflict is answered by moving a sequence and re-issuing the write; get it
too NARROW and the repair silently never fires, which is indistinguishable from not having built
it — and is exactly what a version matching only `str(e)` would do to a supabase-py `APIError`.
"""
from __future__ import annotations

from common.sequences import is_sequence_drift


class _APIError(Exception):
    """The shape supabase-py raises: a dict in `args[0]`, no useful `str()`.

    ⚠ This is the shape that makes a naive `"23505" in str(exc)` test FAIL. Reproduced here
    rather than imported so the pin does not move if the client library changes its repr.
    """

    def __init__(self, payload: dict):
        super().__init__(payload)
        self.code = payload.get("code")
        self.message = payload.get("message")
        self.details = payload.get("details")

    def __str__(self) -> str:                      # deliberately uninformative
        return "APIError"


def _api_error(constraint: str, code: str = "23505") -> _APIError:
    return _APIError({
        "message": f'duplicate key value violates unique constraint "{constraint}"',
        "code": code,
        "hint": None,
        "details": "Key (snapshot_id)=(3408) already exists.",
    })


class TestTheRealProductionError:
    def test_the_2026_09_07_snapshot_failure_is_recognised(self):
        exc = _api_error("current_picks_snapshot_pkey")
        assert is_sequence_drift(exc, "current_picks_snapshot") is True

    def test_the_2026_09_07_ingest_run_failure_is_recognised(self):
        exc = _api_error("ingest_run_pkey")
        assert is_sequence_drift(exc, "ingest_run") is True

    def test_a_psycopg_style_error_carrying_it_all_in_str_is_recognised(self):
        """The other transport: everything in the message, code on `sqlstate`."""
        class _PgError(Exception):
            sqlstate = "23505"

        exc = _PgError(
            'duplicate key value violates unique constraint "ingest_run_pkey"\n'
            "DETAIL:  Key (run_id)=(2899) already exists."
        )
        assert is_sequence_drift(exc, "ingest_run") is True


class TestWhatItMustRefuse:
    def test_a_business_unique_constraint_is_NOT_a_drifted_sequence(self):
        """⚠ THE IMPORTANT NEGATIVE. `universe.template_key` is unique on purpose; repairing a
        sequence and re-issuing that insert would either fail again or write the row the rule
        exists to refuse."""
        exc = _api_error("universe_template_key_key")
        assert is_sequence_drift(exc, "universe") is False

    def test_another_tables_pkey_is_not_this_tables_problem(self):
        exc = _api_error("ingest_run_pkey")
        assert is_sequence_drift(exc, "current_picks_snapshot") is False

    def test_a_foreign_key_violation_is_not_a_drifted_sequence(self):
        exc = _api_error("current_picks_snapshot_pkey", code="23503")
        assert is_sequence_drift(exc, "current_picks_snapshot") is False

    def test_an_unrelated_exception_is_left_alone(self):
        assert is_sequence_drift(RuntimeError("boom"), "ingest_run") is False

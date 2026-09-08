"""The hardcoded-admin allowlist lives in THREE places, and nothing kept them together.

`routers/auth.py::_ADMIN_EMAIL_HASHES` decides what the API answers; the trigger function decides
what `app_metadata.role` a new signup gets; the backfill decides what existing rows get. The
backend and the database each look correct in isolation while disagreeing — and the disagreement
is invisible in the worst possible direction: an account the API serves as ADMIN while every
screen renders it as a regular user, because the frontend reads `app_metadata.role` verbatim.

⚠⚠ AND THE TRIGGER ITSELF WAS MISSING FOR MONTHS (2026-09-08). Two migrations create the
FUNCTION `public.set_admin_role_on_signup()`; neither ever wrote `CREATE TRIGGER`, so nothing
called it. It looked healthy because 20260527010000 ends with a one-shot backfill UPDATE, which
fixed the rows existing at the time — a backfill is not a rule, and a re-signup or a fresh
environment got `role = NULL`.

⚠ THESE ASSERTIONS READ SOURCE TEXT, WHICH THIS REPO OTHERWISE REFUSES TO DO. The rule exists
because a test over source can only confirm that one CALLER does the right thing, which is the
shape of the bug it is supposed to catch. It is right here for the opposite reason: the artifact
under test IS text. A migration is a file that will be replayed against a database we are banned
from booting in a test, so the only thing there is to check is what the file says.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from routers.auth import _ADMIN_EMAIL_HASHES, _is_hardcoded_admin_email, _resolve_role

MIGRATIONS = Path(__file__).resolve().parents[2] / "supabase" / "migrations"
_HASH_RE = re.compile(r"'([0-9a-f]{64})'")


def _latest_function_sql() -> str:
    """The NEWEST migration that defines `set_admin_role_on_signup()`.

    ⚠⚠ NEWEST, NOT ALL OF THEM. Earlier migrations still carry the OLD hash list and must not be
    edited — an applied migration records what a database already DID, and rewriting it makes the
    file and the history stop describing each other. The only question this test can honestly ask
    is whether the definition IN FORCE matches the backend, and that is the last one to run.
    """
    hits = sorted(
        p for p in MIGRATIONS.glob("*.sql")
        if "FUNCTION public.set_admin_role_on_signup" in p.read_text(encoding="utf-8")
    )
    assert hits, "no migration defines set_admin_role_on_signup()"
    return hits[-1].read_text(encoding="utf-8")


class TestTheAllowlistIsTheSameEverywhere:
    def test_the_live_trigger_function_carries_exactly_the_backend_hashes(self):
        """⚠ The DEFINITION IN FORCE, i.e. the newest one. A hash the backend honours but the
        trigger does not means a signup that the API treats as admin and the frontend renders as a
        user — the desync `_resolve_role` exists to warn about."""
        sql = _latest_function_sql()
        body = sql[sql.index("admin_hashes"):sql.index("BEGIN")]
        assert set(_HASH_RE.findall(body)) == set(_ADMIN_EMAIL_HASHES)

    def test_there_is_exactly_one_hardcoded_admin(self):
        """⚠⚠ NARROWED FROM TWO TO ONE (2026-09-08, on request). Pinned as a COUNT because the
        second address was not merely dropped — with one admin there is no longer a second account
        able to reset the first's authenticator, so re-adding one is a decision about 2FA recovery
        and not a tidy-up."""
        assert len(_ADMIN_EMAIL_HASHES) == 1


class TestTheTriggerIsActuallyAttached:
    """⚠ THE REGRESSION THIS FILE EXISTS FOR. `db diff` and `db dump` do not cover the `auth`
    schema, so a trigger created by hand in the Supabase dashboard leaves no trace in this
    directory and a local `db reset` silently omits it. If it is not written down in a migration,
    it does not exist in every environment."""

    def test_some_migration_binds_the_function_to_auth_users(self):
        sql = "\n".join(p.read_text(encoding="utf-8") for p in MIGRATIONS.glob("*.sql"))
        # Whitespace-tolerant: the point is that a CREATE TRIGGER on auth.users runs this function,
        # not how the statement happens to be wrapped.
        pattern = re.compile(
            r"CREATE\s+TRIGGER\s+\w+\s+BEFORE\s+INSERT\s+ON\s+auth\.users\s+"
            r"FOR\s+EACH\s+ROW\s+EXECUTE\s+FUNCTION\s+public\.set_admin_role_on_signup\(\)",
            re.IGNORECASE,
        )
        assert pattern.search(sql), "nothing calls set_admin_role_on_signup() on signup"


class TestWhoIsAnAdmin:
    def test_the_hardcoded_address_matches(self):
        assert _is_hardcoded_admin_email("reinier7175@gmail.com")

    @pytest.mark.parametrize("email", ["reinier@bustelberg.nl"])
    def test_the_demoted_address_no_longer_matches(self, email):
        """⚠⚠ REMOVED FROM THE ALLOWLIST 2026-09-08, on request — and removing it was only half
        the job. The account carried an EXPLICIT `role: admin` from an earlier backfill, and
        `_resolve_role` prefers an explicit role to this list on purpose, so the row had to be set
        to 'user' as well (migration 20260908150000). Pinned in BOTH directions because the two
        halves fail differently: this one alone leaves the account admin for ever."""
        assert not _is_hardcoded_admin_email(email)
        assert _resolve_role(None, email) == "user"

    def test_matching_ignores_case_and_padding(self):
        """The trigger lowercases before hashing (`lower(NEW.email)`); the backend must agree, or
        an address typed with a capital is an admin in one half of the app and not the other."""
        assert _is_hardcoded_admin_email("  REINIER7175@Gmail.COM  ")

    def test_everyone_else_is_a_plain_user(self):
        assert not _is_hardcoded_admin_email("someone.else@bustelberg.nl")
        assert not _is_hardcoded_admin_email(None)
        assert _resolve_role(None, "someone.else@bustelberg.nl") == "user"

    def test_a_missing_role_falls_back_to_the_allowlist(self):
        """This is the state a signup produced while the trigger was missing."""
        assert _resolve_role(None, "reinier7175@gmail.com") == "admin"

    def test_an_explicit_user_role_is_an_intentional_demotion(self):
        """⚠ NOT overridden by the allowlist. The frontend renders whatever `app_metadata.role`
        says, so an allowlist that outranked an explicit 'user' would serve admin data to an
        account every screen draws as a regular user — see `_resolve_role`'s own docstring."""
        assert _resolve_role("user", "reinier7175@gmail.com") == "user"

    def test_an_explicit_admin_role_stands_on_its_own(self):
        assert _resolve_role("admin", "someone.else@bustelberg.nl") == "admin"

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


def _sql(name_fragment: str) -> str:
    hits = sorted(MIGRATIONS.glob(f"*{name_fragment}*.sql"))
    assert hits, f"no migration matching *{name_fragment}*.sql"
    return "\n".join(p.read_text(encoding="utf-8") for p in hits)


class TestTheAllowlistIsTheSameEverywhere:
    def test_the_trigger_function_carries_exactly_the_backend_hashes(self):
        found = set(_HASH_RE.findall(_sql("admin_email_hash")))
        assert found == set(_ADMIN_EMAIL_HASHES)

    def test_the_repair_migration_carries_them_too(self):
        found = set(_HASH_RE.findall(_sql("admin_signup_trigger")))
        assert found == set(_ADMIN_EMAIL_HASHES)


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
    @pytest.mark.parametrize("email", ["reinier@bustelberg.nl", "reinier7175@gmail.com"])
    def test_the_hardcoded_addresses_match(self, email):
        assert _is_hardcoded_admin_email(email)

    def test_matching_ignores_case_and_padding(self):
        """The trigger lowercases before hashing (`lower(NEW.email)`); the backend must agree, or
        an address typed with a capital is an admin in one half of the app and not the other."""
        assert _is_hardcoded_admin_email("  REINIER@Bustelberg.NL  ")

    def test_everyone_else_is_a_plain_user(self):
        assert not _is_hardcoded_admin_email("someone.else@bustelberg.nl")
        assert not _is_hardcoded_admin_email(None)
        assert _resolve_role(None, "someone.else@bustelberg.nl") == "user"

    def test_a_missing_role_falls_back_to_the_allowlist(self):
        """This is the state a signup produced while the trigger was missing."""
        assert _resolve_role(None, "reinier@bustelberg.nl") == "admin"

    def test_an_explicit_user_role_is_an_intentional_demotion(self):
        """⚠ NOT overridden by the allowlist. The frontend renders whatever `app_metadata.role`
        says, so an allowlist that outranked an explicit 'user' would serve admin data to an
        account every screen draws as a regular user — see `_resolve_role`'s own docstring."""
        assert _resolve_role("user", "reinier@bustelberg.nl") == "user"

    def test_an_explicit_admin_role_stands_on_its_own(self):
        assert _resolve_role("admin", "someone.else@bustelberg.nl") == "admin"

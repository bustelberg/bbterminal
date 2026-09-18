"""The hardcoded-admin allowlist lives in THREE places, and nothing kept them together.

`routers/auth.py::_ADMIN_EMAIL_HASHES` decides what the API answers; the trigger function decides
what `app_metadata.role` a new signup gets; the backfill decides what existing rows get. The
backend and the database each look correct in isolation while disagreeing — and the disagreement
is invisible in the worst possible direction: an account the API serves as ADMIN while every
screen renders it as a regular user, because the frontend reads `app_metadata.role` verbatim.

 AND THE TRIGGER ITSELF WAS MISSING FOR MONTHS (2026-09-08). Two migrations create the
FUNCTION `public.set_admin_role_on_signup()`; neither ever wrote `CREATE TRIGGER`, so nothing
called it. It looked healthy because 20260527010000 ends with a one-shot backfill UPDATE, which
fixed the rows existing at the time — a backfill is not a rule, and a re-signup or a fresh
environment got `role = NULL`.

 THESE ASSERTIONS READ SOURCE TEXT, WHICH THIS REPO OTHERWISE REFUSES TO DO. The rule exists
because a test over source can only confirm that one CALLER does the right thing, which is the
shape of the bug it is supposed to catch. It is right here for the opposite reason: the artifact
under test IS text. A migration is a file that will be replayed against a database we are banned
from booting in a test, so the only thing there is to check is what the file says.
"""
from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import pytest

from routers.auth import _ADMIN_EMAIL_HASHES, _is_hardcoded_admin_email, _resolve_role

REPO = Path(__file__).resolve().parents[2]
MIGRATIONS = REPO / "supabase" / "migrations"
_HASH_RE = re.compile(r"'([0-9a-f]{64})'")

#  The addresses themselves are not in this file, which is the whole point of the hashing.
# 20260527010000 swapped the plaintext allowlist for SHA-256 "so admin emails no longer appear in
# source", and this test then put both of them back in plaintext for months — a hash list is only
# opaque while nothing nearby prints its preimage. So the MECHANISM is exercised against a
# synthetic allowlist below, and the real addresses are checked only when the person running the
# suite supplies them (see `TestTheRealAddressesIfYouSupplyThem`).
_FAKE_ADMIN = "admin@example.test"
_FAKE_OTHER = "someone.else@example.test"
_FAKE_HASHES = frozenset({hashlib.sha256(_FAKE_ADMIN.encode("utf-8")).hexdigest()})


@pytest.fixture
def synthetic_allowlist(monkeypatch):
    """Point `_is_hardcoded_admin_email` at an allowlist whose preimage is public.

     `monkeypatch.setattr` on the module global works because both functions read
    `_ADMIN_EMAIL_HASHES` at CALL time; a `from ... import` copy in this file would not be enough.
    """
    monkeypatch.setattr("routers.auth._ADMIN_EMAIL_HASHES", _FAKE_HASHES)


def _latest_function_sql() -> str:
    """The NEWEST migration that defines `set_admin_role_on_signup()`.

     NEWEST, NOT ALL OF THEM. Earlier migrations still carry the OLD hash list and must not be
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
        """ The DEFINITION IN FORCE, i.e. the newest one. A hash the backend honours but the
        trigger does not means a signup that the API treats as admin and the frontend renders as a
        user — the desync `_resolve_role` exists to warn about."""
        sql = _latest_function_sql()
        body = sql[sql.index("admin_hashes"):sql.index("BEGIN")]
        assert set(_HASH_RE.findall(body)) == set(_ADMIN_EMAIL_HASHES)

    def test_there_is_exactly_one_hardcoded_admin(self):
        """ NARROWED FROM TWO TO ONE (2026-09-08, on request). Pinned as a COUNT because the
        second address was not merely dropped — with one admin there is no longer a second account
        able to reset the first's authenticator, so re-adding one is a decision about 2FA recovery
        and not a tidy-up."""
        assert len(_ADMIN_EMAIL_HASHES) == 1


class TestTheTriggerIsActuallyAttached:
    """ THE REGRESSION THIS FILE EXISTS FOR. `db diff` and `db dump` do not cover the `auth`
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
    """ Against a SYNTHETIC allowlist, not the real one. Every assertion here is about the
    RULE — how an address is normalised before hashing, and how an explicit role ranks against the
    allowlist — and none of them needs a real preimage to state it. Substituting a fake list keeps
    the addresses out of source without losing a single behaviour."""

    def test_an_address_on_the_list_matches(self, synthetic_allowlist):
        assert _is_hardcoded_admin_email(_FAKE_ADMIN)

    def test_matching_ignores_case_and_padding(self, synthetic_allowlist):
        """The trigger lowercases before hashing (`lower(NEW.email)`); the backend must agree, or
        an address typed with a capital is an admin in one half of the app and not the other."""
        assert _is_hardcoded_admin_email("  ADMIN@Example.TEST  ")

    def test_everyone_else_is_a_plain_user(self, synthetic_allowlist):
        assert not _is_hardcoded_admin_email(_FAKE_OTHER)
        assert not _is_hardcoded_admin_email(None)
        assert _resolve_role(None, _FAKE_OTHER) == "user"

    def test_a_missing_role_falls_back_to_the_allowlist(self, synthetic_allowlist):
        """This is the state a signup produced while the trigger was missing."""
        assert _resolve_role(None, _FAKE_ADMIN) == "admin"

    def test_an_explicit_user_role_is_an_intentional_demotion(self, synthetic_allowlist):
        """ NOT overridden by the allowlist. The frontend renders whatever `app_metadata.role`
        says, so an allowlist that outranked an explicit 'user' would serve admin data to an
        account every screen draws as a regular user — see `_resolve_role`'s own docstring. This is
        also the ONLY thing that demotes an address still on the list, which is half of what
        20260908150000 had to do."""
        assert _resolve_role("user", _FAKE_ADMIN) == "user"

    def test_an_explicit_admin_role_stands_on_its_own(self, synthetic_allowlist):
        assert _resolve_role("admin", _FAKE_OTHER) == "admin"


class TestTheRealAddressesIfYouSupplyThem:
    """ THE ONE THING A HASH LIST CANNOT BE ASKED IN PUBLIC: whether the RIGHT person is on it.
    Checking that costs the preimage, which is exactly what must not be committed — so the
    addresses come from the environment and these two skip when it is empty:

        BB_TEST_ADMIN_EMAIL=... BB_TEST_DEMOTED_EMAIL=... uv run pytest tests/test_admin_email_hashes.py

     Skipped in CI by design. The identity is a one-time fact; what regresses is the mechanism
    above and the three-way agreement pinned at the top of this file, and both of those run always.
    """

    def test_the_configured_admin_address_matches(self):
        email = os.environ.get("BB_TEST_ADMIN_EMAIL")
        if not email:
            pytest.skip("set BB_TEST_ADMIN_EMAIL to check the real address")
        assert _is_hardcoded_admin_email(email)
        assert _resolve_role(None, email) == "admin"

    def test_the_configured_demoted_address_does_not(self):
        """ Pinned in BOTH directions because the two halves fail differently: dropping the hash
        alone leaves an account that carries an EXPLICIT `role: admin` from an earlier backfill
        admin for ever, since `_resolve_role` prefers an explicit role to this list on purpose."""
        email = os.environ.get("BB_TEST_DEMOTED_EMAIL")
        if not email:
            pytest.skip("set BB_TEST_DEMOTED_EMAIL to check the demoted address")
        assert not _is_hardcoded_admin_email(email)
        assert _resolve_role(None, email) == "user"


class TestNoAdminAddressIsCommittedInPlaintext:
    """ THE RATCHET, AND IT NAMES NOBODY. It collects every email-shaped token in the tracked
    source and hashes each one against the allowlists — so it fails if an admin address is ever
    pasted back into a comment, a fixture or a docstring, WITHOUT this file having to contain the
    address it is defending. That is the property the plaintext version of this test destroyed.

     It checks the HISTORICAL hashes too, not just the one in force: `_ADMIN_EMAIL_HASHES` is down
    to one entry, and the demoted address is no less private for having been demoted. The old
    lists are still readable in the earlier migrations, which is where the extra hashes come from.
    """

    #  Source only. The repo root also holds an untracked 124 MB `.pgdump`, and reading a database
    # dump to lint comments would make this test the slowest in the suite for no coverage at all.
    _ROOTS = ("backend", "frontend/app", "frontend/lib", "supabase/migrations", "docs")
    _SUFFIXES = {".py", ".ts", ".tsx", ".sql", ".md"}
    _SKIP_DIRS = {
        "node_modules", ".venv", ".next", "__pycache__", ".pytest_cache",
        ".git", "dist", "build", "coverage", ".gurufocus_cache",
    }
    _EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

    def _known_hashes(self) -> set[str]:
        """Every hash the allowlist has ever carried: the live set, plus every 64-hex literal in a
        migration that defines or repairs the admin role."""
        hashes = set(_ADMIN_EMAIL_HASHES)
        for path in MIGRATIONS.glob("*.sql"):
            sql = path.read_text(encoding="utf-8")
            if "set_admin_role_on_signup" in sql:
                hashes.update(_HASH_RE.findall(sql))
        return hashes

    def _source_files(self):
        for root in self._ROOTS:
            base = REPO / root
            if not base.exists():
                continue
            for path in base.rglob("*"):
                if path.suffix not in self._SUFFIXES or not path.is_file():
                    continue
                if self._SKIP_DIRS.intersection(path.parts):
                    continue
                yield path
        for path in REPO.glob("*.md"):
            yield path

    def test_no_committed_file_contains_an_admin_address(self):
        known = self._known_hashes()
        assert known, "no admin hashes found — the scan would pass vacuously"
        offenders = []
        for path in self._source_files():
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            for email in set(self._EMAIL_RE.findall(text)):
                digest = hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()
                if digest in known:
                    #  The file and the line, never the address — a failure message that printed
                    # the preimage would leak it into CI logs, which is the same disclosure by
                    # another route.
                    offenders.append(str(path.relative_to(REPO)))
        assert not offenders, (
            "an admin email appears in plaintext in: " + ", ".join(sorted(set(offenders)))
            + " — refer to it by hash, or use a synthetic address"
        )

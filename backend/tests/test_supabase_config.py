"""What `supabase/config.toml` must say, pinned against a regenerated stock template.

⚠⚠ THE FAILURE THIS GUARDS IS A SILENT REVERT, NOT A TYPO. `config.toml` ships with the Supabase
CLI and every setting here starts life `false` in that template; anyone re-running `supabase init`
against a newer CLI, or merging its updated defaults, turns MFA back off with no diff anyone would
read as security-relevant. The symptom lands nowhere near the cause — `mfa.enroll()` starts
failing 422 for everybody, which reads as a client bug.

⚠ IT CAN ONLY SEE THE LOCAL HALF. The hosted project is configured in the Supabase dashboard and
this file does not reach it (`db push` carries migrations, not config), so a test cannot tell you
prod is right. Same split as `supabase/templates/*.html`. What it can do is make the local
intention explicit, so a drift between the two is a decision somebody made rather than a default
that crept back.

⚠ Reads source text, which this repo otherwise refuses to do — for the same reason
`test_admin_email_hashes.py` does: the artifact under test IS a file, and the alternative is
booting the stack, which the unit-tests-only rule bans.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

CONFIG = Path(__file__).resolve().parents[2] / "supabase" / "config.toml"


def _auth() -> dict:
    return tomllib.loads(CONFIG.read_text(encoding="utf-8"))["auth"]


def _mfa() -> dict:
    return _auth()["mfa"]


class TestMultiFactorAuth:
    def test_totp_is_enabled_on_both_halves(self):
        """⚠ BOTH FLAGS OR NEITHER. `enroll_enabled` without `verify_enabled` lets somebody add a
        factor they can then never satisfy — an account locked out by a feature that looked like
        it worked."""
        totp = _mfa()["totp"]
        assert totp["enroll_enabled"] is True
        assert totp["verify_enabled"] is True

    def test_sms_stays_off(self):
        """⚠ A CHOICE, NOT AN OVERSIGHT — see the comment in `config.toml`. SMS costs money per
        message and inherits the built-in provider's rate cap, which is the same cap
        `describeSendError` already has to explain to people trying to sign in."""
        phone = _mfa()["phone"]
        assert phone["enroll_enabled"] is False
        assert phone["verify_enabled"] is False

    def test_more_than_one_factor_may_be_enrolled(self):
        """⚠ THE ONLY RECOVERY PATH THERE IS. Supabase TOTP ships no backup codes, so a second
        enrolled authenticator (a spare device, a desktop app) is what stands between a lost phone
        and an admin deleting the factor by hand. A cap of 1 would remove it."""
        assert _mfa()["max_enrolled_factors"] > 1


class TestSessionLifetime:
    """⚠⚠ A SUPABASE SESSION IS OTHERWISE PERMANENT, WHICH IS WHAT MAKES THIS LOAD-BEARING. The
    access token expires hourly but the refresh token rotates for ever, so somebody who signed in
    once in January still holds a live session in December and has never been asked for a second
    factor again. The timebox is the entire mechanism behind "sign in again every month" — without
    it, mandatory MFA is a one-off ritual at signup rather than a recurring check."""

    def test_a_session_is_timeboxed(self):
        assert _auth()["sessions"]["timebox"] == "720h"   # 30 days

    def test_there_is_no_inactivity_timeout(self):
        """⚠ DELIBERATELY ABSENT. It logs people out for not visiting, which on a dashboard opened
        twice a week is a lockout dressed as a security control — and it bounds nothing the
        timebox does not already bound."""
        assert "inactivity_timeout" not in _auth()["sessions"]

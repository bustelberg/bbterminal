"""Auth + admin user management.

Endpoints:
    DELETE /api/auth/delete-account            self-service account deletion
    GET    /api/auth/me                        caller's user info + role
    GET    /api/auth/users                     list all users (admin only)
    POST   /api/auth/users                     create a user (admin only)
    PATCH  /api/auth/users/{user_id}/role      promote/demote (admin only)
    POST   /api/auth/users/{user_id}/mfa/reset clear another user's authenticators (admin only)
    DELETE /api/auth/users/{user_id}           delete a user (admin only)

The `_require_admin` helper checks app_metadata.role == 'admin' on the
caller's JWT — set by the signup trigger (20260908090000_admin_signup_trigger.sql) or by
PATCH /api/auth/users/{id}/role.

⚠⚠ THERE IS NO `POST /api/auth/impersonate` ANY MORE, AND IT MUST NOT COME BACK (removed
2026-09-08, on request). It minted a REAL session for another user — `admin.generate_link`
followed by `verify_otp` — which the frontend then installed with `setSession`. Three things made
it worth deleting rather than guarding:

  · It was the way around every future authentication rule. A minted session is `aal1` and the
    target's factors are never challenged, so it would have been a standing MFA bypass the day
    2FA shipped — see the notes in `lib/sessionStore` history for the client half.
  · It required the browser to keep the target's `refresh_token` in `localStorage` to be useful,
    which is the exposure `lib/sessionStore.ts` was written to shrink and is now gone with it.
  · It produced sessions indistinguishable from the real user's in every log and every audit
    trail, so "who did this" had no answer.

The supported way to see what a non-admin sees is the `X-View-As` preview (`routers/_authz.py`),
which changes what is RENDERED and AUTHORIZED without minting anything: same identity, same JWT,
one header. If someone needs to act as another user, promote/demote through
PATCH /api/auth/users/{id}/role and let them sign in themselves.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time

import httpx
import jwt

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from deps import supabase

_log = logging.getLogger(__name__)

router = APIRouter(tags=["auth"])

# SHA-256(lower(email)) hex of the hardcoded admin email.
#
# ⚠ TWO COPIES, CHANGED TOGETHER: this set and the LATEST migration that redefines
# `set_admin_role_on_signup()` — currently 20260908150000_single_hardcoded_admin.sql. Earlier
# migrations still carry the OLD list and must not be edited: they record what already ran.
# Pinned by tests/test_admin_email_hashes.py, which reads the newest definition.
#
# ⚠⚠ IT WENT FROM TWO ADDRESSES TO ONE (2026-09-08, on request): reinier7175@gmail.com keeps the
# automatic grant, reinier@bustelberg.nl was demoted to a plain user by that migration. Dropping
# the hash alone would have done nothing — the row carried an EXPLICIT role, and `_resolve_role`
# prefers an explicit role to this allowlist on purpose. ⚠ The cost is that there is now ONE admin,
# so the two-account 2FA recovery path (one admin resetting the other's authenticator) is gone.
#
# ⚠⚠ THE FUNCTION WAS ATTACHED TO NOTHING UNTIL 20260908090000 — both earlier migrations
# create it and neither wrote CREATE TRIGGER, so a signup never set app_metadata.role and
# only that migration's one-shot backfill had ever written it. The fallback below is what
# kept the API answering correctly, which is also what hid it: the frontend reads
# app_metadata.role verbatim, so the account was admin to every endpoint and a plain user
# on every screen.
_ADMIN_EMAIL_HASHES: frozenset[str] = frozenset({
    "5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f",
})


def _is_hardcoded_admin_email(email: str | None) -> bool:
    if not email:
        return False
    h = hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()
    return h in _ADMIN_EMAIL_HASHES


def _resolve_role(role: str | None, email: str | None) -> str:
    """Effective role for a verified user. An EXPLICIT role always wins —
    only a missing/blank role (an account predating the signup trigger, or
    one whose role was wiped) falls back to the hardcoded-admin-email
    allowlist.

    Critically, an explicit `role == "user"` is an INTENTIONAL demotion
    (e.g. an admin's second account used to exercise the non-admin UI) and
    must NOT be overridden by the allowlist — otherwise the backend serves
    admin data to an account the frontend correctly renders as a regular
    user (the `app_metadata.role` the frontend reads stays "user"),
    desyncing the two and e.g. leaking admin-only scheduled strategies into
    the read-only /schedule view."""
    if role:
        return role
    if _is_hardcoded_admin_email(email):
        return "admin"
    return "user"


# In-process verification cache: token → (expiry_monotonic, {id,email,role}).
# The API auth gate runs on every request (including high-frequency polling
# reads), so without this every poll would round-trip to GoTrue. A short TTL
# keeps revocation reasonably fresh while making the common case a dict hit.
_TOKEN_CACHE: dict[str, tuple[float, dict]] = {}
_TOKEN_CACHE_TTL = 60.0


def _token_aal(token: str) -> str | None:
    """The session's Authenticator Assurance Level: 'aal2' once a second factor was used.

    ⚠⚠ IT HAS TO COME OUT OF THE JWT, BECAUSE IT IS NOT ON THE USER. `supabase.auth.get_user()`
    returns the user record — id, email, app_metadata — and assurance is a property of the SESSION,
    not of the person: the same account is `aal1` in one browser and `aal2` in another. So the one
    call this module already makes cannot answer the question, and the claim has to be read.

    ⚠ THE SIGNATURE IS DELIBERATELY NOT VERIFIED HERE, and that is safe for one specific reason:
    `verify_token` has ALREADY handed this exact string to GoTrue and been told it is valid. This
    decode only asks what the token it just authenticated says about itself. Verifying again would
    need the project's JWT secret in this process — a second copy of a credential, to re-answer a
    question already answered over the wire. ⚠ It is therefore only ever correct at THIS call site,
    after that check: decoding an unverified token anywhere else would trust its claims outright.

    ⚠⚠ ON ITS OWN IT CANNOT TELL "SKIPPED THE FACTOR" FROM "HAS NO FACTOR" — measured on the live
    stack, an account with no authenticator at all still gets `aal1`. See `_has_verified_factor`;
    the two are only a rule together.

    ⚠ RETURNS None RATHER THAN A DEFAULT on anything unreadable. 'aal1' would be a guess that reads
    as a fact, and the caller (`_auth_middleware`) decides what an unknown level means — which is
    where that decision belongs, since it is a policy about denial rather than about parsing.
    """
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except Exception as e:
        # ⚠ WARNING, NOT DEBUG. A token GoTrue accepted that we cannot parse means the two sides
        # disagree about what a token is, which is worth seeing before it becomes a lockout.
        _log.warning("[auth] could not read aal from an accepted token: %s", e)
        return None
    aal = claims.get("aal")
    return aal if isinstance(aal, str) else None


def _has_verified_factor(user) -> bool:
    """Does this account have an authenticator it could have used?

    ⚠⚠ THIS IS THE HALF `aal` DOES NOT CARRY, AND ASSUMING OTHERWISE WAS A LOCKOUT WAITING TO
    DEPLOY. The first cut of the gate refused any admin whose token said `aal1`, on the belief that
    an account with no factor would carry no `aal` claim at all. Measured against the live stack it
    does: a brand-new account with zero authenticators signs in and gets **`aal1`**. So that rule
    refused EVERY admin — including the ones who had never enrolled and had no way to comply —
    which is the exact flag-day lockout the design was supposed to avoid.

    The honest test is `aal1` AND a verified factor exists, which is precisely what the browser's
    `getAuthenticatorAssuranceLevel().nextLevel === 'aal2'` means. Same rule, both sides.

    ⚠ FREE. `verify_token` already holds the user object from the call it makes anyway; GoTrue puts
    the factors on it (absent → None, never an error). No extra round trip, and it is cached with
    the rest of the verdict.

    ⚠ VERIFIED ONLY. An abandoned enrolment leaves an `unverified` factor behind (see
    `mfaFactors.unverifiedIds`), and counting one would lock somebody out with a half-set-up
    authenticator that can never produce a valid code.
    """
    try:
        return any(getattr(f, "status", None) == "verified" for f in (getattr(user, "factors", None) or []))
    except Exception as e:
        # ⚠ FAIL OPEN, AND LOUDLY. Not knowing whether a factor exists is not evidence that one
        # was skipped — the same distinction `AuthBackendUnavailable` draws.
        _log.warning("[auth] could not read factors off the user object: %s", e)
        return False


class AuthBackendUnavailable(RuntimeError):
    """GoTrue could not be reached — we do not know whether the token is valid.

    !! THIS EXISTS BECAUSE "WE COULDN'T CHECK" AND "YOUR TOKEN IS BAD" ARE
    DIFFERENT ANSWERS, AND CONFLATING THEM COSTS HOURS. `verify_token` used to
    catch *everything* from `get_user` and return None, which the gate renders
    as **401 Authentication required** — a sentence that says the credentials
    are wrong. On 2026-08-11 a `REINDEX INDEX CONCURRENTLY` saturated prod's
    disk I/O; every query slowed ~14x (a trivial count went 350ms -> 5,094ms),
    GoTrue's own DB lookup timed out, and the whole app answered 401. Nothing
    was wrong with anyone's session, and the 401 sent the diagnosis toward
    expired logins and broken auth config instead of toward the one process
    that was eating the disk.

    A 401 tells the client to re-authenticate — which cannot help, and which a
    frontend may act on by destroying a perfectly good session. The honest
    answer is 503: transient, not your fault, retry.
    """


# httpx.TransportError is the base of TimeoutException, NetworkError,
# ProtocolError and ProxyError — i.e. every "never got an answer" case, and
# nothing that carries an actual auth verdict. Anything else (an AuthApiError
# saying the JWT is expired or malformed) is a real rejection.
#
# The auth client also wraps some transport faults in its own retryable type, and
# !! THE MODULE IT LIVES IN HAS BEEN RENAMED: it is `supabase_auth.errors` in the
# version we pin (`gotrue` no longer imports at all). Both spellings are tried so
# neither a downgrade nor a future rename silently empties this tuple — an empty
# tuple would not fail loudly, it would just route every timeout back to a 401,
# which is precisely the bug this code exists to prevent.
_AUTH_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (httpx.TransportError,)
for _mod in ("supabase_auth.errors", "gotrue.errors"):
    try:
        _errs = __import__(_mod, fromlist=["AuthRetryableError"])
    except Exception:  # pragma: no cover - version-dependent
        continue
    _retryable = getattr(_errs, "AuthRetryableError", None)
    if _retryable is not None:
        _AUTH_TRANSPORT_ERRORS = (*_AUTH_TRANSPORT_ERRORS, _retryable)
        break


def verify_token(authorization: str) -> dict | None:
    """Verify a Bearer token and return {id, email, role} (role defaults to
    'user'), or None when the token is missing/invalid. Cached for
    `_TOKEN_CACHE_TTL`s. Used by the API auth-gate middleware; raising
    helpers (`_require_admin`) stay for per-endpoint defense-in-depth."""
    token = (authorization or "").replace("Bearer ", "").strip()
    if not token:
        return None
    now = time.monotonic()
    hit = _TOKEN_CACHE.get(token)
    if hit and hit[0] > now:
        return hit[1]
    try:
        user_resp = supabase.auth.get_user(token)
    except _AUTH_TRANSPORT_ERRORS as e:
        # Never reached GoTrue -> we have no verdict. Say so (503), don't guess 401.
        raise AuthBackendUnavailable(f"{type(e).__name__}: {e}") from e
    except Exception:
        return None
    user = getattr(user_resp, "user", None) if user_resp else None
    if not user:
        return None
    role = (getattr(user, "app_metadata", None) or {}).get("role")
    email = getattr(user, "email", None)
    info = {
        "id": user.id,
        "email": email,
        "role": _resolve_role(role, email),
        "aal": _token_aal(token),
        # ⚠ The other half of the MFA rule — see `_has_verified_factor`. Cached with the verdict
        # because it comes off the user object `get_user` already returned.
        "has_verified_factor": _has_verified_factor(user),
    }
    _TOKEN_CACHE[token] = (now + _TOKEN_CACHE_TTL, info)
    return info


def _require_admin(authorization: str) -> dict:
    """Verify the Bearer token and return {id, email, role}. Raises 403
    unless the user has app_metadata.role == 'admin' OR their email
    hashes to a hardcoded admin (fallback for accounts that predate the
    signup trigger or whose role was wiped)."""
    token = (authorization or "").replace("Bearer ", "")
    if not token:
        raise HTTPException(401, "Missing Authorization header")
    try:
        user_resp = supabase.auth.get_user(token)
    except Exception as e:
        raise HTTPException(401, f"Token verification failed: {e}")
    user = getattr(user_resp, "user", None) if user_resp else None
    if not user:
        raise HTTPException(401, "Invalid token — no user found")
    role = (getattr(user, "app_metadata", None) or {}).get("role")
    email = getattr(user, "email", None)
    if _resolve_role(role, email) != "admin":
        raise HTTPException(403, "Admin role required")
    return {"id": user.id, "email": email, "role": "admin"}


@router.delete("/api/auth/delete-account")
async def delete_account(authorization: str = Header(...)):
    """Delete the authenticated user's own account."""
    token = authorization.replace("Bearer ", "")
    try:
        user_resp = supabase.auth.get_user(token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {e}")
    if not user_resp or not user_resp.user:
        raise HTTPException(status_code=401, detail="Invalid token — no user found")
    user_id = user_resp.user.id
    try:
        supabase.auth.admin.delete_user(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Admin delete failed: {e}")
    return {"ok": True}


@router.get("/api/auth/me")
async def auth_me(authorization: str = Header(...)):
    """Return the caller's user info + role. The frontend uses this to
    decide what to show; the source of truth for access is still the
    middleware / per-endpoint admin check, not this endpoint."""
    token = (authorization or "").replace("Bearer ", "")
    try:
        user_resp = supabase.auth.get_user(token)
    except Exception as e:
        raise HTTPException(401, f"Token verification failed: {e}")
    user = getattr(user_resp, "user", None) if user_resp else None
    if not user:
        raise HTTPException(401, "Invalid token")
    role = (getattr(user, "app_metadata", None) or {}).get("role") or "user"
    return {"id": user.id, "email": user.email, "role": role}


class CreateUserRequest(BaseModel):
    email: str
    password: str
    role: str = "user"  # 'user' or 'admin'


def _user_detail() -> dict[str, dict]:
    """Per-user state the admin API does not return, keyed by user id. Best-effort.

    ⚠⚠ ONE QUERY, NOT ONE CALL PER USER. `admin.mfa.list_factors` exists but is per-user, so the
    obvious version is N+1 round trips to GoTrue every time /users loads — fine at six users and
    quietly awful later. The `auth` schema answers all of it at once.

    ⚠⚠ THE PASSWORD ITSELF IS NOT HERE AND MUST NOT BE, NOT EVEN AS A HASH. Asked for directly
    (2026-09-08) and declined: a bcrypt hash is not a fact about a person, it is an OFFLINE
    CRACKING TARGET — put it on a screen and it lives in screenshots, browser history and the DOM
    of a page anyone shoulder-reading can see, and the only thing anybody can DO with it is attack
    it. Plaintext does not exist at all, correctly. What an admin actually needs to know is
    WHETHER a password is set — an invited user who never chose one still signs in by link, and
    that is the difference `has_password` reports.

    ⚠ `banned_until` IS INCLUDED because Supabase's own dashboard can set it and nothing in this
    app can, so an account locked there would otherwise look perfectly healthy here.
    """
    from common.pg import _db_url  # noqa: PLC0415

    url = _db_url()
    if not url:
        # ⚠ NOT AN ERROR. The list still renders with everything the admin API knows; the extra
        # columns simply say "unknown" rather than taking the page down with them.
        return {}
    try:
        import psycopg  # noqa: PLC0415

        with psycopg.connect(url, connect_timeout=15) as conn:
            rows = conn.execute("""
                SELECT u.id::text,
                       u.encrypted_password IS NOT NULL           AS has_password,
                       u.email_confirmed_at IS NOT NULL           AS email_confirmed,
                       u.banned_until,
                       (SELECT count(*) FROM auth.mfa_factors f
                         WHERE f.user_id = u.id AND f.status = 'verified')   AS mfa_verified,
                       (SELECT count(*) FROM auth.mfa_factors f
                         WHERE f.user_id = u.id AND f.status <> 'verified')  AS mfa_pending,
                       (SELECT min(f.created_at) FROM auth.mfa_factors f
                         WHERE f.user_id = u.id AND f.status = 'verified')   AS mfa_since,
                       (SELECT count(*) FROM auth.sessions s
                         WHERE s.user_id = u.id)                             AS sessions
                  FROM auth.users u
            """).fetchall()
    except Exception as e:
        _log.warning("[auth] could not read per-user detail: %s", e)
        return {}
    return {
        r[0]: {
            "has_password": bool(r[1]),
            "email_confirmed": bool(r[2]),
            "banned_until": str(r[3]) if r[3] else None,
            "mfa_verified": int(r[4]),
            # ⚠ SURFACED SEPARATELY FROM `mfa_verified`. A pending factor protects NOTHING — it is
            # an abandoned enrolment — so folding it into the count would report an unprotected
            # account as covered, which is the one direction this column must never be wrong in.
            "mfa_pending": int(r[5]),
            "mfa_since": str(r[6]) if r[6] else None,
            "sessions": int(r[7]),
        }
        for r in rows
    }


@router.get("/api/auth/users")
async def list_users(authorization: str = Header(...)):
    """List all users (admin only) with their sign-in state. See `_user_detail`."""
    _require_admin(authorization)
    try:
        resp = supabase.auth.admin.list_users()
    except Exception as e:
        raise HTTPException(500, f"List users failed: {e}")
    # supabase-py returns a list of User objects (not a wrapper).
    raw_users = resp if isinstance(resp, list) else getattr(resp, "users", []) or []
    detail = await asyncio.to_thread(_user_detail)
    out: list[dict] = []
    for u in raw_users:
        meta = getattr(u, "app_metadata", None) or {}
        uid = getattr(u, "id", None)
        d = detail.get(str(uid), {})
        out.append({
            "id": uid,
            "email": getattr(u, "email", None),
            "role": meta.get("role") or "user",
            "created_at": str(getattr(u, "created_at", "") or ""),
            "last_sign_in_at": str(getattr(u, "last_sign_in_at", "") or ""),
            # ⚠ `None` WHERE UNKNOWN, NOT A ZERO. Without `SUPABASE_DB_URL` these cannot be read,
            # and "0 authenticators" is a claim about the account while `null` is a claim about
            # us — the client renders them differently on purpose.
            "mfa_verified": d.get("mfa_verified"),
            "mfa_pending": d.get("mfa_pending"),
            "mfa_since": d.get("mfa_since"),
            "has_password": d.get("has_password"),
            "email_confirmed": d.get("email_confirmed"),
            "banned_until": d.get("banned_until"),
            "sessions": d.get("sessions"),
        })
    out.sort(key=lambda u: (u.get("role") != "admin", u.get("email") or ""))
    return {"users": out}


@router.post("/api/auth/users")
async def create_user(req: CreateUserRequest, authorization: str = Header(...)):
    """Create a new user with an initial password (admin only)."""
    _require_admin(authorization)
    if req.role not in ("user", "admin"):
        raise HTTPException(400, "role must be 'user' or 'admin'")
    try:
        result = supabase.auth.admin.create_user({
            "email": req.email,
            "password": req.password,
            "email_confirm": True,
            "app_metadata": {"role": req.role},
        })
    except Exception as e:
        raise HTTPException(500, f"Create user failed: {e}")
    user = getattr(result, "user", None) or result
    return {
        "id": getattr(user, "id", None),
        "email": getattr(user, "email", None),
        "role": req.role,
    }


class SetRoleRequest(BaseModel):
    role: str  # 'user' or 'admin'


@router.patch("/api/auth/users/{user_id}/role")
async def set_user_role(user_id: str, req: SetRoleRequest, authorization: str = Header(...)):
    """Promote/demote a user (admin only)."""
    _require_admin(authorization)
    if req.role not in ("user", "admin"):
        raise HTTPException(400, "role must be 'user' or 'admin'")
    try:
        existing = supabase.auth.admin.get_user_by_id(user_id)
        existing_user = getattr(existing, "user", None) or existing
        existing_meta = (getattr(existing_user, "app_metadata", None) or {})
        new_meta = {**existing_meta, "role": req.role}
        supabase.auth.admin.update_user_by_id(user_id, {"app_metadata": new_meta})
    except Exception as e:
        raise HTTPException(500, f"Update role failed: {e}")
    return {"id": user_id, "role": req.role}


@router.post("/api/auth/users/{user_id}/mfa/reset")
async def reset_user_mfa(user_id: str, authorization: str = Header(...)):
    """Remove every authenticator on another user's account (admin only).

    ⚠⚠ THIS IS THE ENTIRE RECOVERY STORY, BECAUSE SUPABASE TOTP HAS NO BACKUP CODES. A lost phone
    is otherwise a permanent lockout: two-factor is mandatory (`_auth_middleware`), so the person
    cannot sign in to remove the factor, and the factor is what they cannot produce. Without this
    the fix was hand-written SQL against production auth tables, performed under pressure on the
    worst possible day.

    ⚠⚠ IT REFUSES SELF-SERVICE, AND THAT IS NOT TIDINESS. `/account/security` makes removing your
    OWN authenticator require a current code — proof you still hold it — and an admin resetting
    themselves here would walk straight around that check. The result would be that a stolen
    `aal2` session could strip two-factor off the account and re-enrol on the thief's phone,
    turning a session compromise into a permanent one. Nothing is lost by refusing: an admin who
    is genuinely locked out cannot sign in to press this anyway. Their route back is a SECOND
    admin account, or `REQUIRE_MFA=0` on the host.

    ⚠⚠ AND IT EVICTS THEIR SESSIONS, WHICH IS THE HALF THAT MAKES IT SAFE. Removing a factor does
    not touch a session that already proved one: the `aal2` claim is in the issued token and the
    refresh keeps it. So a phone stolen WITH the app open would keep working after a "reset" — the
    exact scenario the button is pressed for. GoTrue exposes no admin logout (measured:
    `/admin/users/{id}/logout` and `/sessions` both 404), so the sessions are deleted directly,
    which is what GoTrue itself does on sign-out. Verified: the refresh token then answers
    `refresh_token_not_found`.

    ⚠ EVICTION IS BEST-EFFORT AND SAID SO IN THE RESPONSE. It needs `SUPABASE_DB_URL`; without it
    the factors still go — which is the ask — and the caller is told the sessions did not. Failing
    the whole reset because the optional half is unavailable would leave somebody locked out to
    protect them from a stale session.
    """
    me = _require_admin(authorization)
    if me["id"] == user_id:
        raise HTTPException(
            400,
            "Use /account/security to manage your own authenticators — it asks for a current "
            "code, which this does not.",
        )
    try:
        listed = supabase.auth.admin.mfa.list_factors({"user_id": user_id})
    except Exception as e:
        raise HTTPException(500, f"Could not list factors: {e}")
    # ⚠⚠ IT RETURNS A PLAIN LIST, NOT AN OBJECT WITH `.factors` (measured against supabase-py on
    # the live stack). The obvious `listed.factors` reads as None, the loop below never runs, and
    # the endpoint answers `{"ok": true, "factors_removed": 0}` — a SUCCESS that removed nothing,
    # for a person who has just been told their access is restored. It shipped that way for one
    # test run, and the test agreed with it because the probe used the same wrong accessor.
    # Both shapes are accepted so a library change cannot silently reintroduce that.
    factors = listed if isinstance(listed, list) else (getattr(listed, "factors", None) or [])
    removed = 0
    for f in factors:
        fid = getattr(f, "id", None)
        if not fid:
            continue
        try:
            supabase.auth.admin.mfa.delete_factor({"user_id": user_id, "id": fid})
            removed += 1
        except Exception as e:
            raise HTTPException(500, f"Could not remove factor {fid}: {e}")

    sessions_cleared = _evict_sessions(user_id)
    # ⚠ WARNING, NOT INFO. This is one admin removing another person's second factor — the single
    # most security-relevant thing this router does, and the line somebody will go looking for.
    _log.warning(
        "[auth] %s reset two-factor for user %s — %d factor(s) removed, sessions cleared: %s",
        me.get("email"), user_id, removed, sessions_cleared,
    )
    return {
        "ok": True,
        "id": user_id,
        "factors_removed": removed,
        "sessions_cleared": sessions_cleared,
    }


def _evict_sessions(user_id: str) -> bool:
    """Delete the user's sessions so an already-signed-in device stops working. Best-effort."""
    from common.pg import _db_url  # noqa: PLC0415

    url = _db_url()
    if not url:
        _log.warning(
            "[auth] SUPABASE_DB_URL is not set — factors removed for %s but existing sessions "
            "were NOT evicted; a device already signed in keeps working until the timebox ends.",
            user_id,
        )
        return False
    try:
        import psycopg  # noqa: PLC0415

        with psycopg.connect(url, connect_timeout=15) as conn:
            conn.execute("DELETE FROM auth.sessions WHERE user_id = %s", (user_id,))
            conn.commit()
        return True
    except Exception as e:
        # ⚠ NOT FATAL. The factors are already gone, which is what was asked for; reporting
        # failure now would suggest nothing happened.
        _log.warning("[auth] could not evict sessions for %s: %s", user_id, e)
        return False


@router.delete("/api/auth/users/{user_id}")
async def delete_user(user_id: str, authorization: str = Header(...)):
    """Delete a user (admin only)."""
    me = _require_admin(authorization)
    if me["id"] == user_id:
        raise HTTPException(400, "Use /api/auth/delete-account to delete your own account")
    try:
        supabase.auth.admin.delete_user(user_id)
    except Exception as e:
        raise HTTPException(500, f"Delete user failed: {e}")
    return {"ok": True, "id": user_id}



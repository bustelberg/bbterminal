"""Auth + admin user management.

Endpoints:
    DELETE /api/auth/delete-account            self-service account deletion
    GET    /api/auth/me                        caller's user info + role
    GET    /api/auth/users                     list all users (admin only)
    POST   /api/auth/users                     create a user (admin only)
    PATCH  /api/auth/users/{user_id}/role      promote/demote (admin only)
    DELETE /api/auth/users/{user_id}           delete a user (admin only)
    POST   /api/auth/impersonate               mint a session for another user (admin only)

The `_require_admin` helper checks app_metadata.role == 'admin' on the
caller's JWT — set by migration 20260502000000_admin_role.sql or by
PATCH /api/auth/users/{id}/role.
"""

from __future__ import annotations

import hashlib
import os
import time

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from supabase import create_client

from deps import supabase

router = APIRouter(tags=["auth"])

# SHA-256(lower(email)) hex of hardcoded admin emails. Mirrors the
# trigger in 20260527010000_admin_email_hash.sql — change both together.
_ADMIN_EMAIL_HASHES: frozenset[str] = frozenset({
    "9fe083c7c1b2b6273a30b369870280d9cdfd3a89e165e6c2d68035cf1f7f144f",
    "5db5e75947119ef23451bc46919479a90b6bd51cd2e81815f2c7083e20fde36f",
})


def _is_hardcoded_admin_email(email: str | None) -> bool:
    if not email:
        return False
    h = hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()
    return h in _ADMIN_EMAIL_HASHES


# In-process verification cache: token → (expiry_monotonic, {id,email,role}).
# The API auth gate runs on every request (including high-frequency polling
# reads), so without this every poll would round-trip to GoTrue. A short TTL
# keeps revocation reasonably fresh while making the common case a dict hit.
_TOKEN_CACHE: dict[str, tuple[float, dict]] = {}
_TOKEN_CACHE_TTL = 60.0


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
    except Exception:
        return None
    user = getattr(user_resp, "user", None) if user_resp else None
    if not user:
        return None
    role = (getattr(user, "app_metadata", None) or {}).get("role")
    email = getattr(user, "email", None)
    if role != "admin" and _is_hardcoded_admin_email(email):
        role = "admin"
    info = {"id": user.id, "email": email, "role": role or "user"}
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
    if role != "admin" and not _is_hardcoded_admin_email(email):
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


@router.get("/api/auth/users")
async def list_users(authorization: str = Header(...)):
    """List all users (admin only). Returns id, email, role, created_at."""
    _require_admin(authorization)
    try:
        resp = supabase.auth.admin.list_users()
    except Exception as e:
        raise HTTPException(500, f"List users failed: {e}")
    # supabase-py returns a list of User objects (not a wrapper).
    raw_users = resp if isinstance(resp, list) else getattr(resp, "users", []) or []
    out: list[dict] = []
    for u in raw_users:
        meta = getattr(u, "app_metadata", None) or {}
        out.append({
            "id": getattr(u, "id", None),
            "email": getattr(u, "email", None),
            "role": meta.get("role") or "user",
            "created_at": str(getattr(u, "created_at", "") or ""),
            "last_sign_in_at": str(getattr(u, "last_sign_in_at", "") or ""),
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


class ImpersonateRequest(BaseModel):
    target_user_id: str


@router.post("/api/auth/impersonate")
async def impersonate_user(req: ImpersonateRequest, authorization: str = Header(...)):
    """Mint a real session for another user (admin only).

    Two-step server-side dance:
      1. `admin.generate_link({type: "magiclink", email})` produces a
         hashed_token normally embedded in an email.
      2. `auth.verify_otp({token_hash, type: "magiclink"})` consumes the
         hashed_token and returns a fresh `{access_token, refresh_token}`
         for the target user.

    The frontend then calls `supabase.auth.setSession(...)` with those
    tokens to swap the active session. No URL fragment, no magic-link
    redirect, no race with cookie writes.
    """
    _require_admin(authorization)
    try:
        existing = supabase.auth.admin.get_user_by_id(req.target_user_id)
    except Exception as e:
        raise HTTPException(404, f"Target user not found: {e}")
    target = getattr(existing, "user", None) or existing
    target_email = getattr(target, "email", None)
    if not target_email:
        raise HTTPException(404, "Target user has no email")

    try:
        link_result = supabase.auth.admin.generate_link({
            "type": "magiclink",
            "email": target_email,
            "options": {"redirect_to": "http://localhost"},
        })
    except Exception as e:
        raise HTTPException(500, f"generate_link failed: {e}")
    properties = getattr(link_result, "properties", None)
    hashed_token = getattr(properties, "hashed_token", None) if properties else None
    if not hashed_token:
        raise HTTPException(500, "generate_link returned no hashed_token")

    # Use a throwaway client for verify_otp — supabase-py is stateful and
    # verify_otp stores the resulting session on the client, replacing the
    # service_role auth header. Calling it on the global `supabase` would
    # silently switch every subsequent DB insert to the impersonated user's
    # JWT, which is then subject to RLS and fails with 42501.
    auth_only = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_KEY"],
    )
    try:
        verification = auth_only.auth.verify_otp({
            "token_hash": hashed_token,
            "type": "magiclink",
        })
    except Exception as e:
        raise HTTPException(500, f"verify_otp failed: {e}")
    session = getattr(verification, "session", None)
    if not session:
        raise HTTPException(500, "verify_otp returned no session")

    return {
        "target_email": target_email,
        "user_id": req.target_user_id,
        "access_token": getattr(session, "access_token", None),
        "refresh_token": getattr(session, "refresh_token", None),
        "expires_at": getattr(session, "expires_at", None),
    }

"""Shared verification for the `X-Cron-Secret`-protected cron endpoints.

Centralized so every cron entry point gets the SAME two guarantees:

  * **Fail closed**: if `CRON_SECRET` is unset/empty on the server, no
    request is accepted (500, not a silent pass-through).
  * **Constant-time compare**: `hmac.compare_digest`, not `!=`. A plain
    `!=` short-circuits on the first differing byte, leaking a timing
    oracle an attacker could use to recover the secret byte-by-byte.

Pinned by tests/test_cron_secret.py.
"""
from __future__ import annotations

import hmac
import os

from fastapi import HTTPException


def verify_cron_secret(provided: str) -> None:
    """Raise if the caller's `X-Cron-Secret` doesn't match `CRON_SECRET`.

    - 500 when `CRON_SECRET` is not configured on the server (fail closed).
    - 401 when the provided value doesn't match (constant-time).
    Returns None when valid.
    """
    expected = os.environ.get("CRON_SECRET", "")
    if not expected:
        raise HTTPException(500, "CRON_SECRET env var is not set on the server")
    if not hmac.compare_digest(provided or "", expected):
        raise HTTPException(401, "Invalid cron secret")

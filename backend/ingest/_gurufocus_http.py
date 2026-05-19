"""Cloudflare-aware HTTP client for GuruFocus.

GuruFocus puts its (authenticated) API behind Cloudflare's TLS-
fingerprint bot detection. `curl_cffi` replays a real-browser handshake
to sneak past it — but Cloudflare retires impersonation profiles every
few months, which used to mean a hand-edited "fixchrome" commit each
time. This module makes that self-healing:

  (A) At import, introspect `curl_cffi.requests.BrowserType` and rank
      every desktop profile newest-first. The default target is whatever
      Chrome version curl_cffi shipped most recently — no constant to
      bump in code.

  (B) On a 403/503 with an HTML body (Cloudflare's challenge page —
      detected by `<!doctype html>` / `<html` / `cloudflare` markers),
      retry the same URL with the next profile in the ladder. The first
      profile that gets a non-blocked response is cached as the
      preferred target for subsequent calls so we don't pay the ladder
      cost on every request.

A 403 with a JSON body is a real GuruFocus error (auth, quota,
unsubscribed exchange) — those pass through untouched.

What this does NOT solve: an IP-based block (Cloudflare cranking up
cloud-IP weighting won't be bypassed by any fingerprint in this
library). For that you need a proxy hop or GuruFocus dropping CF.
"""
from __future__ import annotations

import logging
import re
import threading

log = logging.getLogger(__name__)

try:
    from curl_cffi import requests as cf_requests  # type: ignore[import-not-found]
    from curl_cffi.requests import BrowserType  # type: ignore[import-not-found]
    _HAS_CURL_CFFI = True
    _CURL_CFFI_IMPORT_ERROR: str | None = None
except ImportError as _e:
    _HAS_CURL_CFFI = False
    _CURL_CFFI_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"


def _enumerate_targets() -> list[str]:
    """Ordered preference of impersonation profiles to try.

    Strategy: top-3 newest desktop Chrome (most likely to pass), then
    newest Firefox + newest Safari as a different-fingerprint-family
    fallback in case Cloudflare blocks the entire Chrome cluster.

    Skips `_android` / `_ios` suffixed variants (different TLS stacks,
    rarely useful), suffixed builds like `chrome133a` (experimental
    versions of the same fingerprint), and Edge/Tor (rare; unlikely to
    score higher than the Chrome/Firefox/Safari trio)."""
    if not _HAS_CURL_CFFI:
        return []
    chromes: list[tuple[int, str]] = []
    firefoxes: list[tuple[int, str]] = []
    safaris: list[tuple[int, str]] = []
    for name in dir(BrowserType):
        if name.startswith("_") or not isinstance(name, str):
            continue
        # Use fullmatch so `chrome131_android`, `safari260_ios`,
        # `chrome133a` etc. are filtered out — they're a different
        # fingerprint family or experimental.
        m = re.fullmatch(r"chrome(\d+)", name)
        if m:
            chromes.append((int(m.group(1)), name))
            continue
        m = re.fullmatch(r"firefox(\d+)", name)
        if m:
            firefoxes.append((int(m.group(1)), name))
            continue
        m = re.fullmatch(r"safari(\d+)", name)
        if m:
            safaris.append((int(m.group(1)), name))
            continue
    chromes.sort(reverse=True)
    firefoxes.sort(reverse=True)
    safaris.sort(reverse=True)
    out: list[str] = []
    out.extend(name for _, name in chromes[:3])
    out.extend(name for _, name in firefoxes[:1])
    out.extend(name for _, name in safaris[:1])
    return out


_TARGETS: list[str] = _enumerate_targets()

# Cached "currently working" profile. Starts at newest; updated to
# whatever the ladder lands on after a recovered block. Protected by
# a lock so concurrent callers (worker pool during a backtest stream)
# don't race on updates.
_preferred_lock = threading.Lock()
_preferred: str = _TARGETS[0] if _TARGETS else ""

if _HAS_CURL_CFFI:
    log.warning(
        "gurufocus http: curl_cffi ladder %s (preferred=%s)",
        _TARGETS, _preferred,
    )
else:
    log.error(
        "gurufocus http: curl_cffi import failed (%s) — Cloudflare WILL block "
        "production calls; install curl_cffi or accept the urllib fallback",
        _CURL_CFFI_IMPORT_ERROR,
    )


def _is_cloudflare_block(status: int, body: str) -> bool:
    """True if the response looks like Cloudflare's bot challenge / block
    page rather than a real upstream response. We use this to decide
    whether to step through the impersonation ladder — a 403 with a
    JSON body is a genuine GuruFocus auth/quota error and should NOT
    retry."""
    if status not in (403, 503):
        return False
    if not body:
        return False
    head = body[:500].lower()
    if "<!doctype html>" in head or "<html" in head:
        return True
    if "cloudflare" in head:
        return True
    return False


class CfResponse:
    """Lightweight response object returned by `cf_get`. The caller is
    responsible for URL masking and JSON parsing — we just hand back
    what we got, plus context for diagnostics."""
    __slots__ = ("status_code", "text", "used_target", "error", "attempted")

    def __init__(
        self,
        status_code: int | None,
        text: str,
        used_target: str,
        error: str | None,
        attempted: list[str],
    ):
        self.status_code = status_code
        self.text = text
        self.used_target = used_target
        self.error = error
        self.attempted = attempted

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code is not None and 200 <= self.status_code < 400

    @property
    def is_cloudflare_block(self) -> bool:
        return _is_cloudflare_block(self.status_code or 0, self.text or "")


def cf_get(
    url: str,
    headers: dict[str, str] | None = None,
    timeout: int = 30,
) -> CfResponse:
    """GET `url` through curl_cffi's impersonation ladder.

    Order: cached preferred profile → newest Chrome → next-newest Chrome
    → ... → newest Firefox → newest Safari. Stops on the first
    non-Cloudflare-blocked response and updates the cached preferred if
    we had to step past it.

    Returns a `CfResponse` capturing the final response (or the last
    attempt's response if every profile got blocked, so the caller has
    something to log). Always check `.ok` before consuming `.text`."""
    global _preferred
    if not _HAS_CURL_CFFI or not _TARGETS:
        return CfResponse(
            status_code=None,
            text="",
            used_target="",
            error=f"curl_cffi unavailable: {_CURL_CFFI_IMPORT_ERROR}",
            attempted=[],
        )

    with _preferred_lock:
        preferred = _preferred or _TARGETS[0]
    # Build the try-order: preferred first, then anything else in
    # ladder order, dedup'd. The preferred is usually equal to
    # _TARGETS[0]; only diverges after a recovered block.
    order: list[str] = [preferred]
    for t in _TARGETS:
        if t not in order:
            order.append(t)

    attempted: list[str] = []
    last: CfResponse | None = None
    for target in order:
        attempted.append(target)
        try:
            resp = cf_requests.get(
                url,
                headers=headers or {},
                timeout=timeout,
                impersonate=target,
            )
            body = resp.text or ""
            if _is_cloudflare_block(resp.status_code, body):
                last = CfResponse(
                    status_code=resp.status_code,
                    text=body,
                    used_target=target,
                    error=f"Cloudflare blocked impersonation target {target}",
                    attempted=attempted,
                )
                log.warning(
                    "gurufocus http: target=%s blocked by Cloudflare "
                    "(status=%s) — trying next in ladder",
                    target, resp.status_code,
                )
                continue
            # Either 2xx, or a non-CF 4xx (real GuruFocus error — auth,
            # quota, unknown ticker). Pass it through; the caller will
            # log it.
            if target != preferred:
                with _preferred_lock:
                    _preferred = target
                log.warning(
                    "gurufocus http: preferred target updated %s → %s "
                    "(previous was blocked)",
                    preferred, target,
                )
            return CfResponse(
                status_code=resp.status_code,
                text=body,
                used_target=target,
                error=None,
                attempted=attempted,
            )
        except Exception as e:
            last = CfResponse(
                status_code=None,
                text="",
                used_target=target,
                error=f"curl_cffi {target} exception: {type(e).__name__}: {e}",
                attempted=attempted,
            )
            log.warning(
                "gurufocus http: target=%s raised %s: %s — trying next",
                target, type(e).__name__, e,
            )
            continue

    if last is not None:
        return last
    return CfResponse(
        status_code=None,
        text="",
        used_target="",
        error="all impersonation targets failed",
        attempted=attempted,
    )


def is_available() -> bool:
    """Exposed so caller modules can log a clear "curl_cffi missing"
    diagnostic at boot without duplicating the import-try."""
    return _HAS_CURL_CFFI and bool(_TARGETS)


def current_preferred_target() -> str:
    """The profile cf_get will try first. Mostly useful for diagnostics
    / startup logging."""
    with _preferred_lock:
        return _preferred


def ladder() -> list[str]:
    """The full ordered list of profiles cf_get will try, in order."""
    return list(_TARGETS)

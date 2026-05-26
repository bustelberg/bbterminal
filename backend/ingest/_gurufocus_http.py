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
library). Mitigations the module DOES support for the IP-block case:

  * `GURUFOCUS_PROXY` env var (e.g. `http://user:pass@host:port` or
    `socks5://host:port`) — routes every curl_cffi call through that
    proxy. Point at a residential/ISP proxy (BrightData, Smartproxy,
    Oxylabs etc.) or your own home IP via a tunnel.
  * Circuit breaker — once `_CIRCUIT_THRESHOLD` consecutive
    Cloudflare blocks accumulate across all profiles, the client
    short-circuits for `_CIRCUIT_COOLDOWN_S` seconds. This stops the
    log spam + GuruFocus-quota waste during an extended IP block; the
    cooldown resets on the first successful request after the window.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time

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

    Strategy: a wide multi-family ladder. When Cloudflare flips its
    fingerprint detection (which has happened multiple times) it usually
    flags the most-popular *cluster* (e.g. the 3 newest Chromes), so the
    only way to keep working is to have un-popular fallbacks in reserve.

    Top-6 Chromes first (newest is statistically the best bet on any
    given day), then top-3 Firefoxes + top-3 Safaris (different TLS
    families — survive Chrome-cluster blocks), then newest Edge as a
    last-ditch (rare but distinct fingerprint).

    Skips `_android` / `_ios` suffixed variants (different TLS stacks,
    rarely useful) and suffixed builds like `chrome133a` (experimental).
    Tor is skipped because exit nodes are pre-blocked by Cloudflare."""
    if not _HAS_CURL_CFFI:
        return []
    chromes: list[tuple[int, str]] = []
    firefoxes: list[tuple[int, str]] = []
    safaris: list[tuple[int, str]] = []
    edges: list[tuple[int, str]] = []
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
        m = re.fullmatch(r"edge(\d+)", name)
        if m:
            edges.append((int(m.group(1)), name))
            continue
    chromes.sort(reverse=True)
    firefoxes.sort(reverse=True)
    safaris.sort(reverse=True)
    edges.sort(reverse=True)
    out: list[str] = []
    out.extend(name for _, name in chromes[:6])
    out.extend(name for _, name in firefoxes[:3])
    out.extend(name for _, name in safaris[:3])
    out.extend(name for _, name in edges[:1])
    return out


_TARGETS: list[str] = _enumerate_targets()

# Cached "currently working" profile. Starts at newest; updated to
# whatever the ladder lands on after a recovered block. Protected by
# a lock so concurrent callers (worker pool during a backtest stream)
# don't race on updates.
_preferred_lock = threading.Lock()
_preferred: str = _TARGETS[0] if _TARGETS else ""

# ── Proxy (for IP-block mitigation) ────────────────────────────────
# Read at import — set GURUFOCUS_PROXY (or fall back to the standard
# HTTPS_PROXY) before the backend starts. Empty string = direct
# connection. curl_cffi forwards the value verbatim to libcurl's
# --proxy, so any scheme libcurl supports works (http, https, socks5,
# socks5h). Credentials embedded in the URL are masked in logs.
_PROXY_ENV_VAR = "GURU" + "FOCUS_PROXY"  # = GURUFOCUS_PROXY (concat avoids a linter that mangles the bare literal)
_PROXY_URL = (
    os.environ.get(_PROXY_ENV_VAR)
    or os.environ.get("HTTPS_PROXY")
    or ""
).strip()


def _mask_proxy_url(url: str) -> str:
    """Hide creds in `user:password@host` proxy URLs before logging."""
    if not url:
        return ""
    return re.sub(r"://([^:@/]+):([^@]+)@", r"://***:***@", url)


# ── Circuit breaker (for IP-block silence) ─────────────────────────
# When N consecutive calls all get Cloudflare-blocked across the full
# ladder, assume the IP is blocked and stop hammering for COOLDOWN_S
# seconds. The first non-blocked response resets the counter. Without
# this an extended block (Railway IP gets Cloudflare-graylisted for
# hours) produced 6×_per_call log spam + wasted GuruFocus quota.
_CIRCUIT_THRESHOLD = 5
_CIRCUIT_COOLDOWN_S = 600  # 10 minutes
_circuit_lock = threading.Lock()
_consecutive_blocks = 0
_circuit_open_until: float = 0.0

if _HAS_CURL_CFFI:
    log.warning(
        "gurufocus http: curl_cffi ladder %s (preferred=%s) proxy=%s",
        _TARGETS, _preferred, _mask_proxy_url(_PROXY_URL) or "<direct>",
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
    what we got, plus context for diagnostics.

    `headers` is the final attempt's response headers (lower-cased keys).
    Surfacing `cf-ray`, `server`, `cf-mitigated`, `x-cache` etc. lets the
    caller (and the diagnostic probe) confirm Cloudflare vs some other
    intermediary without having to re-run the request."""
    __slots__ = ("status_code", "text", "used_target", "error", "attempted", "headers")

    def __init__(
        self,
        status_code: int | None,
        text: str,
        used_target: str,
        error: str | None,
        attempted: list[str],
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self.text = text
        self.used_target = used_target
        self.error = error
        self.attempted = attempted
        self.headers = headers or {}

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code is not None and 200 <= self.status_code < 400

    @property
    def is_cloudflare_block(self) -> bool:
        return _is_cloudflare_block(self.status_code or 0, self.text or "")

    def diagnostic_headers(self) -> dict[str, str]:
        """Subset of response headers that pin down which CDN / intermediary
        served the response. Used in error messages so we don't carry every
        cookie / cache header around."""
        keys = (
            "cf-ray", "cf-cache-status", "cf-mitigated", "cf-chl-bypass",
            "server", "x-cache", "x-served-by", "x-amz-cf-id",
            "via", "x-content-type-options",
        )
        return {
            k: self.headers[k]
            for k in keys
            if k in self.headers and self.headers[k]
        }


def _note_block_and_check_circuit() -> bool:
    """Increment the consecutive-block counter and return True if the
    circuit is now OPEN (i.e. callers should short-circuit). Thread-
    safe so concurrent ladder runs don't race."""
    global _consecutive_blocks, _circuit_open_until
    with _circuit_lock:
        _consecutive_blocks += 1
        if _consecutive_blocks >= _CIRCUIT_THRESHOLD:
            _circuit_open_until = time.time() + _CIRCUIT_COOLDOWN_S
            log.error(
                "gurufocus http: circuit OPEN — %s consecutive Cloudflare "
                "blocks across all impersonation targets. Suppressing further "
                "calls for %ss. Likely an IP-based block on this host; set "
                "%s to a residential proxy URL to mitigate.",
                _consecutive_blocks, _CIRCUIT_COOLDOWN_S, _PROXY_ENV_VAR,
            )
            return True
    return False


def _note_success() -> None:
    """Reset the circuit on a successful (non-blocked) response."""
    global _consecutive_blocks, _circuit_open_until
    with _circuit_lock:
        if _consecutive_blocks > 0 or _circuit_open_until > 0:
            log.info(
                "gurufocus http: circuit CLOSED — recovered after %s blocks.",
                _consecutive_blocks,
            )
        _consecutive_blocks = 0
        _circuit_open_until = 0.0


def _circuit_open_seconds_remaining() -> float:
    """Returns 0 when the circuit is closed; otherwise the seconds left
    until it auto-resets. Locked so we don't read a half-written
    `_circuit_open_until` from a parallel update."""
    with _circuit_lock:
        if _circuit_open_until <= 0:
            return 0.0
        remaining = _circuit_open_until - time.time()
        return remaining if remaining > 0 else 0.0


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

    Routes through `GURUFOCUS_PROXY` (or `HTTPS_PROXY`) when either
    env var is set — mandatory mitigation when the host's egress IP
    is in a Cloudflare-graylisted range (typical for cloud providers).

    Short-circuits with a clear error when the circuit breaker is
    OPEN (i.e. we've observed `_CIRCUIT_THRESHOLD` consecutive blocks
    across all profiles, the IP is almost certainly blocked, and the
    cooldown hasn't yet elapsed). This keeps logs readable + saves
    GuruFocus quota during an extended outage.

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

    # Circuit-breaker check — if recently saturated with blocks, skip
    # the ladder entirely and surface an explicit message.
    cooldown_left = _circuit_open_seconds_remaining()
    if cooldown_left > 0:
        return CfResponse(
            status_code=None,
            text="",
            used_target="",
            error=(
                f"GuruFocus circuit breaker open — IP appears blocked by "
                f"Cloudflare. Auto-retry in {int(cooldown_left)}s; set "
                f"{_PROXY_ENV_VAR} to bypass."
            ),
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

    # Strip any caller-supplied User-Agent (case-insensitively). curl_cffi's
    # `impersonate=` sets a *full* set of browser-matching headers including
    # User-Agent + Sec-CH-UA + Accept-Language. If the caller pins a Chrome/146
    # UA but we end up impersonating chrome142 or safari2601, the TLS
    # fingerprint disagrees with the UA -- Cloudflare's bot scorer flags
    # exactly this kind of inconsistency. Let curl_cffi own the browser
    # headers; the caller can still supply Accept, Accept-Encoding, etc.
    if headers:
        headers = {k: v for k, v in headers.items() if k.lower() != "user-agent"}

    # Pass the proxy through to libcurl when set. curl_cffi forwards
    # the kwarg verbatim.
    proxies = {"https": _PROXY_URL, "http": _PROXY_URL} if _PROXY_URL else None

    attempted: list[str] = []
    last: CfResponse | None = None
    saw_cf_block_this_call = False
    for target in order:
        attempted.append(target)
        try:
            resp = cf_requests.get(
                url,
                headers=headers or {},
                timeout=timeout,
                impersonate=target,
                proxies=proxies,
            )
            body = resp.text or ""
            # Lower-case header keys so callers / diagnostics can index
            # consistently regardless of the upstream casing. curl_cffi's
            # response.headers is typically a CaseInsensitiveDict, but
            # downstream consumers shouldn't have to assume that.
            resp_headers = {
                str(k).lower(): str(v)
                for k, v in (resp.headers or {}).items()
            }
            if _is_cloudflare_block(resp.status_code, body):
                saw_cf_block_this_call = True
                last = CfResponse(
                    status_code=resp.status_code,
                    text=body,
                    used_target=target,
                    error=f"Cloudflare blocked impersonation target {target}",
                    attempted=attempted,
                    headers=resp_headers,
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
            _note_success()
            return CfResponse(
                status_code=resp.status_code,
                text=body,
                used_target=target,
                error=None,
                attempted=attempted,
                headers=resp_headers,
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

    # Whole ladder failed. If at least one was a Cloudflare block (vs
    # all exceptions), count this call against the circuit breaker
    # threshold. Don't count network-error storms — those reset the
    # counter back to zero so a real CF block doesn't get masked.
    if saw_cf_block_this_call:
        _note_block_and_check_circuit()

    if last is not None:
        return last
    return CfResponse(
        status_code=None,
        text="",
        used_target="",
        error="all impersonation targets failed",
        attempted=attempted,
    )


def explain_failure(
    resp: "CfResponse",
    masked_url: str,
    *,
    subject: str | None = None,
) -> str:
    """Render a failed CfResponse as ONE clear, human-readable line.

    The previous error strings concatenated 200 chars of Cloudflare
    HTML, the impersonation-ladder debug list, and the full URL — none
    of which told a user "why didn't this work". This helper classifies
    the failure into a small set of root causes and emits the message
    that matches:

      * Circuit breaker open → "GuruFocus temporarily unreachable
        (proxy retry in Xs)"
      * Cloudflare-blocked entire ladder → "Cloudflare blocked all N
        browser fingerprints — set GURUFOCUS_PROXY"
      * Pre-response network error → "Network error: ..."
      * Real HTTP error from GuruFocus (non-CF 4xx/5xx) → "GuruFocus
        <status>: <message from JSON body, or first 120 chars>"
      * Empty body → "GuruFocus <status>: empty body"

    `subject` is a one-word context (e.g. ticker `"NYSE:ANF"`) inserted
    after "for"; pass None to omit. The URL is appended in parens at
    the end so developers can grep for it without burying the message.
    """
    import json as _json  # noqa: PLC0415

    for_clause = f" for {subject}" if subject else ""

    # Circuit-breaker short-circuit. The cf_get error string contains
    # the retry window; strip the redundant "GuruFocus circuit breaker
    # open — " prefix so we don't say "GuruFocus" twice.
    if resp.error and "circuit breaker open" in resp.error:
        detail = resp.error
        for prefix in (
            "GuruFocus circuit breaker open — ",
            "GuruFocus circuit breaker open: ",
        ):
            if detail.startswith(prefix):
                detail = detail[len(prefix):]
                break
        return f"GuruFocus temporarily unreachable{for_clause}: {detail} ({masked_url})"

    # Cloudflare blocked the entire ladder. is_cloudflare_block is set
    # whenever the FINAL response we kept was a CF-style HTML 403/503.
    if resp.is_cloudflare_block:
        n = len(resp.attempted) or len(_TARGETS)
        # Surface the CDN-identifying headers + a longer body slice so we
        # can tell a real Cloudflare block from any other HTML 403 (nginx,
        # AWS WAF, vendor's own gateway). Without this the message says
        # "Cloudflare" even when the upstream might be something else.
        diag = resp.diagnostic_headers()
        diag_str = ", ".join(f"{k}={v}" for k, v in diag.items()) if diag else "(no diagnostic headers)"
        body_excerpt = (resp.text or "")[:400].replace("\n", " ").strip()
        return (
            f"Cloudflare-style HTML {resp.status_code or '?'} blocked GuruFocus"
            f"{for_clause} on all {n} TLS fingerprints. "
            f"If `cf-ray` / `server=cloudflare` are below, this host's egress IP "
            f"is graylisted -- set {_PROXY_ENV_VAR}=<proxy URL>. If those headers "
            f"are absent, the 403 came from somewhere else and we should "
            f"investigate before blaming Cloudflare. "
            f"headers=[{diag_str}] "
            f"body={body_excerpt!r} ({masked_url})"
        )

    # Pre-response failure (network error / library exception / no
    # response at all).
    if resp.status_code is None:
        why = resp.error or "no response"
        return f"Network error{for_clause}: {why} ({masked_url})"

    # Real upstream HTTP error. Try to surface the API's own message
    # field rather than the raw body; if it's not JSON, take a short
    # excerpt — never the full body, never the HTML head matter.
    body = (resp.text or "").strip()
    if body:
        try:
            parsed = _json.loads(body)
            if isinstance(parsed, dict):
                for key in ("message", "error", "detail"):
                    val = parsed.get(key)
                    if val:
                        return f"GuruFocus {resp.status_code}{for_clause}: {val} ({masked_url})"
        except Exception:
            pass
        return f"GuruFocus {resp.status_code}{for_clause}: {body[:120]} ({masked_url})"
    return f"GuruFocus {resp.status_code}{for_clause}: empty body ({masked_url})"


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

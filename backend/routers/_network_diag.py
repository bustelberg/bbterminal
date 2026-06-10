"""Reachability diagnostics for the external services the app depends on.

Backs `GET /api/admin/network-diagnostics` (the `/network` page). For each
upstream we resolve its DNS, time a lightweight reachability probe, and
classify the result into one clear verdict + a human-readable reason.

The recurring prod failure mode is GuruFocus: its API sits behind
Cloudflare's bot detection, and Railway's egress IPs periodically land in a
Cloudflare-graylisted range, so every call gets a 403 challenge page (the
"IP appears blocked by Cloudflare. Auto-retry in Xs" the ingest logs show).
So GuruFocus is NOT probed with a plain GET (which a real browser
fingerprint would always fail) — it goes through the SAME `cf_get`
impersonation ladder + circuit breaker the ingest pipeline uses, so the
verdict matches what production actually experiences. The live circuit-
breaker countdown is surfaced separately so the page can show *why* calls
are being short-circuited right now.

Every other source is a plain HTTP reachability check: ANY HTTP response
(even a 401/404) means the network path works; only a DNS failure,
timeout, or connection error counts as "unreachable".

Verdicts:
    ok          2xx/3xx, or any non-block HTTP response → path healthy
    blocked     reached the host but Cloudflare (or a WAF) blocked us
    degraded    got an HTTP response but a server error (5xx)
    unreachable DNS / TCP / TLS failure — never got an HTTP response
"""
from __future__ import annotations

import asyncio
import os
import socket
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

# ── Catalogue of external dependencies ─────────────────────────────────
# `kind="gurufocus"` routes through the curl_cffi ladder; everything else
# is a plain HTTP probe. `url` is what we actually hit (kept to a cheap
# root/health path so we don't consume vendor API quota). Supabase's URL
# is resolved from the env at probe time.
_SOURCES: list[dict] = [
    {
        "name": "GuruFocus API",
        "kind": "gurufocus",
        "category": "critical",
        "purpose": "Price, volume & earnings data (the whole terminal)",
        "url": None,  # built from GURUFOCUS_BASE_URL + key at probe time
    },
    {
        "name": "Supabase",
        "kind": "supabase",
        "category": "critical",
        "purpose": "Database + auth (every page)",
        "url": None,  # filled from SUPABASE_URL at probe time
    },
    {
        "name": "OpenFIGI",
        "kind": "http",
        "category": "important",
        "purpose": "Ticker → exchange resolution (ingest / ACWI / S&P 500)",
        "url": "https://api.openfigi.com/v3/mapping",
    },
    {
        "name": "ECB FX",
        "kind": "http",
        "category": "important",
        "purpose": "EUR FX rates (returns shown in EUR)",
        "url": "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?lastNObservations=1&format=csvdata",
    },
    {
        "name": "Yahoo Finance",
        "kind": "http",
        "category": "optional",
        "purpose": "FX fallback when ECB is missing a pair",
        "url": "https://query1.finance.yahoo.com/v8/finance/chart/EURUSD=X?range=1d&interval=1d",
    },
    {
        "name": "MSCI",
        "kind": "http",
        "category": "optional",
        "purpose": "ACWI index-change announcements (/acwi)",
        "url": "https://app2.msci.com/webapp/index_ann/Announcement?doc_type=ANNOUNCEMENT&lang=en&prod_type=STANDARD&visibility=public&format=html&date_range=0",
    },
    {
        "name": "Wikipedia",
        "kind": "http",
        "category": "optional",
        "purpose": "S&P 500 constituent history (/universe_index)",
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
    },
    {
        "name": "Leonteq",
        "kind": "http",
        "category": "optional",
        "purpose": "Leonteq structured-products universe (/leonteq)",
        "url": "https://structuredproducts-ch.leonteq.com/website-api",
    },
    {
        "name": "AirSPMS",
        "kind": "http",
        "category": "optional",
        "purpose": "AIRS portfolio scraper (/airs-portfolio)",
        "url": "https://bustelberg.airspms.cloud",
    },
]

_USER_AGENT = "BBTerminal-NetworkCheck/1.0"
_HTTP_TIMEOUT = 8


def _resolve(host: str | None) -> tuple[str | None, str | None]:
    """(ip, error) — the A-record IP for `host`, or an error string. This is
    the "the IP and domain we're trying to reach" the page wants to show."""
    if not host:
        return None, "no host"
    try:
        return socket.gethostbyname(host), None
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {e}"


def _looks_like_cf_block(status: int | None, headers: dict[str, str], body_head: str) -> bool:
    """A Cloudflare *block/challenge* (vs. Cloudflare merely fronting a 200).
    Mirrors the ingest client's heuristic: a 403/503 whose body is an HTML
    challenge page and/or carries Cloudflare edge headers."""
    if status not in (403, 503):
        return False
    head = body_head.lower()
    html = "<!doctype html>" in head or "<html" in head
    cf_marker = (
        "cloudflare" in head
        or bool(headers.get("cf-ray"))
        or "cloudflare" in headers.get("server", "").lower()
        or bool(headers.get("cf-mitigated"))
    )
    return html or cf_marker


def _cdn_headers(headers: dict[str, str]) -> dict[str, str]:
    """The subset of response headers that pin down the CDN / intermediary —
    so the page can show *evidence* for a "blocked by Cloudflare" verdict
    rather than just asserting it."""
    keys = ("cf-ray", "cf-cache-status", "cf-mitigated", "server", "via", "x-cache")
    return {k: headers[k] for k in keys if headers.get(k)}


def _base_entry(src: dict, url: str | None) -> dict:
    host = urlparse(url).hostname if url else None
    resolved_ip, dns_err = _resolve(host)
    return {
        "name": src["name"],
        "category": src["category"],
        "purpose": src["purpose"],
        "domain": host,
        "url": url,
        "resolved_ip": resolved_ip,
        "dns_error": dns_err,
        "status_code": None,
        "latency_ms": None,
        "server": None,
        "cdn_headers": {},
        "verdict": "unreachable",
        "reason": "",
    }


def _probe_http(src: dict) -> dict:
    """Plain reachability probe. Any HTTP response = network path OK; only a
    connection-level failure is "unreachable"."""
    import requests  # noqa: PLC0415

    url = src["url"]
    entry = _base_entry(src, url)
    if entry["dns_error"]:
        entry["reason"] = f"DNS resolution failed for {entry['domain']}: {entry['dns_error']}"
        return entry

    t0 = time.monotonic()
    try:
        r = requests.get(
            url,
            timeout=_HTTP_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": _USER_AGENT, "Accept": "*/*"},
        )
    except requests.exceptions.Timeout:
        entry["latency_ms"] = round((time.monotonic() - t0) * 1000)
        entry["reason"] = f"Connection timed out after {_HTTP_TIMEOUT}s — host reachable in DNS ({entry['resolved_ip']}) but no HTTP response."
        return entry
    except Exception as e:  # noqa: BLE001
        entry["latency_ms"] = round((time.monotonic() - t0) * 1000)
        entry["reason"] = f"Connection failed: {type(e).__name__}: {e}"
        return entry

    entry["latency_ms"] = round((time.monotonic() - t0) * 1000)
    headers = {str(k).lower(): str(v) for k, v in r.headers.items()}
    body_head = (r.text or "")[:500]
    entry["status_code"] = r.status_code
    entry["server"] = headers.get("server")
    entry["cdn_headers"] = _cdn_headers(headers)

    if _looks_like_cf_block(r.status_code, headers, body_head):
        entry["verdict"] = "blocked"
        entry["reason"] = (
            f"Reached {entry['domain']} ({entry['resolved_ip']}) but Cloudflare returned a "
            f"{r.status_code} challenge — this host's egress IP looks graylisted."
        )
        return entry

    if r.status_code >= 500:
        entry["verdict"] = "degraded"
        entry["reason"] = f"Reachable, but upstream returned HTTP {r.status_code} (server-side error)."
        return entry

    entry["verdict"] = "ok"
    note = "" if r.status_code < 400 else f" (HTTP {r.status_code} — expected for an unauthenticated probe; the path is reachable)"
    entry["reason"] = f"Reachable — HTTP {r.status_code} in {entry['latency_ms']}ms{note}."
    return entry


def _probe_supabase(src: dict) -> dict:
    """Supabase has no fixed public host — read it from the env. Probe the
    PostgREST root, which answers even unauthenticated."""
    base = (os.environ.get("SUPABASE_URL", "") or "").strip().rstrip("/")
    url = f"{base}/rest/v1/" if base else None
    if not url:
        entry = _base_entry(src, None)
        entry["reason"] = "SUPABASE_URL is not set in this backend's environment."
        return entry
    return _probe_http({**src, "url": url})


def _gurufocus_probe_url() -> tuple[str | None, str | None, str | None]:
    """Build the URL to probe + a key-masked copy for display.

    Returns (url, masked_url, config_error). We hit a REAL authenticated
    endpoint (AAPL/price) rather than the bare domain root: Cloudflare
    serves a challenge page on the root regardless of IP, so a root probe
    would report "blocked" even when the live API works. On the
    authenticated path Cloudflare challenges *before* GuruFocus auth runs,
    so a block there genuinely means our egress IP is graylisted. Costs one
    GuruFocus API call per check — acceptable for a manual diagnostic."""
    base = (os.environ.get("GURUFOCUS_BASE_URL", "") or "").strip().rstrip("/")
    if base.endswith("/data"):
        base = base[: -len("/data")]
    key = os.environ.get("GURUFOCUS_API_KEY", "") or ""
    if not base or not key:
        return None, None, "GURUFOCUS_BASE_URL / GURUFOCUS_API_KEY not set in this backend's environment."
    url = f"{base}/public/user/{key}/stock/AAPL/price"
    masked = url.replace(key, key[:4] + "***")
    return url, masked, None


def _probe_gurufocus(src: dict) -> dict:
    """Probe GuruFocus through the curl_cffi impersonation ladder — the exact
    path the ingest pipeline uses — so the verdict matches production. A
    plain GET would always be Cloudflare-blocked and give a false negative."""
    from ingest._gurufocus_http import (  # noqa: PLC0415
        cf_get,
        circuit_seconds_remaining,
        is_available,
    )

    url, masked_url, cfg_err = _gurufocus_probe_url()
    # _base_entry resolves DNS from the URL host; the masked URL has the same
    # host, so use it for display (never leak the API key).
    entry = _base_entry(src, masked_url or "https://api.gurufocus.com/")
    entry["domain"] = "api.gurufocus.com"

    if cfg_err:
        entry["verdict"] = "degraded"
        entry["reason"] = cfg_err
        return entry

    if not is_available():
        entry["verdict"] = "degraded"
        entry["reason"] = (
            "curl_cffi is not installed — the backend can't replay a browser TLS "
            "handshake, so Cloudflare WILL block every GuruFocus call in prod."
        )
        return entry

    # If the circuit breaker is already open, cf_get short-circuits without a
    # network call. Report that as the live blocked state with the countdown.
    cooldown = circuit_seconds_remaining()

    t0 = time.monotonic()
    resp = cf_get(url, headers={"Accept": "application/json"}, timeout=15)
    entry["latency_ms"] = round((time.monotonic() - t0) * 1000)
    entry["status_code"] = resp.status_code
    entry["server"] = resp.headers.get("server")
    entry["cdn_headers"] = _cdn_headers(resp.headers)
    entry["used_target"] = resp.used_target or None

    if resp.error and "circuit breaker open" in resp.error:
        entry["verdict"] = "blocked"
        secs = int(circuit_seconds_remaining() or cooldown)
        entry["reason"] = (
            f"Circuit breaker OPEN — repeated Cloudflare blocks tripped it, so calls are "
            f"suppressed for ~{secs}s. The egress IP is blocked by Cloudflare; set "
            f"GURUFOCUS_PROXY to a residential proxy to bypass."
        )
        return entry

    if resp.is_cloudflare_block:
        entry["verdict"] = "blocked"
        entry["reason"] = (
            f"Cloudflare blocked all browser fingerprints (HTTP {resp.status_code}). "
            f"This host's egress IP is graylisted — set GURUFOCUS_PROXY to bypass."
        )
        return entry

    if resp.status_code is None:
        entry["verdict"] = "unreachable"
        entry["reason"] = f"No response: {resp.error or 'unknown network error'}."
        return entry

    if resp.ok:
        entry["verdict"] = "ok"
        entry["reason"] = (
            f"Cloudflare passed us through and the AAPL/price call returned HTTP "
            f"{resp.status_code} via '{resp.used_target}' in {entry['latency_ms']}ms — "
            f"GuruFocus is fully reachable from this IP."
        )
        return entry

    # Got a real, non-Cloudflare HTTP error — the network path is fine but
    # GuruFocus rejected the call (revoked key, quota, unsubscribed). That's a
    # credentials/quota problem, NOT the Cloudflare IP block.
    entry["verdict"] = "degraded"
    entry["reason"] = (
        f"Reached GuruFocus past Cloudflare (HTTP {resp.status_code}, not a block), but the "
        f"API rejected the call — likely a revoked key, quota, or subscription issue, not an IP block."
    )
    return entry


def _probe_source(src: dict) -> dict:
    try:
        if src["kind"] == "gurufocus":
            return _probe_gurufocus(src)
        if src["kind"] == "supabase":
            return _probe_supabase(src)
        return _probe_http(src)
    except Exception as e:  # noqa: BLE001 — a probe must never 500 the page
        entry = _base_entry(src, src.get("url"))
        entry["verdict"] = "unreachable"
        entry["reason"] = f"Probe crashed: {type(e).__name__}: {e}"
        return entry


def _egress_ip() -> dict:
    """The public IP this backend egresses from — "our IP" at the top of the
    page. Tries a few reflectors so one outage doesn't blind us."""
    import requests  # noqa: PLC0415

    reflectors = (
        "https://api.ipify.org?format=json",
        "https://ifconfig.me/all.json",
        "https://ifconfig.co/json",
    )
    for url in reflectors:
        try:
            r = requests.get(url, timeout=8, headers={"User-Agent": _USER_AGENT})
            if not r.ok:
                continue
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            ip = data.get("ip") or data.get("ip_addr")
            if ip:
                return {"ip": ip, "source": urlparse(url).hostname, "error": None}
        except Exception:  # noqa: BLE001
            continue
    return {"ip": None, "source": None, "error": "all egress-IP reflectors failed"}


def _gurufocus_circuit() -> dict:
    """Live curl_cffi ladder + circuit-breaker state — the "why" behind the
    GuruFocus verdict, independent of this run's probe."""
    from ingest._gurufocus_http import (  # noqa: PLC0415
        circuit_seconds_remaining,
        current_preferred_target,
        is_available,
        ladder,
        proxy_configured,
    )

    secs = circuit_seconds_remaining()
    return {
        "curl_cffi_available": is_available(),
        "circuit_open": secs > 0,
        "circuit_seconds_remaining": int(secs),
        "proxy_configured": proxy_configured(),
        "preferred_target": current_preferred_target() or None,
        "ladder": ladder(),
    }


async def run_diagnostics() -> dict:
    """Run the egress-IP lookup + every source probe concurrently (each is
    blocking I/O, so off-thread) and return the assembled report."""
    egress_task = asyncio.to_thread(_egress_ip)
    circuit_task = asyncio.to_thread(_gurufocus_circuit)
    source_tasks = [asyncio.to_thread(_probe_source, s) for s in _SOURCES]
    egress, circuit, *sources = await asyncio.gather(egress_task, circuit_task, *source_tasks)

    counts: dict[str, int] = {}
    for s in sources:
        counts[s["verdict"]] = counts.get(s["verdict"], 0) + 1

    return {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "egress": egress,
        "gurufocus_circuit": circuit,
        "sources": sources,
        "summary": counts,
    }

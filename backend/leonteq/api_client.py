"""HTTP client for Leonteq's /website-api/* endpoints.

Replaces the Playwright DOM scraper. Leonteq's underlyings SPA paints
its equity table from these endpoints — calling them directly skips
~60s of headless-Chromium pagination + modal/dropdown click strategies.

Endpoints used:
  POST /website-api/underlyings        → name, sector, industry, country,
                                          ticker, ric, sophisInternalId,
                                          financials, earnings, indexMemberships
  POST /website-api/feed/identifiers   → isin, sophisInternalId, ric,
                                          bloombergTicker, currency
                                          (body shape: `{"sophisInternalIds":
                                          [int, ...]}` — verified 2026-05-20
                                          after the SPA migrated from
                                          `{"identifiers": [...]}`)

Joining on `sophisInternalId` gives every field the universe template
needs, in 4–6 HTTP requests total at resultPerPage=500.

Bearer token: a 10-year SPA-client JWT minted by Leonteq's Keycloak
realm at `/website-api/auth/realms/website-api`. iat=2022-01-05,
exp=2032-01-04. It's the same token anonymous browser visitors get from
the SPA, so hardcoding it here doesn't leak anything (the SPA ships it
to every visitor). If the token ever rejects, the 401 branch in
`_post_with_retry` raises a clear "grab a fresh one from DevTools"
RuntimeError so the failure mode is obvious.

Fallback: setting `LEONTEQ_USE_PLAYWRIGHT=1` in the environment routes
`LeonteqTemplate.refresh()` back to the DOM scraper — kept in place as
a backup path in case the API shape ever changes.
"""
from __future__ import annotations

import logging
import time
from typing import Callable

import requests

_log = logging.getLogger(__name__)

# 10-year SPA-client JWT. Captured 2026-05-19, exp 2032-01-04.
_BEARER_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9."
    "eyJzdWIiOiJMZW9udGVxIFdlYnNpdGUgQ0giLCJhenAiOiJzcC5sZW9udGVxLmNvbSIsInJvbGVzIjpbXSwiaXNzIjoiaHR0cHM6Ly9zdHJ1Y3R1cmVkcHJvZHVjdHMubGVvbnRlcS5jb20vd2Vic2l0ZS1hcGkvYXV0aC9yZWFsbXMvd2Vic2l0ZS1hcGkiLCJuYW1lIjoiTEVPTlRFUSIsImV4cCI6MTk1NjczNzE4NCwiaWF0IjoxNjQxMzc3MTg0fQ."
    "pL3NHCftsdZgeyJaYE9FzY2PhhKpQPlwzo8i9Zd-7gchs8QAc3CdyA4e4SoFFcVelLVmny7UwoV8lfg3rWGmfNxmVrUzVgWvgncRS1L5OJlrb5lQ-XrWIxFUN2ifeDH78h4TRk44HIF5NLYr-985PfaUrcLrC-R67dWgYuwczmmkBFG0oWKk4ynXQRP9fFkd1aMWwGdAwv_toM3semchkDlakk6l8mboSQ6ALlfR0qLPqTMLhmjZaMdAkZdR2hWidWG6ILOpFlWt6B2I2Pdbwdo4K2jQAtML0k4KA1FX6zznhTLigyXWBoAzXOxIm1htEt-oiieCRs-LADhfB7e7dw"
)

_BASE_URL = "https://structuredproducts-ch.leonteq.com/website-api"
# Leonteq's /underlyings caps responses at 100 rows server-side
# regardless of requested resultPerPage (verified empirically: asked
# for 500, got 100). We still REQUEST 500 — the cap might be relaxed
# in the future, and asking for more than we'll get is harmless — but
# we advance `resultsOffset` by the actual returned count, not the
# requested page size.
_PAGE_SIZE = 500
_MAX_PAGES = 50      # Safety cap: 50 pages × 100 actual rows = 5000 rows.
_REQUEST_TIMEOUT = 30
_IDENTIFIERS_BATCH = 100  # /feed/identifiers caps input arrays to ~30 in
                          # the SPA's call; 100 works in practice (verified
                          # 2026-05-20).

# Same sanity floor as the scraper's: if we end with <1500 rows, raise
# instead of persisting a partial universe.
_MIN_EXPECTED_ROWS = 1500


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_BEARER_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Origin": "https://structuredproducts-ch.leonteq.com",
        "Referer": "https://structuredproducts-ch.leonteq.com/services/underlyings",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
        ),
    }


def _empty_filter_body(offset: int) -> dict:
    """The shape Leonteq's SPA POSTs — verified against the captured
    DevTools payload. Filter arrays empty = no filtering = every
    equity. Sort by name ASC matches what the UI shows by default."""
    return {
        "pagination": {"resultPerPage": _PAGE_SIZE, "resultsOffset": offset},
        "sort": [{"fieldName": "name", "sortOrder": "ASC"}],
        "countries": [],
        "currencies": [],
        "indices": [],
        "industries": [],
        "regions": [],
        "sectors": [],
        "omni": "",
    }


def _post_with_retry(url: str, body: dict, retries: int = 3) -> dict | list:
    """POST with simple exponential-backoff retry on 5xx + network
    errors. Raises on 401 with an actionable message (token rotation)
    and on non-2xx after the retry budget is exhausted."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            resp = requests.post(
                url, json=body, headers=_headers(), timeout=_REQUEST_TIMEOUT,
            )
            if resp.status_code == 401:
                raise RuntimeError(
                    f"Leonteq returned 401 from {url}. The hardcoded Bearer token "
                    f"has likely expired or been rotated; capture a fresh one from "
                    f"DevTools (Network → any /website-api/* request → request "
                    f"headers → 'authorization') and update _BEARER_TOKEN in "
                    f"backend/leonteq/api_client.py."
                )
            if resp.status_code >= 500:
                last_exc = RuntimeError(
                    f"Leonteq {url} → HTTP {resp.status_code}: {resp.text[:200]}"
                )
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_exc = e
            time.sleep(2 ** attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Leonteq {url} failed after {retries} retries (no exception)")


def fetch_all_underlyings(
    on_progress: Callable[[str, int | None], None] | None = None,
) -> list[dict]:
    """Every equity underlying from /website-api/underlyings. Each row
    carries name, sector, industry, country, ticker, ric,
    sophisInternalId + extras (description, financials, earnings).

    Paginates via `pagination.resultsOffset`. Leonteq caps responses to
    ~100 rows regardless of requested resultPerPage, so we advance the
    offset by the *actual* count returned per call, not the requested
    page size — otherwise we'd skip rows 100-499 between calls."""
    def emit(msg: str, pct: int | None = None) -> None:
        _log.info("[leonteq.api] %s", msg)
        if on_progress is not None:
            try:
                on_progress(msg, pct)
            except Exception:
                pass

    url = f"{_BASE_URL}/underlyings"
    all_rows: list[dict] = []
    offset = 0
    for page_idx in range(_MAX_PAGES):
        body = _empty_filter_body(offset)
        pct = 10 + min(35, int((page_idx + 1) * 35 / 20))
        emit(f"/underlyings: GET offset={offset} limit={_PAGE_SIZE}", pct)
        data = _post_with_retry(url, body)
        rows = data.get("underlyings") if isinstance(data, dict) else None
        if not rows:
            emit(f"/underlyings: empty response at offset={offset}, stopping.", None)
            break
        all_rows.extend(rows)
        emit(f"/underlyings: +{len(rows)} rows (total {len(all_rows)})", None)
        # Advance by actual returned count, not requested page size —
        # Leonteq caps at ~100 even when we ask for 500.
        offset += len(rows)
    return all_rows


def fetch_isins_for_sophis_ids(
    sophis_ids: list[int],
    on_progress: Callable[[str, int | None], None] | None = None,
) -> dict[int, str]:
    """Look up ISIN for each `sophisInternalId` via the
    /website-api/feed/identifiers endpoint. The endpoint isn't paginated
    — it's a lookup. Body shape is `{"sophisInternalIds": [int, ...]}`
    (the SPA migrated from `{"identifiers": [...]}` somewhere between
    its initial capture and 2026-05-20 — the old shape now 500s with
    a "NonEmpty" validation error from `FeedIdentifiersRequestValidator`).
    We batch by `_IDENTIFIERS_BATCH` to keep request bodies under ~10KB.
    Returns `{sophis_id: isin}` — ids without an isin field are omitted.

    Per-batch failures are isolated so a single 5xx doesn't lose every
    ISIN — we log the batch and continue."""
    def emit(msg: str, pct: int | None = None) -> None:
        _log.info("[leonteq.api] %s", msg)
        if on_progress is not None:
            try:
                on_progress(msg, pct)
            except Exception:
                pass

    url = f"{_BASE_URL}/feed/identifiers"
    isin_by_sid: dict[int, str] = {}
    unique_ids = [sid for sid in sophis_ids if sid is not None]
    total_batches = max(1, (len(unique_ids) + _IDENTIFIERS_BATCH - 1) // _IDENTIFIERS_BATCH)
    failed_batches = 0
    for batch_idx in range(total_batches):
        batch = unique_ids[batch_idx * _IDENTIFIERS_BATCH : (batch_idx + 1) * _IDENTIFIERS_BATCH]
        if not batch:
            break
        body = {"sophisInternalIds": batch}
        pct = 50 + min(35, int((batch_idx + 1) * 35 / total_batches))
        emit(
            f"/feed/identifiers: batch {batch_idx + 1}/{total_batches} "
            f"({len(batch)} ids)",
            pct,
        )
        try:
            data = _post_with_retry(url, body)
        except Exception as e:
            failed_batches += 1
            emit(
                f"/feed/identifiers: batch {batch_idx + 1} failed "
                f"({type(e).__name__}: {str(e)[:120]}); continuing.",
                None,
            )
            continue
        # Response is a bare array of identifier rows.
        rows = data if isinstance(data, list) else []
        for r in rows:
            sid = r.get("sophisInternalId")
            isin = r.get("isin")
            if sid is not None and isin:
                isin_by_sid[sid] = isin
    suffix = f" ({failed_batches} batch(es) failed)" if failed_batches else ""
    emit(f"Resolved {len(isin_by_sid)} ISINs for {len(unique_ids)} ids{suffix}.", 85)
    return isin_by_sid


def fetch_underlyings_with_isin(
    on_progress: Callable[[str, int | None], None] | None = None,
) -> list[dict]:
    """Fetch all underlyings + identifiers, join on sophisInternalId,
    return the same dict shape `scrape_underlyings` produced so
    `LeonteqTemplate.refresh` doesn't need to change:

        [{name, ticker, isin, sector, industry, country, ric, sophisInternalId}, ...]

    `isin` is None when no identifier row matches (rare; logged as a
    count). Raises `RuntimeError` if the joined result has fewer than
    `_MIN_EXPECTED_ROWS` — same loud-fail behavior the scraper has, so a
    silent breakage downstream is impossible."""
    def emit(msg: str, pct: int | None = None) -> None:
        _log.info("[leonteq.api] %s", msg)
        if on_progress is not None:
            try:
                on_progress(msg, pct)
            except Exception:
                pass

    emit("Fetching Leonteq underlyings via API (replaces Playwright scrape)…", 5)
    underlyings = fetch_all_underlyings(on_progress)
    sophis_ids = [u.get("sophisInternalId") for u in underlyings]
    # ISIN lookup is best-effort — the /feed/identifiers request shape
    # is undocumented and may reject our body with HTTP 500. The
    # universe itself doesn't need ISIN (sector + industry come from
    # /underlyings); if the lookup fails we proceed with isin=None on
    # every row and log a warning so the user knows to update the
    # request shape.
    try:
        isin_by_sid = fetch_isins_for_sophis_ids(sophis_ids, on_progress)
    except Exception as e:
        emit(
            f"ISIN lookup failed ({type(e).__name__}: {e}); proceeding without "
            f"ISINs. The universe is still complete — name/sector/industry/"
            f"ticker all come from /underlyings, only ISIN is missing.",
            None,
        )
        isin_by_sid = {}

    out: list[dict] = []
    missing_isin = 0
    for u in underlyings:
        sid = u.get("sophisInternalId")
        isin = isin_by_sid.get(sid) if sid is not None else None
        if isin is None:
            missing_isin += 1
        out.append({
            "name": u.get("name") or "",
            "ticker": u.get("ticker") or "",
            "isin": isin,
            "sector": u.get("sector"),
            "industry": u.get("industry"),
            # Bonus fields beyond what the scraper used to emit; passed
            # through for downstream consumers that want them.
            "country": u.get("country"),
            "ric": u.get("ric"),
            "sophisInternalId": sid,
        })

    if missing_isin:
        emit(
            f"{missing_isin} underlyings had no matching ISIN in /feed/identifiers "
            f"(still indexed by ticker).",
            None,
        )

    if len(out) < _MIN_EXPECTED_ROWS:
        raise RuntimeError(
            f"Leonteq API returned only {len(out)} underlyings; expected >= "
            f"{_MIN_EXPECTED_ROWS}. Check `pagination.resultsOffset` advancement "
            f"or whether the endpoint shape changed."
        )

    emit(f"Joined {len(out)} underlyings ({len(isin_by_sid)} ISINs resolved).", 90)
    return out

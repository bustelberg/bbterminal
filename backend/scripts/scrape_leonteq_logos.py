"""Scrape company logos + names for every Leonteq equity underlying.

Leonteq's underlyings API gives us name + ticker + ISIN + the company's
own `website`, but NO logo URL. Leonteq's SPA renders each logo from
**Brandfetch**, keyed by ISIN — so that's our primary source, and the
domain-based favicon services are only a fallback:

  0. Brandfetch    https://cdn.brandfetch.io/{ISIN}/icon/...?c={client}
                   (PRIMARY — exactly what Leonteq's own site uses. Keyed
                   by ISIN, so it has Asian listings that domain-based
                   services miss; 400px. Needs `Accept: image/*` or it
                   serves its HTML docs page instead.)

  Fallback chain (used when a row has no ISIN, or Brandfetch 404s),
  by website domain, apex first (so `ir.united.com` -> `united.com`):

  1. logo.dev      https://img.logo.dev/{domain}?token=...   (only if
                   LOGODEV_TOKEN is set — free token at logo.dev)
  2. DuckDuckGo    https://icons.duckduckgo.com/ip3/{domain}.ico
  3. Google S2     https://www.google.com/s2/favicons?domain={domain}&sz=256
  4. site icons    https://{domain}/apple-touch-icon.png then /favicon.ico

(Clearbit's Logo API was shut down by HubSpot and is intentionally not
used — it 404s on everything now.)

When every source misses (Brandfetch 404 + no favicon — ~28 companies,
mostly Japanese/Chinese listings), we generate a first-letter tile
badge, the same fallback Leonteq itself renders. Tagged
`logo_source="letter-placeholder"` / `status="letter"` in the manifest
so the upload step can tell real logos from generated ones. Disable
with --no-letter-fallback.

Validation is by Pillow decode + a minimum pixel dimension — NOT a byte
floor. An earlier 512-byte floor wrongly rejected legitimately tiny
16px favicons (amgen 274 B, vodafone 444 B); the real "not found"
placeholders from DuckDuckGo/Google come back as HTTP 404 instead, so
the status check already filters them. Transient throttling under a
1,700-request bulk run is absorbed by a small retry on network / 5xx /
429 errors.

Each logo lands in `{out}/images/{sophisInternalId}.png` and every row
is recorded in `{out}/companies.json` — a manifest the later upload
script reads to pair each image file with its company name.

`sophisInternalId` is the filename key because it's the only field
guaranteed unique + stable across Leonteq runs (tickers collide across
exchanges; names have punctuation).

Every logo is normalized to PNG via Pillow (DuckDuckGo serves .ico, so
we decode + re-encode). Pillow + tldextract (for apex extraction) are
pulled in ephemerally with `uv run --with`, so neither touches the
backend's own dependencies.

Usage (from backend/, so the leonteq package imports cleanly):

    uv run --with pillow --with tldextract python scripts/scrape_leonteq_logos.py            # full run
    uv run --with pillow --with tldextract python scripts/scrape_leonteq_logos.py --limit 20 # quick test

Re-running is cheap: images already on disk are skipped unless --force —
so a plain re-run only retries the previous misses. Exits non-zero
(with a loud ALERT listing the companies) when the logo-not-found count
exceeds --alert-misses (default 3).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
import tldextract
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont  # ephemeral via `uv run --with pillow`.

# Make the backend package root importable regardless of cwd (Python
# only puts this script's own dir, scripts/, on sys.path).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from leonteq.api_client import (  # noqa: E402
    fetch_all_underlyings,
    fetch_isins_for_sophis_ids,
)

# Fast favicon-service timeout vs slower direct-site fetch timeout (a
# company's own server can be slow or hang — keep it short so one bad
# site doesn't stall the run).
_TIMEOUT = 12
_DIRECT_TIMEOUT = 4
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
    ),
    # Brandfetch's CDN serves its HTML docs page (not an image) unless
    # the Accept header asks for an image — without this the primary
    # source silently returns a 444 KB HTML blob. Harmless for the
    # favicon services too (they always return images).
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

# Brandfetch is what Leonteq's own SPA uses to render company logos:
# `https://cdn.brandfetch.io/{ISIN}/icon/...?c={CLIENT_ID}`. It's keyed
# by ISIN (so it covers Asian listings that domain-based favicon
# services miss) and returns 400px logos. The client id is Brandfetch's
# publishable key, shipped to every visitor in Leonteq's page source
# (same nature as the hardcoded Leonteq bearer token in api_client.py).
# `fallback/404` makes Brandfetch return a clean HTTP 404 when it has no
# logo, so misses are unambiguous.
_BRANDFETCH_CLIENT_ID = "1idTr2Xodj3MwMWZYJs"
# Reject anything smaller than this on either side after decode — kills
# 1x1 tracking pixels without rejecting real 16px favicons. We validate
# by pixel dimension, not byte count (see module docstring).
_MIN_DIM = 16
# Tiny floor just to skip empty / truncated bodies before we hand them
# to Pillow — NOT a quality gate.
_MIN_BODY_BYTES = 70

# Apex (registrable-domain) extractor. `suffix_list_urls=()` forces
# tldextract's bundled Public Suffix List snapshot — no network call,
# and it correctly stops at multi-label suffixes (honda.co.jp stays
# honda.co.jp; ir.united.com → united.com).
_extract = tldextract.TLDExtract(suffix_list_urls=())


def _domain(website: str | None) -> str | None:
    """Bare registrable host from a website URL: 'https://www.2-g.com/x'
    -> '2-g.com'. Returns None when there's no usable host."""
    if not website:
        return None
    raw = website.strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "https://" + raw
    host = (urlparse(raw).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


# Muted, professional tile colours for the generated letter badges
# (the fallback Leonteq itself shows when Brandfetch has no logo). One
# is picked deterministically per company so the same name always gets
# the same colour.
_LETTER_BG_PALETTE = [
    (92, 107, 192), (66, 165, 245), (38, 166, 154), (102, 187, 106),
    (236, 64, 122), (171, 71, 188), (126, 87, 194), (255, 167, 38),
    (141, 110, 99), (120, 144, 156), (38, 198, 218), (255, 112, 67),
]
# Windows-first font search for the badge letter — falls back to
# Pillow's built-in bitmap font if none load (badge still renders, just
# smaller). arialbd/segoeuib are bold + always present on Windows.
_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\segoeuib.ttf",
    r"C:\Windows\Fonts\arial.ttf",
    "DejaVuSans-Bold.ttf",
    "DejaVuSans.ttf",
]


def _load_font(size: int):
    for path in _FONT_CANDIDATES:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _letter_placeholder(name: str, size: int = 400) -> bytes:
    """A 400px solid tile with the company's first letter centered in
    white — the same kind of fallback badge Leonteq shows when Brandfetch
    has no logo. Colour is deterministic from the name."""
    letter = next((c for c in name if c.isalnum()), "?").upper()
    bg = _LETTER_BG_PALETTE[sum(ord(c) for c in name) % len(_LETTER_BG_PALETTE)]
    img = Image.new("RGBA", (size, size), bg + (255,))
    draw = ImageDraw.Draw(img)
    font = _load_font(int(size * 0.55))
    # Center using the glyph's actual bounding box (accounts for the
    # font's internal bearings so the letter is optically centered).
    box = draw.textbbox((0, 0), letter, font=font)
    w, h = box[2] - box[0], box[3] - box[1]
    draw.text(
        ((size - w) / 2 - box[0], (size - h) / 2 - box[1]),
        letter, font=font, fill=(255, 255, 255, 255),
    )
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _apex(domain: str | None) -> str | None:
    """Registrable apex of a domain: 'ir.united.com' -> 'united.com',
    'honda.co.jp' -> 'honda.co.jp' (unchanged — co.jp is a public
    suffix). Returns None when the domain has no registrable part."""
    if not domain:
        return None
    ext = _extract(domain)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return None


def _to_png(data: bytes) -> bytes | None:
    """Convert any raster image (ico/jpg/gif/webp/png) to PNG bytes.
    ICO files often pack several sizes — we pick the largest. Returns
    None if the bytes aren't a decodable raster image (SVG / HTML error
    page) or the image is below `_MIN_DIM` on either side (1x1 pixels)."""
    try:
        img = Image.open(BytesIO(data))
        # ICO carries multiple resolutions; select the biggest so we
        # don't downscale to a 16px favicon when a 256px frame exists.
        if img.format == "ICO":
            if getattr(img, "ico", None) is not None:
                largest = max(img.ico.sizes())
                img = img.ico.getimage(largest)
        if min(img.size) < _MIN_DIM:
            return None
        img = img.convert("RGBA")
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    except Exception:
        return None


# Status codes worth one retry — transient throttling / server hiccups
# under bulk load. 404 is definitive (the favicon isn't there) and is
# NOT retried.
_RETRY_STATUS = {429, 500, 502, 503, 504}


def _try_download(
    url: str, session: requests.Session, timeout: int, retries: int
) -> bytes | None:
    """GET an image URL and return PNG bytes, or None on 404 /
    non-image / undecodable responses. Retries up to `retries` times on
    a connection error or a transient 5xx/429 — but NEVER on a timeout:
    a timeout means the server is hanging, and retrying just doubles the
    wait (this was the bulk-run slowdown). Direct-site fetches pass
    retries=0 for the same reason."""
    for attempt in range(retries + 1):
        try:
            resp = session.get(
                url, headers=_HEADERS, timeout=timeout, allow_redirects=True
            )
        except requests.exceptions.Timeout:
            return None
        except requests.RequestException:
            if attempt < retries:
                time.sleep(0.5)
                continue
            return None
        if resp.status_code in _RETRY_STATUS and attempt < retries:
            time.sleep(0.6)
            continue
        if resp.status_code != 200:
            return None
        if "image" not in resp.headers.get("Content-Type", "").lower():
            return None
        if len(resp.content) < _MIN_BODY_BYTES:
            return None
        return _to_png(resp.content)
    return None


def _sources_for(
    domain: str, logodev_token: str | None
) -> list[tuple[str, str, int, int]]:
    """(source-label, url, timeout, retries) chain for one domain,
    best-first. Favicon services get one retry (throttling-prone);
    direct-site fetches get zero (timeout-prone, retry wouldn't help)."""
    srcs: list[tuple[str, str, int, int]] = []
    if logodev_token:
        srcs.append((
            "logo.dev",
            f"https://img.logo.dev/{domain}?token={logodev_token}&size=256&format=png",
            _TIMEOUT, 1,
        ))
    srcs.append(("duckduckgo", f"https://icons.duckduckgo.com/ip3/{domain}.ico", _TIMEOUT, 1))
    srcs.append((
        "google-favicon",
        f"https://www.google.com/s2/favicons?domain={domain}&sz=256",
        _TIMEOUT, 1,
    ))
    # Direct site fetches last — slower and more likely to hang, but
    # they catch domains no favicon service indexes (e.g. axa.com).
    # Short timeout, no retry, so a hanging server costs at most
    # _DIRECT_TIMEOUT once.
    srcs.append(("site-apple-touch", f"https://{domain}/apple-touch-icon.png", _DIRECT_TIMEOUT, 0))
    srcs.append(("site-favicon", f"https://{domain}/favicon.ico", _DIRECT_TIMEOUT, 0))
    return srcs


def _brandfetch_url(isin: str) -> str:
    return (
        f"https://cdn.brandfetch.io/{isin}/icon/fallback/404/h/400/w/400"
        f"?c={_BRANDFETCH_CLIENT_ID}"
    )


def _fetch_logo(
    isin: str | None,
    domain: str | None,
    session: requests.Session,
    logodev_token: str | None,
) -> tuple[bytes, str] | None:
    """Resolve a logo, best source first. Returns (png_bytes,
    source-label) on the first hit, or None if every source misses.

    1. Brandfetch by ISIN — Leonteq's own source. Keyed by ISIN, so it
       covers Asian listings domain-based services miss; 400px quality.
    2. Domain favicon chain (logo.dev/DuckDuckGo/Google/site files),
       apex first — the fallback for rows with no ISIN or where
       Brandfetch has no logo."""
    if isin:
        png = _try_download(_brandfetch_url(isin), session, _TIMEOUT, 1)
        if png:
            return png, "brandfetch"

    if not domain:
        return None

    apex = _apex(domain)
    candidates: list[tuple[str, bool]] = []
    if apex and apex != domain:
        candidates.append((apex, True))
    candidates.append((domain, False))

    for cand, is_apex in candidates:
        for source, url, timeout, retries in _sources_for(cand, logodev_token):
            png = _try_download(url, session, timeout, retries)
            if png:
                return png, (f"{source}+apex" if is_apex else source)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape Leonteq company logos + names.")
    ap.add_argument(
        "--out",
        default="leonteq_logos",
        help="Output folder (default: ./leonteq_logos under backend/).",
    )
    ap.add_argument("--limit", type=int, default=0, help="Only process first N (0 = all).")
    ap.add_argument(
        "--force", action="store_true", help="Re-download logos already on disk."
    )
    ap.add_argument(
        "--alert-misses",
        type=int,
        default=3,
        help="Print a loud ALERT if logo-not-found count exceeds this (default 3).",
    )
    ap.add_argument(
        "--letter-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Generate a first-letter tile badge when no real logo is found "
        "(like Leonteq's own fallback). On by default; --no-letter-fallback to disable.",
    )
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    img_dir = out_dir / "images"
    img_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output folder: {out_dir}", flush=True)

    print("Fetching Leonteq underlyings (name + website) via API…", flush=True)
    rows = fetch_all_underlyings()
    if args.limit:
        rows = rows[: args.limit]
    print(f"Got {len(rows)} companies.", flush=True)

    # Brandfetch (the primary logo source) is keyed by ISIN, so resolve
    # ISINs for every row via Leonteq's /feed/identifiers endpoint.
    # Best-effort: if the lookup fails entirely we fall back to the
    # domain-based favicon chain for everyone.
    print("Resolving ISINs (Leonteq /feed/identifiers) for Brandfetch lookup…", flush=True)
    try:
        isin_by_sid = fetch_isins_for_sophis_ids(
            [r.get("sophisInternalId") for r in rows]
        )
    except Exception as e:
        print(f"  ISIN lookup failed ({type(e).__name__}: {e}); using favicons only.", flush=True)
        isin_by_sid = {}
    print(f"Resolved {len(isin_by_sid)} ISINs.\n", flush=True)

    logodev_token = os.environ.get("LOGODEV_TOKEN")
    src_note = "Primary: Brandfetch by ISIN (Leonteq's source). Fallback: "
    src_note += "logo.dev + " if logodev_token else ""
    src_note += "DuckDuckGo + Google + site favicons."
    print(src_note, flush=True)

    session = requests.Session()
    manifest: list[dict] = []
    counts = {"saved": 0, "skipped_existing": 0, "letter": 0, "no_logo": 0}

    for i, r in enumerate(rows, 1):
        sid = r.get("sophisInternalId")
        name = (r.get("name") or "").strip()
        ticker = (r.get("ticker") or "").strip()
        website = r.get("website")
        domain = _domain(website)
        isin = isin_by_sid.get(sid) if sid is not None else None

        entry: dict = {
            "sophisInternalId": sid,
            "name": name,
            "ticker": ticker,
            "ric": r.get("ric"),
            "isin": isin,
            "website": website,
            "domain": domain,
            "logo_file": None,
            "logo_source": None,
            "status": None,
        }

        if sid is None:
            entry["status"] = "no_id"
            manifest.append(entry)
            continue

        # Skip if we already have this logo on disk (we only ever write .png).
        png_path = img_dir / f"{sid}.png"
        if png_path.exists() and not args.force:
            entry["logo_file"] = png_path.name
            entry["status"] = "skipped_existing"
            counts["skipped_existing"] += 1
            manifest.append(entry)
        else:
            got = _fetch_logo(isin, domain, session, logodev_token)
            if got is None and args.letter_fallback:
                # No real logo anywhere — generate the first-letter tile,
                # the same fallback Leonteq shows.
                got = (_letter_placeholder(name or "?"), "letter-placeholder")
            if got is None:
                entry["status"] = "no_logo"
                counts["no_logo"] += 1
                # One streaming line per company we actually hit the
                # network for (skips are silent) — so the run shows live
                # activity instead of looking hung.
                print(
                    f"  - [{i}/{len(rows)}] MISS  {name} "
                    f"(isin={isin or '-'} domain={domain or '-'})",
                    flush=True,
                )
            else:
                data, source = got
                png_path.write_bytes(data)
                entry["logo_file"] = png_path.name
                entry["logo_source"] = source
                if source == "letter-placeholder":
                    entry["status"] = "letter"
                    counts["letter"] += 1
                    print(f"  - [{i}/{len(rows)}] letter {name}  (no logo on Brandfetch)", flush=True)
                else:
                    entry["status"] = "saved"
                    counts["saved"] += 1
                    print(f"  - [{i}/{len(rows)}] saved {name}  -> {source}", flush=True)
            manifest.append(entry)

        if i % 50 == 0 or i == len(rows):
            print(
                f"[{i}/{len(rows)}] saved={counts['saved']} "
                f"skip={counts['skipped_existing']} "
                f"letter={counts['letter']} no_logo={counts['no_logo']}",
                flush=True,
            )

    manifest_path = out_dir / "companies.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nDone. Manifest: {manifest_path}")
    print(
        f"Real logos: {counts['saved']} | already had: {counts['skipped_existing']} | "
        f"letter badges: {counts['letter']} | no logo at all: {counts['no_logo']}"
    )

    if counts["no_logo"] > args.alert_misses:
        missed = [
            f"  - {e['name']} (isin={e.get('isin') or '-'} domain={e['domain']})"
            for e in manifest
            if e["status"] == "no_logo"
        ]
        bar = "!" * 60
        print(f"\n{bar}")
        print(
            f"ALERT: {counts['no_logo']} companies missed a logo — that's more "
            f"than the threshold of {args.alert_misses}."
        )
        print("Missed companies:")
        print("\n".join(missed))
        print(bar)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

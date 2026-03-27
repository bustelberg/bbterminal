# src\quick_insight\ingest\gurufocus\utils.py

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from string import Formatter
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
import pandas as pd
from quick_insight.config.config import settings
import numpy as np
from typing import Any, Union, Optional
import re

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX"}


# ---------------------------------------------------------------------------
# Outcome enum
# ---------------------------------------------------------------------------

from enum import Enum

class GFOutcome(str, Enum):
    OK             = "OK"
    SKIP_ENTRY     = "SKIP_ENTRY"
    BLOCK_EXCHANGE = "BLOCK_EXCHANGE"


# ---------------------------------------------------------------------------
# Symbol / path helpers
# ---------------------------------------------------------------------------

def cache_company_key(primary_ticker: str, primary_exchange: str) -> str:
    t  = (primary_ticker  or "").strip().upper().replace(":", "_")
    ex = (primary_exchange or "").strip().upper().replace(":", "_")
    return f"{ex}_{t}"


def normalize_base_url(base: str) -> str:
    base = base.strip().rstrip("/")
    if base.endswith("/data"):
        base = base[: -len("/data")]
    return base.rstrip("/")


def mask_url(url: str) -> str:
    return url.replace(settings.gurufocus_api_key, "DUMMY_API_KEY")


def build_symbol(primary_ticker: str, primary_exchange: str) -> str:
    ex = (primary_exchange or "").upper()
    if ex in US_EXCHANGES:
        return primary_ticker
    return f"{primary_exchange}:{primary_ticker}"


def company_cache_dir(primary_ticker: str, primary_exchange: str) -> Path:
    d = Path(settings.gurufocus_dir) / cache_company_key(primary_ticker, primary_exchange)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def read_http_body(err: HTTPError) -> str:
    try:
        return err.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def extract_gf_error_message(body: str) -> str | None:
    try:
        obj = json.loads(body)
        if isinstance(obj, dict):
            err = obj.get("error") or obj.get("message")
            if isinstance(err, str) and err.strip():
                return err.strip()
    except Exception:
        pass
    return None


def _encode_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((
        parts.scheme,
        parts.netloc,
        quote(parts.path,  safe="/:%"),
        quote(parts.query, safe="=&:%"),
        parts.fragment,
    ))


# ---------------------------------------------------------------------------
# Response dataclass + request helper
# ---------------------------------------------------------------------------

@dataclass
class GFResponse:
    ok: bool
    status_code: int | None
    data: Any
    gf_error: str | None

    @property
    def is_unsubscribed_region(self) -> bool:
        if not self.gf_error:
            return False
        return "unsubscribed region" in self.gf_error.lower()


def request_json(url: str, *, timeout: int = 30) -> GFResponse:
    url = _encode_url(url)
    print("[TRY]", mask_url(url))

    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})

    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return GFResponse(ok=True, status_code=200, data=json.loads(raw) if raw else None, gf_error=None)

    except HTTPError as e:
        body   = read_http_body(e)
        gf_msg = extract_gf_error_message(body)
        print(f"[HTTP {e.code}] {e.reason}")
        if gf_msg:   print(f"[GF ERROR] {gf_msg}")
        elif body:   print("Body preview:", body[:400])
        return GFResponse(ok=False, status_code=e.code, data=None, gf_error=gf_msg)

    except URLError as e:
        print(f"[URL ERROR] {e}")
        return GFResponse(ok=False, status_code=None, data=None, gf_error=str(e))

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return GFResponse(ok=False, status_code=None, data=None, gf_error=str(e))


# ---------------------------------------------------------------------------
# Cache writer
# ---------------------------------------------------------------------------

def write_json_cache(cache_path: Path, data: Any) -> None:
    cache_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

def read_json_cache(cache_path: Path) -> Any:
    with cache_path.open("r", encoding="utf-8") as f:
        return json.load(f)
# ---------------------------------------------------------------------------
# Generic endpoint spec + fetcher
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GFSpec:
    """
    Describes any GuruFocus endpoint.

    Attributes
    ----------
    path_template : str
        URL path after /public/user/{token}/, with {placeholders}.
        Placeholders are filled from `params`; remaining params become
        query-string arguments.

        Examples
        --------
        "stock/{symbol}/insider"
        "stock/{symbol}/financials"
        "stock/{symbol}/segments_data"
        "stock/{symbol}/{indicator_key}"
        "stock/indicators"

    cache_path : Path
        Full path (including filename) where the response JSON is written.
        Caller is responsible for uniqueness per param combination.

    params : dict[str, str]
        Placeholder values and/or query-string parameters.

    block_on_unsubscribed : bool
        True  → 403/unsubscribed blocks the entire exchange.
        False → treated as a soft SKIP_ENTRY for this symbol only.
    """

    path_template: str
    cache_path: Path
    params: dict[str, str] = field(default_factory=dict)
    block_on_unsubscribed: bool = False


def fetch_guru(
    spec: GFSpec,
    *,
    use_cache: bool = True,
    timeout: int = 60,
    return_data: bool = False,
) -> tuple[GFOutcome, Union[Path, dict[str, Any], None]]:
    """
    Generic fetcher for any GuruFocus endpoint described by a GFSpec.

    Parameters
    ----------
    return_data : bool
        If True, return resp.data instead of writing/returning a file path.

    Returns
    -------
    (GFOutcome.OK, Path)      - default success (file written)
    (GFOutcome.OK, dict)      - if return_data=True
    (GFOutcome.SKIP_ENTRY, None)
    (GFOutcome.BLOCK_EXCHANGE, None)
    """

    # ------------------------------------------------------------------
    # Cache short-circuit
    # ------------------------------------------------------------------
    if use_cache and spec.cache_path.exists():
        print(f"[CACHE] → {spec.cache_path}")

        if return_data:
            # Load JSON and return it instead of path
            data = read_json_cache(spec.cache_path)
            return GFOutcome.OK, data

        return GFOutcome.OK, spec.cache_path

    # ------------------------------------------------------------------
    # Build URL
    # ------------------------------------------------------------------
    path_keys = {
        fname
        for _, fname, _, _ in Formatter().parse(spec.path_template)
        if fname is not None
    }

    path_params = {k: v for k, v in spec.params.items() if k in path_keys}
    query_params = {k: v for k, v in spec.params.items() if k not in path_keys}

    path = spec.path_template.format(**path_params)
    base = normalize_base_url(settings.gurufocus_base_url)
    token = settings.gurufocus_api_key

    url = f"{base}/public/user/{token}/{path}"
    if query_params:
        url = f"{url}?{urlencode(query_params)}"

    # ------------------------------------------------------------------
    # Request
    # ------------------------------------------------------------------
    resp = request_json(url, timeout=timeout)

    if resp.is_unsubscribed_region:
        outcome = (
            GFOutcome.BLOCK_EXCHANGE
            if spec.block_on_unsubscribed
            else GFOutcome.SKIP_ENTRY
        )
        print(f"[{outcome}] unsubscribed region → {path}")
        return outcome, None

    if not resp.ok:
        print(
            f"[SKIP_ENTRY] {path} failed "
            f"(status={resp.status_code}, gf_error={resp.gf_error})"
        )
        return GFOutcome.SKIP_ENTRY, None

    # ------------------------------------------------------------------
    # Return data directly (no file write)
    # ------------------------------------------------------------------
    if return_data:
        return GFOutcome.OK, resp.data

    # ------------------------------------------------------------------
    # Default: write to cache
    # ------------------------------------------------------------------
    spec.cache_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_cache(spec.cache_path, resp.data)

    print(f"[SAVE] → {spec.cache_path}")
    return GFOutcome.OK, spec.cache_path








def extract_min_days_from_spec(spec: GFSpec) -> Optional[int]:
    """
    Extract the trailing `_min_days` integer from the actual resolved cache file.

    Examples
    --------
    analyst_estimate.json      -> None
    analyst_estimate_91.json   -> 91

    Returns
    -------
    int  -> extracted min_days
    None -> if no suffix found
    """
    cache_path = resolve_cache_file_path(spec)   # <- important fix
    stem = cache_path.stem

    match = re.search(r"_(\d+)$", stem)
    if not match:
        return None

    return int(match.group(1))

def cache_file_exists(spec: GFSpec) -> bool:
    """
    Check whether exactly one cache file exists for a given GFSpec.

    Supports filenames like:
        base.json
        base_91.json

    Rules
    -----
    - Uses spec.cache_path as the base
    - Matches files with same stem prefix
    - Returns:
        True  -> exactly one file found
        False -> no file found
    - Raises:
        ValueError if multiple files are found

    Returns
    -------
    bool
    """

    base_path: Path = spec.cache_path
    cache_dir = base_path.parent
    base_stem = base_path.stem  # e.g. "analyst_estimate"

    files = list(cache_dir.glob(f"{base_stem}*.json"))

    if not files:
        return False

    if len(files) > 1:
        raise ValueError(
            f"Multiple cache files found for {base_stem}: {[f.name for f in files]}"
        )

    return True





def build_cache_path_with_min_days(spec: GFSpec, min_days: int) -> Path:
    """
    Return a cache path with _{min_days} appended before the file extension.

    Examples
    --------
    analyst_estimate.json      -> analyst_estimate_91.json
    analyst_estimate_88.json   -> analyst_estimate_91.json

    Parameters
    ----------
    spec : GFSpec
    min_days : int

    Returns
    -------
    Path
    """

    base_path: Path = spec.cache_path

    # Remove existing _<digits> suffix if present
    clean_stem = re.sub(r"_\d+$", "", base_path.stem)

    new_name = f"{clean_stem}_{min_days}{base_path.suffix}"

    return base_path.with_name(new_name)


def resolve_cache_file_path(spec: GFSpec) -> Path:
    """
    Resolve the actual cache file path for a GFSpec.

    Matches exactly:
        base.json
        base_<digits>.json

    Examples
    --------
    base_stem = "indicator__price"
    matches:
        indicator__price.json
        indicator__price_1.json
    does not match:
        indicator__price_to_gf_value_1.json

    Returns
    -------
    Path

    Raises
    ------
    FileNotFoundError
        If no cache file exists
    ValueError
        If multiple candidates exist
    """
    base_path: Path = spec.cache_path
    cache_dir = base_path.parent
    base_stem = base_path.stem
    suffix = base_path.suffix

    pattern = re.compile(
        rf"^{re.escape(base_stem)}(?:_(\d+))?{re.escape(suffix)}$"
    )

    files = [
        p for p in cache_dir.iterdir()
        if p.is_file() and pattern.match(p.name)
    ]

    if not files:
        raise FileNotFoundError(
            f"No cache file found for {base_stem} in {cache_dir}"
        )

    if len(files) > 1:
        raise ValueError(
            f"Multiple cache files found for {base_stem}: {[f.name for f in files]}"
        )

    return files[0]
from __future__ import annotations

import calendar
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from supabase import Client

_HARD_STOP = (2025, 8)
_BUCKET = "longequity-raw"
_XLSX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)


@dataclass(frozen=True)
class MonthSpec:
    year: int
    month: int

    @property
    def yyyymm(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class LongEquityFormat:
    filename: str
    url: str


class RemoteNotFound(RuntimeError):
    pass


def _current_month(now: datetime | None = None) -> MonthSpec:
    if now is None:
        now = datetime.now(timezone.utc)
    return MonthSpec(year=now.year, month=now.month)


def _prev_month(spec: MonthSpec) -> MonthSpec:
    month = spec.month - 1
    if month == 0:
        return MonthSpec(year=spec.year - 1, month=12)
    return MonthSpec(year=spec.year, month=month)


def _is_before_hard_stop(spec: MonthSpec) -> bool:
    return (spec.year, spec.month) < _HARD_STOP


def _format_month(spec: MonthSpec) -> LongEquityFormat:
    base_url = os.environ["LONGEQUITY_BASE_URL"]
    filename_template = os.environ["LONGEQUITY_FILENAME_TEMPLATE"]
    month_name_capitalized = calendar.month_name[spec.month]
    filename = filename_template.format(
        year=spec.year,
        month=spec.month,
        month_name_capitalized=month_name_capitalized,
    )
    url = base_url.rstrip("/").format(year=spec.year, month=f"{spec.month:02d}") + "/" + filename
    return LongEquityFormat(filename=filename, url=url)


def _ensure_bucket(supabase: Client) -> None:
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass  # already exists


def _fetch_from_storage(supabase: Client, filename: str) -> bytes | None:
    """Return file bytes if found in Storage, otherwise None."""
    try:
        return supabase.storage.from_(_BUCKET).download(filename)
    except Exception:
        return None


def _fetch_from_url(url: str, timeout: int) -> bytes:
    """Download from remote URL. Raises RemoteNotFound on 404."""
    with requests.get(url, stream=True, timeout=timeout) as r:
        if r.status_code == 404:
            raise RemoteNotFound(f"404: {url}")
        r.raise_for_status()
        return r.content


def _upload_to_storage(supabase: Client, filename: str, content: bytes) -> None:
    try:
        supabase.storage.from_(_BUCKET).upload(
            filename,
            content,
            file_options={"content-type": _XLSX_CONTENT_TYPE},
        )
    except Exception as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg and "409" not in msg:
            raise


def _get_file(supabase: Client, fmt: LongEquityFormat, timeout: int) -> bytes:
    """
    Returns file bytes.
    Checks Supabase Storage first; on miss, downloads from URL and uploads to Storage.
    """
    cached = _fetch_from_storage(supabase, fmt.filename)
    if cached is not None:
        return cached

    content = _fetch_from_url(fmt.url, timeout)
    _upload_to_storage(supabase, fmt.filename, content)
    return content


def check_latest_available_month(
    *,
    now: datetime | None = None,
    timeout: int = 10,
    max_checks: int = 4,
) -> MonthSpec | None:
    """
    Walk backwards from current month (up to max_checks months),
    checking remote URL availability without downloading.
    Returns the most recent MonthSpec that exists, or None.
    """
    spec = _current_month(now)
    for _ in range(max_checks):
        if _is_before_hard_stop(spec):
            return None
        fmt = _format_month(spec)
        try:
            with requests.get(fmt.url, stream=True, timeout=timeout) as r:
                if r.status_code == 200:
                    return spec
        except Exception:
            pass
        spec = _prev_month(spec)
    return None


def acquire_raw_longequity_backfill(
    supabase: Client,
    *,
    timeout: int = 60,
    now: datetime | None = None,
    verbose: bool = True,
) -> list[tuple[str, bytes]]:
    """
    Walk backwards from the current month.
    For each month: check Supabase Storage first, fall back to remote URL.
    Stops after two consecutive 404s or when reaching the hard-stop month.
    Returns list of (filename, bytes) ordered [most recent → oldest].
    """
    _ensure_bucket(supabase)

    results: list[tuple[str, bytes]] = []
    spec = _current_month(now)
    consecutive_404 = 0

    while True:
        if _is_before_hard_stop(spec):
            if verbose:
                print(f"[acquire] hard stop ({_HARD_STOP[0]}-{_HARD_STOP[1]:02d}), stopping.")
            break

        fmt = _format_month(spec)
        try:
            content = _get_file(supabase, fmt, timeout)
            results.append((fmt.filename, content))
            consecutive_404 = 0
            if verbose:
                print(f"[acquire] ok: {spec.yyyymm} -> {fmt.filename}")
        except RemoteNotFound:
            consecutive_404 += 1
            if verbose:
                print(f"[acquire] 404: {spec.yyyymm} -> {fmt.filename}")
            if consecutive_404 >= 2:
                if verbose:
                    print("[acquire] two consecutive 404s, stopping.")
                break

        spec = _prev_month(spec)

    return results

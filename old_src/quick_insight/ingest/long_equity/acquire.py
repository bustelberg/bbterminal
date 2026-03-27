from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from quick_insight.config.config import settings


TZ = ZoneInfo(settings.timezone)

# Hard stop (oldest month you want)
_HARD_STOP = (2025, 8)  # (year, month)


@dataclass(frozen=True)
class MonthSpec:
    year: int
    month: int  # 1..12

    @property
    def yyyymm(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def _current_month(now: datetime | None = None) -> MonthSpec:
    if now is None:
        now = datetime.now(TZ)
    return MonthSpec(year=now.year, month=now.month)



def _previous_month(now: datetime | None = None) -> MonthSpec:
    if now is None:
        now = datetime.now(TZ)

    year = now.year
    month = now.month - 1
    if month == 0:
        month = 12
        year -= 1

    return MonthSpec(year=year, month=month)


def _prev_month(spec: MonthSpec) -> MonthSpec:
    year = spec.year
    month = spec.month - 1
    if month == 0:
        month = 12
        year -= 1
    return MonthSpec(year=year, month=month)


def _is_before_hard_stop(spec: MonthSpec) -> bool:
    """Return True if spec is older than the hard stop month."""
    y, m = spec.year, spec.month
    hy, hm = _HARD_STOP
    return (y, m) < (hy, hm)


@dataclass(frozen=True)
class LongEquityFormat:
    filename: str
    url: str
    local_path: Path


def _format_month(spec: MonthSpec) -> LongEquityFormat:
    year = spec.year
    month = spec.month

    month_name_capitalized = calendar.month_name[month]

    filename = settings.longequity_filename_template.format(
        year=year,
        month=month,
        month_name_capitalized=month_name_capitalized,
    )

    settings.longequity_dir.mkdir(parents=True, exist_ok=True)
    local_path = settings.longequity_dir / filename

    # ✅ Always use two-digit month for URLs
    month_2d = f"{month:02d}"

    base_url = settings.longequity_base_url.format(
        year=year,
        month=month_2d,
    )

    url = base_url.rstrip("/") + "/" + filename

    return LongEquityFormat(
        filename=filename,
        url=url,
        local_path=local_path,
    )



class RemoteNotFound(RuntimeError):
    """Raised when the remote file does not exist (HTTP 404)."""


def _download_if_missing(fmt: LongEquityFormat, *, timeout: int = 60) -> Path:
    if fmt.local_path.exists():
        return fmt.local_path

    tmp_path = fmt.local_path.with_suffix(fmt.local_path.suffix + ".part")

    try:
        with requests.get(fmt.url, stream=True, timeout=timeout) as r:
            if r.status_code == 404:
                raise RemoteNotFound(f"404 Not Found: {fmt.url}")
            r.raise_for_status()

            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        tmp_path.replace(fmt.local_path)
        return fmt.local_path

    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        raise


def acquire_raw_longequity_backfill(
    *,
    timeout: int = 60,
    now: datetime | None = None,
    verbose: bool = True,
) -> list[Path]:
    """
    Walk backwards from previous month.
    Rules:
      - Try each month; if 404, skip once and try the next older month.
      - If you get TWO 404s in a row, stop.
      - Hard stop at August 2025 (inclusive; do not go older).
    Returns paths ordered [most recent, ..., oldest] (as processed).
    """
    paths: list[Path] = []
    spec = _current_month(now)

    consecutive_404 = 0

    while True:
        if _is_before_hard_stop(spec):
            if verbose:
                print(f"[longequity] hard stop reached (< {_HARD_STOP[0]}-{_HARD_STOP[1]:02d}), stopping.")
            break

        fmt = _format_month(spec)

        try:
            p = _download_if_missing(fmt, timeout=timeout)
            paths.append(p)
            consecutive_404 = 0
            if verbose:
                print(f"[longequity] ok: {spec.yyyymm} -> {p.name}")
        except RemoteNotFound:
            consecutive_404 += 1
            if verbose:
                print(f"[longequity] missing (404): {spec.yyyymm} -> {fmt.filename}")

            if consecutive_404 >= 2:
                if verbose:
                    print("[longequity] two consecutive 404s, stopping backfill.")
                break

        spec = _prev_month(spec)

    return paths

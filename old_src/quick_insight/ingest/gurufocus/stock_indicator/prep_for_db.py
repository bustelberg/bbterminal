# src/quick_insight/ingest/gurufocus/stock_indicator/prep_for_db.py
from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from quick_insight.ingest.gurufocus.prep_for_db import (
    LONG_DF_COLUMNS,
    coerce_float,
    finalise_long_df,
    make_row,
    resolve_timestamps,
    yyyymm_to_month_end,
)


# =============================================================================
# Series extraction
# =============================================================================

def _extract_series_pairs(obj: Any) -> list[tuple[Any, Any]]:
    def from_records(records: Any) -> list[tuple[Any, Any]]:
        if not isinstance(records, list):
            return []
        out = []
        for r in records:
            if not isinstance(r, dict):
                continue
            d = r.get("date") or r.get("asOfDate") or r.get("period") or r.get("time")
            v = r.get("value")
            if d is not None and v is not None:
                out.append((d, v))
        return out

    def from_list_rows(rows: Any) -> list[tuple[Any, Any]]:
        if not isinstance(rows, list) or not rows:
            return []
        first = rows[0]
        if not isinstance(first, (list, tuple)) or len(first) < 2:
            return []
        out = []
        for r in rows:
            if not isinstance(r, (list, tuple)) or len(r) < 2:
                continue
            d_like = r[0]
            v_like = r[-1] if len(r) > 2 else r[1]
            if len(r) > 2 and coerce_float(v_like) is None:
                for candidate in reversed(r[1:]):
                    if coerce_float(candidate) is not None:
                        v_like = candidate
                        break
            out.append((d_like, v_like))
        return out

    if isinstance(obj, list):
        return from_records(obj) or from_list_rows(obj)

    if not isinstance(obj, dict):
        return []

    ind = obj.get("indicator")
    if isinstance(ind, (dict, list)):
        pairs = _extract_series_pairs(ind)
        if pairs:
            return pairs

    if "data" in obj:
        pairs = from_records(obj["data"]) or from_list_rows(obj["data"])
        if pairs:
            return pairs

    if isinstance(obj.get("date"), list):
        dates = obj["date"]
        if isinstance(obj.get("value"), list):
            return list(zip(dates, obj["value"]))
        for k, v in obj.items():
            if k != "date" and isinstance(v, list) and len(v) == len(dates):
                if any(coerce_float(x) is not None for x in v):
                    return list(zip(dates, v))

    for v in obj.values():
        if isinstance(v, dict):
            pairs = _extract_series_pairs(v)
            if pairs:
                return pairs

    return []


# =============================================================================
# Date parsing
# =============================================================================

RE_YYYYMM  = re.compile(r"^\d{6}$")
RE_ISO_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RE_UNIX    = re.compile(r"^\d{10,13}$")


def _to_target_date(d: Any) -> date | None:
    if d is None:
        return None
    if isinstance(d, pd.Timestamp):
        return d.date()
    if hasattr(d, "date") and callable(d.date):
        try:
            return d.date()
        except Exception:
            pass
    s = str(d).strip()
    if not s:
        return None
    if RE_YYYYMM.match(s):
        return yyyymm_to_month_end(s).date()
    if RE_ISO_DAY.match(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None
    if RE_UNIX.match(s):
        try:
            n = int(s)
            return pd.to_datetime(n, unit="ms" if len(s) == 13 else "s", utc=True).date()
        except Exception:
            return None
    try:
        return pd.Timestamp(pd.to_datetime(s, errors="raise")).date()
    except Exception:
        return None


# =============================================================================
# Metric code helpers
# =============================================================================

def _metric_code_from_filename(p: Path) -> str:
    name = p.name
    if not name.startswith("indicator__") or not name.endswith(".json"):
        return re.sub(r"_\d+$", "", p.stem)

    core = name[len("indicator__"):-len(".json")]

    # Remove trailing numeric suffix like:
    # free_cash_flow_365 -> free_cash_flow
    # forward_pe_ratio_3 -> forward_pe_ratio
    # gf_value_19 -> gf_value
    core = re.sub(r"_\d+$", "", core)

    return f"indicator_{core}"

# =============================================================================
# Granularity reporting
# =============================================================================

@dataclass(frozen=True)
class GranularityStats:
    metric_code: str
    rows: int
    min_gap_days: float | None
    granularity: str


def _classify_granularity(min_gap_days: float | None, rows: int) -> str:
    if rows <= 1:       return "single"
    if min_gap_days is None: return "unknown"
    if min_gap_days <=   3: return "daily"
    if min_gap_days <=  20: return "monthly"
    if min_gap_days <= 120: return "quarterly"
    return "yearly"


def _compute_granularity_stats(df: pd.DataFrame) -> list[GranularityStats]:
    if df.empty:
        return []
    df2 = df.copy()
    df2["_ts"] = pd.to_datetime(df2["target_date"], errors="coerce")
    df2 = df2.dropna(subset=["_ts"])
    stats = []
    for mc, g in df2.groupby("metric_code", sort=False):
        g = g.sort_values("_ts")
        diffs = g["_ts"].diff().dropna()
        min_gap = float(diffs.min().days) if not diffs.empty else None
        stats.append(GranularityStats(
            metric_code=str(mc), rows=len(g),
            min_gap_days=min_gap,
            granularity=_classify_granularity(min_gap, len(g)),
        ))
    return sorted(stats, key=lambda x: (-x.rows, x.metric_code))


def _print_granularity_report(stats: list[GranularityStats], *, top_n: int = 30) -> None:
    if not stats:
        print("[indicator] granularity: (no data)")
        return
    by_bucket: dict[str, int] = {}
    for s in stats:
        by_bucket[s.granularity] = by_bucket.get(s.granularity, 0) + 1
    print("[indicator] granularity summary:", ", ".join(f"{k}={v}" for k, v in sorted(by_bucket.items())))
    print(f"[indicator] granularity examples (top {min(top_n, len(stats))} by rows):")
    for s in stats[:top_n]:
        mg = "None" if s.min_gap_days is None else f"{s.min_gap_days:.0f}"
        print(f"  - {s.metric_code}: rows={s.rows}, min_gap_days={mg}, granularity={s.granularity}")


# =============================================================================
# Parallel file parsing
# =============================================================================

def _parse_indicator_file_to_rows(
    p: Path, *, primary_ticker: str, primary_exchange: str,
    source_code: str, published_at_date: date, imported_at: datetime,
    skip_lower: str, debug: bool,
) -> list[dict]:
    metric_code = _metric_code_from_filename(p)
    if skip_lower and skip_lower in metric_code.lower():
        return []
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        if debug:
            print(f"[DEBUG] SKIP json error {p.name}: {e}")
        return []

    rows = []
    for d_like, v_like in _extract_series_pairs(payload):
        td  = _to_target_date(d_like)
        val = coerce_float(v_like)
        if td is None or val is None:
            continue
        rows.append(make_row(
            primary_ticker=primary_ticker, primary_exchange=primary_exchange,
            metric_code=metric_code, target_date=td,
            published_at=published_at_date, imported_at=imported_at,
            source_code=source_code, value=val, is_prediction=False,
        ))
    return rows


def _chunked(seq: list[Path], n: int) -> Iterable[list[Path]]:
    for i in range(0, len(seq), n):
        yield seq[i: i + n]


def _parse_indicator_dir_parallel(
    files: Iterable[Path], *, primary_ticker: str, primary_exchange: str,
    source_code: str, published_at_date: date, imported_at: datetime,
    skip_lower: str, debug: bool, max_workers: int,
    progress_every: int = 250, submit_chunk_size: int = 500,
) -> list[dict]:
    files_list = list(files)
    total = len(files_list)
    if not total:
        return []

    heartbeat = max(progress_every, total // 10)
    rows: list[dict] = []
    done = ok_files = empty_files = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for batch in _chunked(files_list, submit_chunk_size):
            futures = [
                ex.submit(
                    _parse_indicator_file_to_rows, p,
                    primary_ticker=primary_ticker, primary_exchange=primary_exchange,
                    source_code=source_code, published_at_date=published_at_date,
                    imported_at=imported_at, skip_lower=skip_lower, debug=debug,
                )
                for p in batch
            ]
            for fut in as_completed(futures):
                done += 1
                file_rows = fut.result()
                if file_rows:
                    ok_files += 1
                    rows.extend(file_rows)
                else:
                    empty_files += 1
                if done % heartbeat == 0 or done == total:
                    print(f"[indicator] parse {done}/{total} | ok_files={ok_files} empty_files={empty_files} rows={len(rows)}")

    return rows


def _normalize_indicator_files(indicators_dir: Path, indicator_files: Iterable[str | Path] | None) -> list[Path]:
    if indicator_files is None:
        return []
    out = []
    for x in indicator_files:
        p = Path(x)
        p = (indicators_dir / p).resolve() if not p.is_absolute() else p.resolve()
        print(f"[DEBUG normalize] {p} exists={p.exists()}")  # ← add this line
        if p.exists() and p.is_file():
            out.append(p)
    return sorted(set(out), key=lambda p: p.as_posix())


# =============================================================================
# Public API
# =============================================================================

def load_indicator_dir_long_df(
    *,
    indicators_dir: str | Path,
    primary_ticker: str,
    primary_exchange: str,
    source_code: str = "gurufocus_api",
    published_at: datetime | None = None,
    imported_at: datetime | None = None,
    skip_if_key_contains: str = "",
    debug: bool = False,
    max_workers: int = 24,
    submit_chunk_size: int = 500,
    progress_every_files: int = 250,
    indicator_files: Iterable[str | Path] | None = None,
    print_granularity: bool = True,
    granularity_top_n: int = 30,
) -> pd.DataFrame:
    published_at_date, imported_at_dt = resolve_timestamps(published_at, imported_at)

    indicators_dir_p = Path(indicators_dir).expanduser().resolve()
    if not indicators_dir_p.exists():
        raise FileNotFoundError(f"Indicators dir not found: {indicators_dir_p}")
    if not indicators_dir_p.is_dir():
        raise NotADirectoryError(f"Not a directory: {indicators_dir_p}")

    only_files = _normalize_indicator_files(indicators_dir_p, indicator_files)
    if only_files:
        files = only_files
        print(f"[indicator] parse files={len(files)} (skipping dir scan)")
    else:
        files = sorted(indicators_dir_p.glob("indicator__*.json"))
        print(f"[indicator] scan dir={indicators_dir_p} → {len(files)} files")

    if not files:
        return finalise_long_df([])


    if len(files) == 1:
        rows_dicts = _parse_indicator_file_to_rows(
            files[0],
            primary_ticker=primary_ticker, primary_exchange=primary_exchange,
            source_code=source_code, published_at_date=published_at_date,
            imported_at=imported_at_dt, skip_lower=(skip_if_key_contains or "").lower(),
            debug=debug,
        )
    else:
        rows_dicts = _parse_indicator_dir_parallel(
            files,
            primary_ticker=primary_ticker, primary_exchange=primary_exchange,
            source_code=source_code, published_at_date=published_at_date,
            imported_at=imported_at_dt, skip_lower=(skip_if_key_contains or "").lower(),
            debug=debug, max_workers=max_workers,
            progress_every=progress_every_files, submit_chunk_size=submit_chunk_size,
        )




    df = finalise_long_df(rows_dicts)
    print(f"[indicator] done | files={len(files)} metrics={df['metric_code'].nunique()} rows={len(df)}")

    if print_granularity:
        _print_granularity_report(_compute_granularity_stats(df), top_n=granularity_top_n)

    return df
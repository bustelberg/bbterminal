"""
Derived metric values for universe tightening.

Each of the 7 LongEquity criteria reduces to a single numeric value per
fiscal year. We persist those values into metric_data (source_code='derived')
so that tightening a base universe is just a threshold filter — no need to
re-read GuruFocus JSON at query time.

All values are stored as percentages (e.g. 15.0 for 15%), regardless of the
native GuruFocus unit. The threshold operator per criterion is baked into
the query side (apply_filter_config); the frontend only adjusts numbers.

Metric codes:
    fcf_ps_3y_cagr_pct      — 3yr CAGR of Free Cash Flow per Share
    roic_3y_median_pct      — 3yr median ROIC %
    fcf_margin_3y_median_pct — 3yr median FCF Margin %
    ppe_to_assets_pct       — latest PPE / Total Assets
    capex_to_rev_pct        — latest Capex / Revenue
    sbc_to_ocf_pct          — latest |SBC| / OCF (OCF must be positive)
    shares_5y_change_pct    — 5yr change in shares outstanding
    interest_to_opinc_pct   — latest |Interest Expense| / Operating Income
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Generator

from supabase import Client

logger = logging.getLogger(__name__)


METRIC_CODES = [
    "fcf_ps_3y_cagr_pct",
    "roic_3y_median_pct",
    "fcf_margin_3y_median_pct",
    "ppe_to_assets_pct",
    "capex_to_rev_pct",
    "sbc_to_ocf_pct",
    "shares_5y_change_pct",
    "interest_to_opinc_pct",
]


@dataclass
class CriterionSpec:
    """How to evaluate one user-facing criterion against derived metric values."""
    key: str                 # matches CRITERIA_NAMES in criteria.py
    label: str
    default_threshold: float
    default_enabled: bool
    # For single-value criteria: the derived metric code + comparator.
    metric: str | None = None
    op: str = ">="           # ">=" or "<"
    # For composite (asset_capital_light): two sub-metrics, BOTH must pass (<=).
    components: list[tuple[str, str, float]] | None = None  # [(label, metric, default)]


CRITERIA_SPECS: list[CriterionSpec] = [
    CriterionSpec(
        key="fcf_growth",
        label="FCF/Share Growth (3yr CAGR)",
        default_threshold=15.0,
        default_enabled=True,
        metric="fcf_ps_3y_cagr_pct",
        op=">=",
    ),
    CriterionSpec(
        key="roic",
        label="ROIC (3yr median)",
        default_threshold=20.0,
        default_enabled=True,
        metric="roic_3y_median_pct",
        op=">=",
    ),
    CriterionSpec(
        key="fcf_margin",
        label="FCF Margin (3yr median)",
        default_threshold=20.0,
        default_enabled=False,
        metric="fcf_margin_3y_median_pct",
        op=">=",
    ),
    CriterionSpec(
        key="asset_capital_light",
        label="Asset & Capital Light",
        default_threshold=0.0,
        default_enabled=False,
        components=[
            ("PPE / Total Assets (max)", "ppe_to_assets_pct", 40.0),
            ("Capex / Revenue (max)",    "capex_to_rev_pct",  20.0),
        ],
    ),
    CriterionSpec(
        key="sbc",
        label="SBC / OCF (max)",
        default_threshold=30.0,
        default_enabled=False,
        metric="sbc_to_ocf_pct",
        op="<",
    ),
    CriterionSpec(
        key="dilution",
        label="Share Dilution (5yr, max)",
        default_threshold=5.0,
        default_enabled=False,
        metric="shares_5y_change_pct",
        op="<",
    ),
    CriterionSpec(
        key="interest_burden",
        label="Interest / Op. Income (max)",
        default_threshold=20.0,
        default_enabled=False,
        metric="interest_to_opinc_pct",
        op="<",
    ),
]


def default_filter_config() -> dict:
    """Produce the default filter_config: FCF growth + ROIC enabled, rest off."""
    cfg: dict = {}
    for c in CRITERIA_SPECS:
        entry: dict = {"enabled": c.default_enabled}
        if c.components is not None:
            entry["components"] = {code: default for _, code, default in c.components}
        else:
            entry["threshold"] = c.default_threshold
        cfg[c.key] = entry
    return cfg


# ---------------------------------------------------------------------------
# Per-FY computation from GuruFocus annuals JSON
# ---------------------------------------------------------------------------

def _periods(annuals: dict) -> list[str]:
    for c in ("Fiscal Year", "Date", "date"):
        v = annuals.get(c)
        if isinstance(v, list):
            return [str(p).strip() for p in v]
    return []


def _series(annuals: dict, section: str, key: str) -> list[float | None]:
    """Return aligned-with-periods list of values (or None) for a section/key."""
    block = annuals.get(section)
    if not isinstance(block, dict):
        return []
    node: object = block
    for part in key.split(" > "):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return []
    if not isinstance(node, list):
        return []
    out: list[float | None] = []
    for v in node:
        if v is None or str(v).strip() in ("", "-", "None"):
            out.append(None)
            continue
        try:
            out.append(float(v))
        except (ValueError, TypeError):
            out.append(None)
    return out


def _fy_end_date(fy: str) -> date | None:
    """Convert 'YYYY-MM' (or 'YYYY-MM-DD') to a date on the first of the month."""
    try:
        return date(int(fy[:4]), int(fy[5:7]), 1)
    except (ValueError, IndexError):
        return None


def compute_per_fy(annuals: dict) -> list[tuple[date, dict[str, float]]]:
    """Compute derived metric values for each fiscal year in the annuals block.

    Returns list of (fy_end_date, {metric_code: value_in_pct}) — only metrics
    that could be computed are included.

    GuruFocus returns periods most-recent-first, so index 0 is the latest FY.
    """
    periods = _periods(annuals)
    if not periods:
        return []

    fcf_ps  = _series(annuals, "Per Share Data",         "Free Cash Flow per Share")
    roic    = _series(annuals, "Ratios",                 "ROIC %")
    fcfm    = _series(annuals, "Ratios",                 "FCF Margin %")
    ppe     = _series(annuals, "Balance Sheet",          "Property, Plant and Equipment")
    ta      = _series(annuals, "Balance Sheet",          "Total Assets")
    ctr     = _series(annuals, "Ratios",                 "Capex-to-Revenue")
    sbc     = _series(annuals, "Cashflow Statement",     "Stock Based Compensation")
    ocf     = _series(annuals, "Cashflow Statement",     "Cash Flow from Operations")
    shares  = _series(annuals, "Valuation and Quality",  "Shares Outstanding (EOP)")
    interest = _series(annuals, "Income Statement",      "Interest Expense")
    opinc   = _series(annuals, "Income Statement",       "Operating Income")

    n = len(periods)
    out: list[tuple[date, dict[str, float]]] = []

    for i, fy in enumerate(periods):
        d = _fy_end_date(fy)
        if d is None:
            continue
        m: dict[str, float] = {}

        # FCF/share 3yr CAGR — needs value at index i and at i+3
        if i + 3 < len(fcf_ps) and fcf_ps[i] is not None and fcf_ps[i + 3] is not None:
            recent, prior = fcf_ps[i], fcf_ps[i + 3]
            if prior and prior > 0 and recent > 0:
                cagr = (recent / prior) ** (1 / 3) - 1
                m["fcf_ps_3y_cagr_pct"] = cagr * 100.0

        # 3yr median ROIC % (already a percentage in GF)
        window = [roic[j] for j in range(i, min(i + 3, len(roic))) if roic[j] is not None]
        if window:
            m["roic_3y_median_pct"] = sorted(window)[len(window) // 2]

        # 3yr median FCF Margin % (already a percentage)
        window = [fcfm[j] for j in range(i, min(i + 3, len(fcfm))) if fcfm[j] is not None]
        if window:
            m["fcf_margin_3y_median_pct"] = sorted(window)[len(window) // 2]

        # PPE / Total Assets (both in same units; ratio → pct)
        if i < len(ppe) and i < len(ta) and ppe[i] is not None and ta[i] not in (None, 0):
            m["ppe_to_assets_pct"] = (ppe[i] / ta[i]) * 100.0

        # Capex / Revenue — GuruFocus stores as a ratio (e.g. 0.15), convert to pct
        if i < len(ctr) and ctr[i] is not None:
            m["capex_to_rev_pct"] = ctr[i] * 100.0

        # |SBC| / OCF — only meaningful when OCF > 0
        if i < len(sbc) and i < len(ocf) and sbc[i] is not None and ocf[i] not in (None, 0) and ocf[i] > 0:
            m["sbc_to_ocf_pct"] = (abs(sbc[i]) / ocf[i]) * 100.0

        # 5yr shares outstanding change (i vs i+5)
        if i + 5 < len(shares) and shares[i] is not None and shares[i + 5] not in (None, 0) and shares[i + 5] > 0:
            m["shares_5y_change_pct"] = ((shares[i] - shares[i + 5]) / shares[i + 5]) * 100.0

        # |Interest| / Operating Income — only when OpInc > 0
        if i < len(interest) and i < len(opinc) and interest[i] is not None and opinc[i] not in (None, 0) and opinc[i] > 0:
            m["interest_to_opinc_pct"] = (abs(interest[i]) / opinc[i]) * 100.0

        if m:
            out.append((d, m))

        _ = n  # silence unused linter on simple guard

    return out


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _fetch_financials_cached(supabase: Client, ticker: str, exchange: str) -> dict | None:
    """Load cached GuruFocus financials JSON from Supabase Storage. None if absent."""
    path = f"{exchange.upper()}_{ticker.upper()}/financials.json"
    try:
        raw = supabase.storage.from_("gurufocus-raw").download(path)
        return json.loads(raw)
    except Exception:
        return None


def _get_annuals(data: dict) -> dict | None:
    f = data.get("financials")
    if not isinstance(f, dict):
        return None
    a = f.get("annuals")
    return a if isinstance(a, dict) else None


def _upsert_rows(supabase: Client, rows: list[dict]) -> int:
    """Upsert derived metric rows into metric_data in chunks."""
    if not rows:
        return 0
    total = 0
    batch = 500
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        resp = supabase.table("metric_data").upsert(
            chunk,
            on_conflict="company_id,metric_code,source_code,target_date",
            ignore_duplicates=False,
        ).execute()
        total += len(resp.data or [])
    return total


# ---------------------------------------------------------------------------
# Precompute pipeline
# ---------------------------------------------------------------------------

def _fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def precompute_for_companies(
    supabase: Client,
    companies: list[dict],
) -> Generator[dict, None, None]:
    """Compute derived metrics for each company. Falls back to GuruFocus API
    when the storage cache is missing (and writes the result back to the cache).

    Skips entire exchanges after their first 'unsubscribed region' 403 response.

    Yields SSE-style events: progress updates per company + a final 'done'.

    Each company dict needs: company_id, gurufocus_ticker, gurufocus_exchange.
    """
    import time
    from universe.screen import _fetch_financials_api  # lazy: avoids circular import

    total = len(companies)
    with_data = 0
    from_cache = 0
    from_api = 0
    no_data = 0
    no_annuals = 0
    invalid = 0
    rows_written = 0
    blocked_exchanges: set[str] = set()
    blocked_skipped = 0

    started = time.monotonic()

    def progress_suffix(idx: int) -> str:
        elapsed = time.monotonic() - started
        if idx <= 0:
            return f"· {_fmt_duration(elapsed)} elapsed"
        rate = idx / elapsed if elapsed > 0 else 0
        remaining = (total - idx) / rate if rate > 0 else 0
        return f"· {_fmt_duration(elapsed)} elapsed · ~{_fmt_duration(remaining)} left"

    for i, c in enumerate(companies):
        cid = c.get("company_id")
        ticker = c.get("gurufocus_ticker")
        exchange = c.get("gurufocus_exchange")
        name = c.get("company_name") or ticker or ""
        idx = i + 1
        if not cid or not ticker or not exchange:
            invalid += 1
            continue

        if exchange.upper() in blocked_exchanges:
            blocked_skipped += 1
            yield {
                "type": "progress_update",
                "message": f"[{idx}/{total}] {ticker} — skipped (unsubscribed exchange {exchange}) {progress_suffix(idx)}",
            }
            continue

        yield {
            "type": "progress_update",
            "message": f"[{idx}/{total}] {ticker} ({name}) {progress_suffix(idx)}",
        }

        data = _fetch_financials_cached(supabase, ticker, exchange)
        if data is not None:
            from_cache += 1
        else:
            yield {
                "type": "progress_update",
                "message": f"[{idx}/{total}] {ticker}: cache miss, fetching from API... {progress_suffix(idx)}",
            }
            try:
                data, log = _fetch_financials_api(supabase, ticker, exchange)
            except Exception as e:
                yield {"type": "progress_update", "message": f"  {ticker}: API error {e}"}
                no_data += 1
                continue
            if data is None:
                if log and "unsubscribed region" in log.lower():
                    blocked_exchanges.add(exchange.upper())
                    yield {
                        "type": "progress_update",
                        "message": f"  {ticker}: unsubscribed region — future {exchange} calls will be skipped",
                    }
                else:
                    yield {"type": "progress_update", "message": f"  {ticker}: API returned no data ({log[:80]})"}
                no_data += 1
                continue
            from_api += 1

        annuals = _get_annuals(data)
        if not annuals:
            no_annuals += 1
            continue

        per_fy = compute_per_fy(annuals)
        if not per_fy:
            no_annuals += 1
            continue

        rows = [
            {
                "company_id": cid,
                "metric_code": code,
                "source_code": "derived",
                "target_date": d.isoformat(),
                "numeric_value": val,
            }
            for d, metrics in per_fy
            for code, val in metrics.items()
        ]
        rows_written += _upsert_rows(supabase, rows)
        with_data += 1

    blocked_summary = (
        f", blocked {blocked_skipped} on {len(blocked_exchanges)} unsubscribed exchanges"
        if blocked_skipped
        else ""
    )
    yield {
        "type": "done",
        "message": (
            f"Computed derived metrics for {with_data}/{total} companies in {_fmt_duration(time.monotonic() - started)} "
            f"(cache: {from_cache}, API: {from_api}, no data: {no_data}, "
            f"no annuals: {no_annuals}, invalid: {invalid}{blocked_summary}). "
            f"{rows_written} rows written."
        ),
        "data": {
            "total": total,
            "with_data": with_data,
            "from_cache": from_cache,
            "from_api": from_api,
            "no_data": no_data,
            "no_annuals": no_annuals,
            "invalid": invalid,
            "blocked_skipped": blocked_skipped,
            "blocked_exchanges": sorted(blocked_exchanges),
            "rows_written": rows_written,
        },
    }


# ---------------------------------------------------------------------------
# Filter application
# ---------------------------------------------------------------------------

def _pass_op(value: float, op: str, threshold: float) -> bool:
    if op == ">=":
        return value >= threshold
    if op == "<":
        return value < threshold
    if op == "<=":
        return value <= threshold
    if op == ">":
        return value > threshold
    return False


def company_passes(
    filter_config: dict,
    metric_by_code: dict[str, float],
) -> bool:
    """True if this company's derived metrics satisfy every ENABLED criterion.

    Missing metric for an enabled criterion → fail (company doesn't qualify).
    """
    for spec in CRITERIA_SPECS:
        entry = filter_config.get(spec.key) or {}
        if not entry.get("enabled"):
            continue

        if spec.components is not None:
            comps = entry.get("components") or {}
            for _, code, default in spec.components:
                threshold = float(comps.get(code, default))
                val = metric_by_code.get(code)
                if val is None or not _pass_op(val, "<=", threshold):
                    return False
        else:
            threshold = float(entry.get("threshold", spec.default_threshold))
            val = metric_by_code.get(spec.metric or "")
            if val is None or not _pass_op(val, spec.op, threshold):
                return False

    return True


def required_metric_codes(filter_config: dict) -> list[str]:
    """Which metric codes the enabled filters actually need. Used to limit DB scans."""
    codes: set[str] = set()
    for spec in CRITERIA_SPECS:
        entry = filter_config.get(spec.key) or {}
        if not entry.get("enabled"):
            continue
        if spec.components is not None:
            for _, code, _default in spec.components:
                codes.add(code)
        elif spec.metric:
            codes.add(spec.metric)
    return sorted(codes)

"""Price + volume coverage audits.

Run after the bulk loads finish. Builds per-company labels, identifies
exchanges that appear fully unsubscribed (every member has zero rows),
and flags true gaps where specific tickers on subscribed exchanges have
missing or sparse data. Returns a structured `AuditResult` so callers
can emit events in the right order and the self-heal path can pick up
the gap lists."""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import pandas as pd


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


@dataclass
class AuditResult:
    events: list[str] = field(default_factory=list)
    # Per-company exchange + label lookups (used by the self-heal block to
    # filter gap lists and format error messages).
    exchange_for_cid: dict[int, str] = field(default_factory=dict)
    label_for_cid: dict[int, str] = field(default_factory=dict)
    # The "true gap" lists — companies on subscribed exchanges missing data.
    no_price_gap_cids: list[int] = field(default_factory=list)
    no_vol_gap_cids: list[int] = field(default_factory=list)
    unsubscribed_exchanges: list[str] = field(default_factory=list)


def audit_price_coverage(
    universe_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    company_ids: list[int],
) -> AuditResult:
    """Audit price coverage. Returns an `AuditResult` whose `events` list
    is in the order the original SSE stream emitted them so the caller
    can `yield from` straight into the response."""
    res = AuditResult()
    price_counts = prices_df.groupby("company_id").size().to_dict() if not prices_df.empty else {}
    _universe_symbol = {
        int(r["company_id"]): f"{r.get('gurufocus_exchange') or '?'}:{r['gurufocus_ticker']}"
        for _, r in universe_df.iterrows()
    }
    _universe_name = {
        int(r["company_id"]): r.get("company_name") or ""
        for _, r in universe_df.iterrows()
    }

    def _label(cid: int) -> str:
        sym = _universe_symbol.get(int(cid), str(cid))
        name = _universe_name.get(int(cid), "")
        return f"{sym} ({name})" if name else sym

    res.label_for_cid = {int(cid): _label(int(cid)) for cid in company_ids}

    _no_price = [cid for cid in company_ids if price_counts.get(int(cid), 0) == 0]
    _sparse_price = [cid for cid in company_ids if 0 < price_counts.get(int(cid), 0) < 20]

    # Group no-price companies by exchange. An exchange where every
    # universe company has zero price rows is almost certainly
    # unsubscribed on GuruFocus (or fully blocked) — surface it
    # separately from one-off gaps so the user can tell the difference.
    res.exchange_for_cid = {
        int(r["company_id"]): r.get("gurufocus_exchange") or "UNKNOWN"
        for _, r in universe_df.iterrows()
    }
    _exchange_totals: dict[str, int] = {}
    _exchange_no_price: dict[str, int] = {}
    for cid in company_ids:
        exch = res.exchange_for_cid.get(int(cid), "UNKNOWN")
        _exchange_totals[exch] = _exchange_totals.get(exch, 0) + 1
        if price_counts.get(int(cid), 0) == 0:
            _exchange_no_price[exch] = _exchange_no_price.get(exch, 0) + 1
    res.unsubscribed_exchanges = sorted(
        exch for exch, no_price in _exchange_no_price.items()
        if _exchange_totals.get(exch, 0) > 0 and no_price == _exchange_totals[exch]
    )
    if res.unsubscribed_exchanges:
        parts = [f"{exch}({_exchange_no_price[exch]})" for exch in res.unsubscribed_exchanges]
        total_unsub = sum(_exchange_no_price[e] for e in res.unsubscribed_exchanges)
        res.events.append(_emit({
            "type": "info",
            "scope": "prices",
            "message": f"Unsubscribed/blocked exchanges (expected to have no data): {', '.join(parts)} — {total_unsub} companies",
        }))

    # Remaining no-price cases: exchanges where some companies have
    # data but specific tickers don't — true one-off gaps.
    res.no_price_gap_cids = [
        cid for cid in _no_price
        if res.exchange_for_cid.get(int(cid), "UNKNOWN") not in res.unsubscribed_exchanges
    ]
    if res.no_price_gap_cids:
        sample = ", ".join(_label(int(c)) for c in res.no_price_gap_cids[:10])
        more = f" (+{len(res.no_price_gap_cids) - 10} more)" if len(res.no_price_gap_cids) > 10 else ""
        res.events.append(_emit({"type": "warning", "scope": "prices", "message": f"{len(res.no_price_gap_cids)} companies on subscribed exchanges have NO price data: {sample}{more}"}))
    if _sparse_price:
        sample = ", ".join(
            f"{_label(int(c))}[{price_counts.get(int(c), 0)} rows]" for c in _sparse_price[:10]
        )
        more = f" (+{len(_sparse_price) - 10} more)" if len(_sparse_price) > 10 else ""
        res.events.append(_emit({"type": "warning", "scope": "prices", "message": f"{len(_sparse_price)} companies have < 20 price rows (insufficient for signals): {sample}{more}"}))

    return res


def audit_volume_coverage(
    audit: AuditResult,
    volumes_df: pd.DataFrame,
    company_ids: list[int],
) -> None:
    """Audit volume coverage. Mutates `audit` in place — appends events
    in the original emit order and fills `audit.no_vol_gap_cids`.

    Companies on unsubscribed exchanges (already flagged in the prices
    info message) are expected to have no volume either, so filter them
    out of the warning set to avoid noise."""
    vol_counts = volumes_df.groupby("company_id").size().to_dict() if not volumes_df.empty else {}
    _no_vol_all = [cid for cid in company_ids if vol_counts.get(int(cid), 0) == 0]
    _sparse_vol = [cid for cid in company_ids if 0 < vol_counts.get(int(cid), 0) < 20]
    audit.no_vol_gap_cids = [
        cid for cid in _no_vol_all
        if audit.exchange_for_cid.get(int(cid), "UNKNOWN") not in audit.unsubscribed_exchanges
    ]
    if audit.no_vol_gap_cids:
        sample = ", ".join(audit.label_for_cid.get(int(c), str(c)) for c in audit.no_vol_gap_cids[:10])
        more = f" (+{len(audit.no_vol_gap_cids) - 10} more)" if len(audit.no_vol_gap_cids) > 10 else ""
        audit.events.append(_emit({"type": "warning", "scope": "volumes", "message": f"{len(audit.no_vol_gap_cids)} companies on subscribed exchanges have NO volume data — volume signals will be skipped for them: {sample}{more}"}))
    if _sparse_vol:
        sample = ", ".join(
            f"{audit.label_for_cid.get(int(c), str(c))}[{vol_counts.get(int(c), 0)} rows]" for c in _sparse_vol[:10]
        )
        more = f" (+{len(_sparse_vol) - 10} more)" if len(_sparse_vol) > 10 else ""
        audit.events.append(_emit({"type": "warning", "scope": "volumes", "message": f"{len(_sparse_vol)} companies have < 20 volume rows: {sample}{more}"}))


def build_universe_snapshot(universe_df: pd.DataFrame) -> list[dict]:
    """Build the per-company snapshot the frontend table renders against.

    `_norm_str` handles None / NaN explicitly: pandas Series .get("col",
    default) only falls through to the default when the COLUMN is
    missing, not when the cell is None or NaN. Without this normalization
    an exchange link that's absent in the DB ends up as the literal
    string "None" or "nan" in the JSON payload, which (a) breaks the
    frontend's GuruFocus URL helper (US-vs-non-US classifier sees
    "None" as non-US and produces "/stock/None:TICKER/summary") and
    (b) renders "(None)" or "(nan)" in the holdings table."""
    def _norm_str(val) -> str:
        if val is None:
            return ""
        try:
            if pd.isna(val):
                return ""
        except (TypeError, ValueError):
            pass
        return str(val)

    return [
        {
            "company_id": int(row["company_id"]),
            "ticker": _norm_str(row.get("gurufocus_ticker")),
            "exchange": _norm_str(row.get("gurufocus_exchange")),
            "company_name": _norm_str(row.get("company_name")),
            "sector": _norm_str(row.get("sector")),
            "country": _norm_str(row.get("country")),
        }
        for _, row in universe_df.iterrows()
    ]

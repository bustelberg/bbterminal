"""
src\quick_insight\web\helpers\gurufocus_data_helpers\ingest_runner.py
ingest_runner.py — runs GuruFocus ingest for one or many companies
with live per-step status yielded back to the caller.

Yields dicts so the Streamlit page can update a live table row-by-row.

Result schema per company:
  {
    "ticker":    str,
    "exchange":  str,
    "estimates": "ok" | "skip" | "blocked" | "error",
    "financials": "ok" | "skip" | "error",
    "indicators": "ok" | "skip" | "error",
    "overall":   "ok" | "partial" | "blocked" | "error",
    "note":      str,   # human-readable detail
  }
"""
from __future__ import annotations

from typing import Generator

from quick_insight.ingest.gurufocus.analyst_estimates.endpoint import GFOutcome
from quick_insight.ingest.gurufocus.analyst_estimates.orchestrate import (
    orchestrate_analyst_estimates,
)
from quick_insight.ingest.gurufocus.financials.orchestrate import orchestrate_financials
from quick_insight.ingest.gurufocus.stock_indicator.orchestrate import (
    orchestrate_indicators,
)


# ── sentinel values for the status columns ───────────────────────────────────
S_OK      = "✅ ok"
S_SKIP    = "⏭ skip"
S_BLOCKED = "🚫 blocked"
S_ERROR   = "❌ error"
S_PENDING = "⏳ …"


def _outcome_str(outcome: GFOutcome) -> str:
    if outcome == GFOutcome.OK:
        return S_OK
    if outcome == GFOutcome.SKIP_ENTRY:
        return S_SKIP
    if outcome == GFOutcome.BLOCK_EXCHANGE:
        return S_BLOCKED
    return S_ERROR


def ingest_companies(
    rows: list[tuple[str, str]],
    *,
    use_cache: bool = True,
) -> Generator[dict, None, None]:
    """
    Yields one result dict per company as it completes.
    rows: list of (primary_ticker, primary_exchange)
    """
    blocked_exchanges: set[str] = set()

    for ticker, exchange in rows:
        result: dict = {
            "ticker":     ticker,
            "exchange":   exchange,
            "estimates":  S_PENDING,
            "financials": S_PENDING,
            "indicators": S_PENDING,
            "overall":    S_PENDING,
            "note":       "",
        }

        # Yield "in-progress" row immediately so the UI shows the company
        yield {**result, "overall": S_PENDING}

        if exchange in blocked_exchanges:
            yield {
                **result,
                "estimates":  S_BLOCKED,
                "financials": S_BLOCKED,
                "indicators": S_BLOCKED,
                "overall":    S_BLOCKED,
                "note":       f"Exchange {exchange} previously blocked",
            }
            continue

        notes: list[str] = []

        # ── 1. Analyst estimates ──────────────────────────────────────────
        try:
            outcome = orchestrate_analyst_estimates(ticker, exchange, use_cache=use_cache)
            result["estimates"] = _outcome_str(outcome)
            if outcome == GFOutcome.BLOCK_EXCHANGE:
                blocked_exchanges.add(exchange)
                result["financials"] = S_BLOCKED
                result["indicators"] = S_BLOCKED
                result["overall"]    = S_BLOCKED
                result["note"]       = f"Exchange {exchange} blocked on estimates"
                yield result
                continue
            if outcome == GFOutcome.SKIP_ENTRY:
                notes.append("estimates skipped")
        except Exception as e:
            result["estimates"] = S_ERROR
            notes.append(f"estimates error: {e}")

        # ── 2. Financials ─────────────────────────────────────────────────
        try:
            fin = orchestrate_financials(ticker, exchange, use_cache=use_cache)
            result["financials"] = S_OK if fin else S_SKIP
            if not fin:
                notes.append("financials skipped/empty")
        except Exception as e:
            result["financials"] = S_ERROR
            notes.append(f"financials error: {e}")

        # ── 3. Indicators ─────────────────────────────────────────────────
        try:
            ind = orchestrate_indicators(ticker, exchange, use_cache=use_cache)
            result["indicators"] = S_OK if ind else S_SKIP
            if not ind:
                notes.append("indicators skipped/empty")
        except Exception as e:
            result["indicators"] = S_ERROR
            notes.append(f"indicators error: {e}")

        # ── overall ───────────────────────────────────────────────────────
        statuses = {result["estimates"], result["financials"], result["indicators"]}
        if S_ERROR in statuses:
            result["overall"] = S_ERROR
        elif statuses == {S_OK}:
            result["overall"] = S_OK
        elif S_OK in statuses:
            result["overall"] = "⚠️ partial"
        else:
            result["overall"] = S_SKIP

        result["note"] = "; ".join(notes) if notes else ""
        yield result

"""IBKR tradeable-instrument resolution — SEAM + stub.

The real adapter targets IBKR's **OAuth Web API** and runs in the USER's
environment (their consumer key / access token / signed requests) — this
backend can't reach their IBKR session, so it ships a stub. Drop the real
client behind `resolve_tradeable_eu` (and later `fetch_prices`):

Target flow (OAuth Web API):
  1. OAuth1a handshake -> live session token (LST) + signed requests.
  2. Resolve ISIN -> contracts: GET /iserver/secdef/search?symbol=<ISIN> (or
     /trsrv/stocks), then /iserver/secdef/info for the conids.
  3. Filter to European tradeable venues (IBIS/XETRA, AEB, SBF, LSE, BVME, ...),
     prefer the primary/most-liquid one; capture conid + currency + exchange.
  4. (later) GET /iserver/marketdata/history for that conid's daily bars — the
     EXECUTION instrument's own price series.

Enable by setting IBKR_ENABLED=1 and implementing the two functions below."""
from __future__ import annotations

import os


def enabled() -> bool:
    return os.environ.get("IBKR_ENABLED", "").lower() in ("1", "true", "yes")


def resolve_tradeable_eu(isin: str, analysis: dict | None = None) -> dict:
    """Resolve an ISIN to its European tradeable IBKR contract(s).

    Returns ``{status, message, isin, candidates:[{symbol, exchange, currency,
    conid, tradeable}], chosen}``. Stubbed until an IBKR OAuth Web API client is
    wired (see module docstring)."""
    if not enabled():
        return {
            "status": "stub",
            "message": (
                "IBKR OAuth Web API not wired yet. Set IBKR_ENABLED=1 and "
                "implement resolve_tradeable_eu() (OAuth1a session -> "
                "/iserver/secdef/search by ISIN -> pick the European venue) to "
                "resolve the tradeable listing + fetch its price series."
            ),
            "isin": isin,
            "candidates": [],
            "chosen": None,
        }
    # TODO: real OAuth Web API implementation here.
    raise NotImplementedError("Wire the IBKR OAuth Web API client here.")

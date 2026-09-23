"""Checked-in sector overrides for securities vendor metadata cannot classify.

The database's ``company_sector_override`` table is the editable management layer, but a row
entered locally does not deploy to production. Reviewed facts that must be identical in every
environment live in ``sector_overrides.json`` and win if a database row disagrees.

Key by ISIN, not company name or ``company_id``: the ISIN is shared by the AIRS holdings and ACWI
constituents, survives database rebuilds, and reaches both sides of the analysis through
``asset_grid``.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

_log = logging.getLogger(__name__)
_FILE = Path(__file__).with_name("sector_overrides.json")

# The GICS vocabulary accepted by the management override endpoint. Keeping the file within the
# same set prevents one typo from creating a plausible-looking extra bucket in production.
GICS_SECTORS = frozenset({
    "Communication Services", "Consumer Discretionary", "Consumer Staples",
    "Energy", "Financials", "Health Care", "Industrials", "Information Technology",
    "Materials", "Real Estate", "Utilities",
})


def load_file_sector_overrides() -> dict[str, str]:
    """Return ``{ISIN: GICS sector}``; log and ignore a malformed file at runtime.

    CI validates the file strictly. Runtime remains fail-soft so one damaged entry cannot prevent
    the rest of the portfolio analysis from loading.
    """
    try:
        raw = json.loads(_FILE.read_text(encoding="utf-8"))
        out: dict[str, str] = {}
        for entry in raw.get("overrides", []):
            isin = str(entry.get("isin") or "").strip().upper()
            sector = str(entry.get("sector") or "").strip()
            if isin and sector in GICS_SECTORS:
                out[isin] = sector
        return out
    except Exception as exc:  # noqa: BLE001 — analysis must still load without the manual layer
        _log.error("[sector_override] %s is unreadable (%s: %s); no checked-in sector "
                   "overrides applied", _FILE.name, type(exc).__name__, exc)
        return {}

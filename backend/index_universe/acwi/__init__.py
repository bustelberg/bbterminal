"""Parse iShares MSCI ACWI ETF holdings and scrape MSCI announcements.

External callers (`routers.index_universe`, `playground/acwi_*.py`,
`fx_rates.py`, `routers.momentum._helpers`) keep importing from
`index_universe.acwi`, so this `__init__.py` re-exports everything they
touched when the module lived in one file.

Layout:
  holdings.py            — iShares fund XML parser + load_acwi_holdings + _FILE
  exchange_map.py        — iShares ↔ GuruFocus exchange / URL / ticker mappings
  announcement_detail.py — per-URL detail fetch + parse + cache
  announcements.py       — MSCI list scraper + 24h list cache
  net_additions.py       — fuzzy-name matching + compute_net_additions
  reconstruction.py      — historical monthly holdings reconstruction
"""
from __future__ import annotations

from .holdings import _FILE, _parse_xml_spreadsheet, load_acwi_holdings
from .exchange_map import (
    FEASIBLE_GF_EXCHANGES,
    _GF_URL_TO_API,
    _ISHARES_TO_GF,
    _SKIP_LISTINGS,
    _US_EXCHANGES,
    _load_gf_ticker_overrides,
    _normalize_gf_ticker,
    _resolve_ticker_override,
    expected_db_exchange_codes,
    gurufocus_exchange,
    gurufocus_exchange_for_db,
    gurufocus_ticker_normalized,
    gurufocus_url,
)
from .announcement_detail import (
    _AnnouncementContentParser,
    _load_detail_cache,
    _parse_old_format,
    _save_detail_cache,
    fetch_announcement_detail,
    fetch_announcement_detail_cached,
    fetch_bulk_details,
)
from .announcements import (
    _MsciAnnouncementParser,
    _load_cache,
    _save_cache,
    _scrape_msci_announcements,
    get_msci_announcements,
)
from .net_additions import (
    _clean_name,
    _extract_first_company,
    _fuzzy_token_match,
    _load_name_overrides,
    _tokenize,
    _tokenize_significant,
    compute_net_additions,
)
from .reconstruction import (
    _parse_effective_date,
    feasible_holdings_for_db,
    reconstruct_monthly_holdings,
)

__all__ = [
    # holdings
    "load_acwi_holdings", "_FILE",
    # exchange map
    "FEASIBLE_GF_EXCHANGES", "_ISHARES_TO_GF",
    "gurufocus_exchange", "gurufocus_exchange_for_db",
    "gurufocus_url", "gurufocus_ticker_normalized",
    "expected_db_exchange_codes",
    # announcement detail
    "fetch_announcement_detail", "fetch_announcement_detail_cached",
    "fetch_bulk_details",
    "_load_detail_cache", "_save_detail_cache",
    # announcements
    "get_msci_announcements",
    # net additions
    "compute_net_additions",
    "_clean_name", "_extract_first_company",
    # reconstruction
    "feasible_holdings_for_db", "reconstruct_monthly_holdings",
    "_parse_effective_date",
]

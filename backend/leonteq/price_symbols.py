"""Currency-specific price-check mappings, independent of analysis listing choices."""
from datetime import datetime, timezone
import math
import logging

from asset_pipeline import openfigi, yahoo
from asset_pipeline.fast_resolve import build_candidates
from asset_pipeline.fx import SUBUNIT
from asset_pipeline.resolve import same_company
from deps import supabase

_log = logging.getLogger(__name__)
_STORAGE_WARNING = 'Yahoo mappings could not be saved or loaded. Prices can still be checked; check the price-symbol database migration and connection.'


def currency(value):
    value = (value or '').strip()
    return SUBUNIT.get(value, (value.upper(), 1))[0]


def resolve(isin, source_currency, *, force=False, check=lambda: None, warn=lambda message: None):
    """Only accept FIGI-derived listings with matching issuer, currency and real closes."""
    ccy = currency(source_currency)
    if not isin or not ccy:
        return None
    if not force:
        try:
            saved = (supabase.table('leonteq_price_symbol').select('symbol')
                     .eq('isin', isin).eq('currency', ccy).execute().data or [])
        except Exception as exc:
            _log.warning('Price-symbol cache read failed: %s', type(exc).__name__)
            warn(_STORAGE_WARNING)
            saved = []
        if saved:
            return saved[0]['symbol']
    check()
    identity = openfigi.lookup_isin(isin)
    if not identity:
        return None
    # Generated candidates retain the FIGI security identity, including ADR/GDR class.
    anchored = build_candidates(isin, identity, limit=40)
    hits = yahoo.search(isin)
    candidates = list(dict.fromkeys([h['symbol'] for h in hits if h.get('symbol') in anchored] + anchored))
    for symbol in candidates[:12]:
        check()
        data = yahoo.chart(symbol, rng='3mo', interval='1d') or {}
        meta = data.get('meta') or {}
        name = meta.get('longName') or meta.get('shortName')
        closes = (((data.get('indicators') or {}).get('quote') or [{}])[0]).get('close') or []
        if (meta.get('symbol', '').upper() != symbol.upper() or currency(meta.get('currency')) != ccy
                or not name or not any(item.get('name') and same_company(name, item['name']) for item in identity)
                or not any(isinstance(p, (int, float)) and math.isfinite(p) and p > 0 for p in closes)):
            continue
        check()
        try:
            supabase.table('leonteq_price_symbol').upsert({
                'isin': isin, 'currency': ccy, 'symbol': symbol,
                'verified_at': datetime.now(timezone.utc).isoformat(),
            }, on_conflict='isin,currency').execute()
        except Exception as exc:
            _log.warning('Price-symbol cache write failed: %s', type(exc).__name__)
            warn(_STORAGE_WARNING)
        return symbol
    return None

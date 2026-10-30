"""Repair fixed-window benchmark coverage before any historical risk calculation."""
from datetime import date
from threading import Lock
import logging
import time

from routers._risk_history_window import covers_history, history_marks

_lock = Lock()
_attempts: dict[tuple, float] = {}


def ensure_benchmark_history(series: dict, benchmark_isins: set[str], start: str,
                             years: int, reload) -> dict:
    """Single-flight missing benchmark repairs; always reread after another caller repairs."""
    from routers._risk_read import ALLOW_REPAIR
    if not ALLOW_REPAIR.get():
        return series
    marks = history_marks(start, date.today().isoformat())
    # The latest completed trading close may precede today (weekends and holidays).
    latest = [day for isin in benchmark_isins for day, price in series.get(isin, [])
              if day <= date.today().isoformat() and price is not None and price > 0]
    if latest:
        marks = history_marks(start, max(latest))
    missing = {isin for isin in benchmark_isins if not covers_history(series.get(isin, []), marks)}
    if not missing:
        return series
    from asset_pipeline.price_refresh import repair_price_histories

    identity = (tuple(sorted(missing)), start)
    with _lock:
        if time.monotonic() >= _attempts.get(identity, 0):
            # Full history, not merely the newest gap; includes reviewed ticker pins and FX.
            _attempts[identity] = time.monotonic() + 60
            try:
                repair_price_histories(missing, years=years, full_history=True, yahoo_only=True)
            except Exception:
                logging.getLogger(__name__).exception('Automatic benchmark history repair failed')
        # A previous caller may have filled the store while this request waited on the lock.
        return reload()

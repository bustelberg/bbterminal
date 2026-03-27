"""
portfolio_cache.py
Centralised cache registry for all portfolio-related st.cache_data functions.

Why this exists
---------------
Multiple pages (Upload_Portfolio, Earnings_Dashboard) cache data that
derives from the same DB tables.  When the Upload page mutates the DB,
it must also bust the Earnings page caches — and vice-versa.

Usage
-----
In every page that caches portfolio data:

    from quick_insight.web.helpers.portfolio_cache import portfolio_cache

    @portfolio_cache.register
    @st.cache_data(show_spinner=False)
    def my_cached_fn(...): ...

Then call  `portfolio_cache.clear_all()`  wherever you previously called
individual `.clear()` methods.
"""
from __future__ import annotations

from typing import Callable, TypeVar

F = TypeVar("F")


class _PortfolioCache:
    """Tracks all registered st.cache_data callables and clears them together."""

    def __init__(self) -> None:
        self._fns: list = []

    def register(self, fn: F) -> F:
        """Decorator — call after @st.cache_data so `fn` already has .clear()."""
        self._fns.append(fn)
        return fn

    def clear_all(self) -> None:
        for fn in self._fns:
            try:
                fn.clear()
            except Exception:
                pass


portfolio_cache = _PortfolioCache()
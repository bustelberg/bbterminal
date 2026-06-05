"""Shared dependencies for router modules.

Pulled out of `main.py` so each router can `from deps import supabase`
without importing main (which would cause a circular import since main
needs to include the routers in turn).

Anything router code touches that doesn't belong to a specific domain —
the Supabase client, env loading, common type aliases — lives here. Keep
this module thin; if a helper has a clear home in one of the
`ingest`/`momentum`/`universe` packages, put it there instead.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Callable, Iterator

from dotenv import load_dotenv
from supabase import create_client

if TYPE_CHECKING:
    from supabase import Client

# .env first (prod defaults), .env.local overrides (local dev only — file
# doesn't exist on Railway/Vercel so this is a no-op there).
load_dotenv()
load_dotenv(".env.local", override=True)


class _LazySupabase:
    """Proxy that defers `create_client(...)` until the first method call.

    `from deps import supabase` resolves to this instance at import
    time — no env vars touched. The real client is built on the first
    attribute access (e.g. `supabase.table('foo')`) and cached. Lets
    `pytest`, `scripts/dump_openapi.py`, and any future tool import
    router modules without SUPABASE_URL / SUPABASE_SERVICE_KEY set.

    Functionally a drop-in for `Client` — all method/attribute access
    falls through `__getattr__` after the first call materializes it.
    Code that hits the DB still needs the env vars; code that doesn't
    no longer does.
    """

    __slots__ = ("_real",)

    def __init__(self) -> None:
        # Use `object.__setattr__` so initialization doesn't recurse
        # through `_LazySupabase.__setattr__`.
        object.__setattr__(self, "_real", None)

    def _build(self) -> "Client":
        real = object.__getattribute__(self, "_real")
        if real is None:
            real = create_client(
                os.environ["SUPABASE_URL"],
                os.environ["SUPABASE_SERVICE_KEY"],
            )
            object.__setattr__(self, "_real", real)
        return real

    def __getattr__(self, name: str) -> Any:
        # Only invoked when normal lookup fails — i.e. `name` isn't on
        # the proxy itself. Forward to the real client.
        return getattr(self._build(), name)


supabase = _LazySupabase()


# Default chunk size for `.in_()` queries.
#
# PostgREST encodes IN-clauses into the URL query string ("?col=in.(1,2,3,...)"),
# so a long company-id list can blow past Cloudflare's URL/header limits and
# return a 502 before the request ever reaches Supabase. Previous value was 50,
# chosen as a known-safe lower bound. Bumped to 200 because (a) company_id is
# at most ~5 digits + a comma = ~6 chars per entry, so 200 IDs is ~1.2 KB of
# query string -- well under Cloudflare's 8 KB default and PostgREST's 4 KB
# request-line limit, and (b) halving the number of round trips noticeably
# speeds up the momentum backtest universe load. If 502s reappear, drop to 100
# or revert to 50.
IN_CHUNK_SIZE = 200


def chunked(items: list, size: int = IN_CHUNK_SIZE) -> Iterator[list]:
    """Yield successive `size`-length slices of `items`. The canonical
    list-splitter for the `.in_()` / batched-write chunking the PostgREST +
    Cloudflare URL/row limits force (see `IN_CHUNK_SIZE`). `fetch_in_chunks`
    builds on this for the select-and-collect case; callers doing batched
    DELETE/UPDATE iterate it directly."""
    for start in range(0, len(items), size):
        yield items[start:start + size]


def fetch_in_chunks(
    ids: list,
    query: Callable[[list], Any],
    *,
    chunk_size: int = IN_CHUNK_SIZE,
) -> list:
    """Run `query(chunk)` for each `IN_CHUNK_SIZE`-sized slice of `ids` and
    concatenate the resulting rows.

    Single home for the `.in_()` chunking that the PostgREST/Cloudflare
    URL-length limit forces on every bulk id lookup (see `IN_CHUNK_SIZE`).
    `query` receives one id slice and returns either an executed supabase
    response (with a `.data` list) or a plain list of rows; the rows are
    flattened in slice order. Returns `[]` for empty `ids` (no query issued).

    Example:
        rows = fetch_in_chunks(cids, lambda chunk:
            supabase.table("company").select("company_id, company_name")
            .in_("company_id", chunk).execute())

    For paginated / parallel / retrying loads (e.g. price+volume history),
    use the purpose-built loaders in `momentum/data/` instead.
    """
    rows: list = []
    for chunk in chunked(ids, chunk_size):
        result = query(chunk)
        data = getattr(result, "data", result)
        if data:
            rows.extend(data)
    return rows


def paginate(query: Callable[[int, int], Any], *, page_size: int = 1000) -> Iterator[dict]:
    """Yield every row from a `.range()`-paginated PostgREST select.

    `query(lo, hi)` receives inclusive 0-based range bounds (PostgREST
    `.range(lo, hi)` semantics) and returns an executed response (with a
    `.data` list) or a plain list of rows. Pages of `page_size` are walked
    until one comes back short (or empty) — the single home for the
    offset/`.range()` loop that PostgREST's 1000-row cap forces on every
    full-table scan.

    Example:
        for row in paginate(lambda lo, hi:
            supabase.table("company").select("company_id").range(lo, hi).execute()):
            ...
    """
    offset = 0
    while True:
        result = query(offset, offset + page_size - 1)
        data = getattr(result, "data", result) or []
        yield from data
        if len(data) < page_size:
            return
        offset += page_size

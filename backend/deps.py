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

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterator

from dotenv import load_dotenv
from supabase import ClientOptions, create_client  # ClientOptions == SyncClientOptions

if TYPE_CHECKING:
    from supabase import Client

# .env first (prod defaults), .env.local overrides (local dev only — file
# doesn't exist on Railway/Vercel so this is a no-op there).
#
# IMPORTANT: resolve both files relative to THIS file (backend/), NOT the
# process CWD. `load_dotenv(".env.local")` is CWD-relative — starting uvicorn
# from the repo root (or anywhere but backend/) silently skipped backend/.env.local
# and left SUPABASE_URL pointing at the prod default in .env, so local dev hit
# the prod DB. Anchoring to __file__ makes the launch directory irrelevant.
_BACKEND_DIR = Path(__file__).resolve().parent
load_dotenv(_BACKEND_DIR / ".env")
load_dotenv(_BACKEND_DIR / ".env.local", override=True)

# Log which DB this process is wired to — startup line makes "local vs prod"
# obvious instead of a silent mystery (local dev hitting prod = empty market
# caps, etc.). Host only, no keys.
print(f"[deps] SUPABASE_URL = {os.environ.get('SUPABASE_URL', '<UNSET>')}", flush=True)


class _CachingSession:
    """The PostgREST session, with a per-request memo on identical GETs.

    ⚠ IT IS INERT UNLESS SOMETHING OPTED IN. Outside a `common.read_cache.read_cache()` block —
    which is everything except the endpoints that asked for it — `send` is the base client's,
    byte for byte. There is no global cache here and no TTL to reason about.

    ⚠ IT SUBCLASSES THE SESSION RATHER THAN PATCHING `httpx.Client`. Yahoo, GuruFocus, OpenFIGI,
    iShares and Supabase Storage all use their own `httpx.Client`s; a patch on the class would
    quietly memoize a vendor call whose repetition may be the entire point (a paced price loop
    asking about a symbol twice is not a duplicate).

    See `common/read_cache.py` for what was measured and why the count of round trips, rather
    than a local stopwatch, is the number that matters.
    """

    def __init__(self, **kw: Any) -> None:
        import httpx  # noqa: PLC0415

        # Composition, not inheritance of httpx.Client — wrapping keeps us clear of httpx's own
        # constructor and attribute surface changing under us.
        #
        # ⚠ THE FORWARDING IS READ-ONLY, WHICH IS SAFE ONLY BECAUSE NOTHING WRITES TO THE SESSION.
        # `__getattr__` forwards attribute READS to the real client, but an assignment
        # (`session.headers = ...`) would land on this wrapper and be silently ignored by the
        # client underneath. Checked against the installed postgrest 2.28.3: the entire surface it
        # uses is `self.session.request(...)` (intercepted below) and `self.session.close()`
        # (forwarded) — there is no assignment anywhere in postgrest or supabase-py. If that ever
        # changes, this needs a `__setattr__` too.
        self._c = httpx.Client(**kw)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._c, name)

    def request(self, method: str, url: Any, **kw: Any) -> Any:
        """⚠ `request`, NOT `send` — postgrest builds no `Request` object.

        `RequestConfig.send()` calls `session.request(method, path, json=, params=, headers=,
        auth=)`, so an override of `send` is never reached: the memo silently did nothing, the
        round-trip count stayed at 212, and the only symptom was that it did not get faster. If
        postgrest ever moves to `send`, this stops caching rather than starting to serve wrong
        answers — the failure mode is a lost optimisation, which is the right way round.
        """
        from common import read_cache  # noqa: PLC0415  (avoid an import cycle at module load)

        if read_cache.active() is None:
            return self._c.request(method, url, **kw)
        if method.upper() not in ("GET", "HEAD"):
            # A write invalidates the snapshot — see `note_write`.
            read_cache.note_write()
            return self._c.request(method, url, **kw)
        # ⚠ THE KEY INCLUDES `prefer` AND `range`. The same URL asked with `Prefer: count=exact`,
        # or over a different `Range`, is a DIFFERENT question — pagination and the count variant
        # both ride on HEADERS rather than on the query string, so a URL-only key would serve
        # page 1 for every page of a paged read. That is the one mistake here that would produce
        # wrong data rather than merely slow data.
        headers = kw.get("headers") or {}
        key = (method.upper(), str(url), str(kw.get("params") or ""),
               headers.get("prefer"), headers.get("range"))
        hit = read_cache.lookup(key)
        if hit is not None:
            return hit
        import time  # noqa: PLC0415

        t0 = time.perf_counter()
        r = self._c.request(method, url, **kw)
        if r.is_success:
            # Reading `.content` forces the body so the cached response can be handed out again;
            # postgrest re-parses those bytes into FRESH rows per caller, which is what makes
            # sharing one response object safe.
            _ = r.content
            read_cache.store(key, r, (time.perf_counter() - t0) * 1000)
        return r


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
            # Bound every PostgREST/Storage call. The default postgrest
            # timeout is 120s — far too long for a worker thread to hold when
            # Supabase slows (e.g. the ingest pipeline contends with many
            # polling read endpoints), which lets the default `to_thread`
            # pool starve and read endpoints (/api/usage, …) hang until the
            # client gives up (~300s). 30s fails fast instead; the pipeline's
            # `_retry_transient` re-tries timed-out metric_data upserts, and
            # read endpoints catch + return empty, so a slow dependency
            # degrades gracefully rather than wedging the UI.
            real = create_client(
                os.environ["SUPABASE_URL"],
                os.environ["SUPABASE_SERVICE_KEY"],
                options=ClientOptions(
                    postgrest_client_timeout=30,
                    storage_client_timeout=30,
                ),
            )
            # Force PostgREST onto HTTP/1.1. postgrest-py hardcodes http2=True on
            # its httpx session; Supabase's Cloudflare gateway caps requests per
            # connection and sends an HTTP/2 GOAWAY once hit, which httpx surfaces
            # as `RemoteProtocolError: ConnectionTerminated` on whatever request
            # was in flight (it can't safely retry a non-idempotent-looking one).
            # A chatty read sweep (e.g. the price refresh's per-company
            # `_db_max_date`) then sees a RUN of failures mid-batch. HTTP/1.1 uses
            # a plain connection pool with no GOAWAY-mid-stream failure mode, so
            # swap in an http1 session that copies the base_url + auth headers
            # postgrest already set. Best-effort: if supabase-py internals move we
            # keep the default (http2) client rather than crash the app.
            try:
                _pg = real.postgrest.session
                real.postgrest.session = _CachingSession(
                    base_url=_pg.base_url,
                    headers=_pg.headers,
                    timeout=_pg.timeout,
                    follow_redirects=True,
                    http2=False,
                )
                _pg.close()  # fresh client, no in-flight requests — safe to drop
            except Exception:
                logging.getLogger(__name__).debug(
                    "[deps] postgrest HTTP/1.1 swap skipped; using default client",
                    exc_info=True,
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

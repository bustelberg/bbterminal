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

from dotenv import load_dotenv
from supabase import create_client

# .env first (prod defaults), .env.local overrides (local dev only — file
# doesn't exist on Railway/Vercel so this is a no-op there).
load_dotenv()
load_dotenv(".env.local", override=True)

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"],
)

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

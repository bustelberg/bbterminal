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

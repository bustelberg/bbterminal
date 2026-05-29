"""Dump the FastAPI app's OpenAPI spec to backend/openapi.json.

The committed JSON is the source of truth for the frontend type
generator (`frontend/lib/api-types.ts`) — running this script after
adding / changing an endpoint or a Pydantic model produces a fresh
spec, and CI fails if a commit doesn't include the regenerated file.

Run from the backend directory:

    uv run python scripts/dump_openapi.py

Output is deterministic: sorted keys + 2-space indent so diffs are
review-friendly. Importing `main` loads every router, scheduler hooks,
and Supabase client init, but this script never touches the network —
the OpenAPI spec is built from in-memory route metadata.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Importing main has the side-effect of constructing the FastAPI app
# and mounting every router — exactly what we need to enumerate paths.
from main import app  # noqa: E402

OUT = ROOT / "openapi.json"


def main() -> int:
    spec = app.openapi()
    # `sort_keys=True` makes the output reproducible across runs and
    # process invocations — without it, dict iteration order would vary
    # by router import order and produce noisy diffs.
    OUT.write_text(
        json.dumps(spec, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

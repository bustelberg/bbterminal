"""Per-domain FastAPI APIRouter modules.

Imported and mounted by `main.py` via `app.include_router(...)`. Each
module owns one domain (benchmarks, momentum, universe, etc.) — see
individual files for endpoint listings. Shared client/state lives in
`backend/deps.py`.
"""

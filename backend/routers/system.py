"""Small system-level endpoints: health check, hello, GuruFocus API usage.

Endpoints:
    GET /api/hello   sanity ping
    GET /api/health  Supabase connectivity probe (used by uptime checks)
    GET /api/items   demo endpoint kept around for the boilerplate page
    GET /api/usage   GuruFocus API call counter for the current month
"""

from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter

from deps import supabase
from ingest.api_usage import get_usage

router = APIRouter(tags=["system"])


@router.get("/api/hello")
def hello():
    return {"message": "Hello from FastAPI + uv!"}


@router.get("/api/health")
def health():
    """Probe Supabase connectivity. Returns {status: ok | error, ...}."""
    try:
        url = os.environ.get("SUPABASE_URL", "NOT SET")
        has_key = "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO"
        resp = supabase.table("company").select("company_id").limit(1).execute()
        return {
            "status": "ok",
            "supabase_url": url,
            "has_service_key": has_key,
            "test_query": "success",
            "rows": len(resp.data or []),
        }
    except Exception as e:
        return {
            "status": "error",
            "supabase_url": os.environ.get("SUPABASE_URL", "NOT SET"),
            "has_service_key": "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO",
            "error": str(e),
        }


@router.get("/api/items")
def get_items():
    try:
        result = supabase.table("items").select("*").execute()
        return {"items": result.data}
    except Exception:
        return {"items": []}


@router.get("/api/usage")
async def api_usage():
    """GuruFocus API usage counter for the current month."""
    return await asyncio.to_thread(get_usage, supabase)

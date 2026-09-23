"""Company sector edits retain the verified actor on the active override row."""
from __future__ import annotations

import asyncio

from starlette.requests import Request

from routers import companies
from tests._fake_supabase import FakeSupabase


def _request(user_id: str = "b29a8667-87e6-4a66-a5dc-1bfb462c885e",
             email: str = "editor@example.com") -> Request:
    request = Request({"type": "http", "method": "PUT", "path": "/", "headers": []})
    request.state.auth = {"id": user_id, "email": email, "role": "user"}
    return request


def test_setting_an_override_records_the_verified_user(monkeypatch):
    fake = FakeSupabase({"company": [{"company_id": 42}], "company_sector_override": []})
    monkeypatch.setattr(companies, "supabase", fake)

    result = asyncio.run(companies.set_company_sector_override(
        42, companies.SectorOverrideRequest(sector="Information Technology"), _request()))

    assert result["sector"] == "Information Technology"
    saved = fake.tables["company_sector_override"][0]
    assert saved["updated_by_user_id"] == "b29a8667-87e6-4a66-a5dc-1bfb462c885e"
    assert saved["updated_by_email"] == "editor@example.com"
    assert saved["updated_at"]


def test_automatic_removes_the_active_override(monkeypatch):
    fake = FakeSupabase({
        "company": [{"company_id": 42}],
        "company_sector_override": [{
            "company_id": 42, "sector": "Energy",
            "updated_by_user_id": "b29a8667-87e6-4a66-a5dc-1bfb462c885e",
            "updated_by_email": "editor@example.com",
        }],
    })
    monkeypatch.setattr(companies, "supabase", fake)

    result = asyncio.run(companies.set_company_sector_override(
        42, companies.SectorOverrideRequest(sector=None), _request()))

    assert result == {"company_id": 42, "sector": None}
    assert fake.tables["company_sector_override"] == []

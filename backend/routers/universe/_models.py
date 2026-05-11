"""Pydantic request models shared across the universe routers."""
from __future__ import annotations

from pydantic import BaseModel


class ScreenRequest(BaseModel):
    as_of_year: str | None = None  # e.g. "2025-12"
    force_refresh: bool = False


class BuildUniverseRequest(BaseModel):
    start_month: str  # "YYYY-MM"
    end_month: str    # "YYYY-MM"
    label: str = "default"
    max_companies: int = 5


class UniverseRenameRequest(BaseModel):
    new_label: str


class DeriveUniverseRequest(BaseModel):
    base_universe_id: int
    label: str | None = None  # required for non-preview
    description: str | None = None
    filter_config: dict


class RecomputeRequest(BaseModel):
    universe_ids: list[int] | None = None  # None = all companies in any universe

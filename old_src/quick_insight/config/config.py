# src\quick_insight\config.py
from __future__ import annotations

from dotenv import load_dotenv
load_dotenv() 

import os
from dataclasses import dataclass
from pathlib import Path


# ------------------------------------------------------------------
# Required env helpers (fail fast)
# ------------------------------------------------------------------

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )
    return val


def _require_int(name: str) -> int:
    val = _require_env(name)
    try:
        return int(val)
    except ValueError:
        raise RuntimeError(
            f"Environment variable {name} must be an integer, got: {val}"
        )


def _require_bool(name: str) -> bool:
    val = _require_env(name).strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    raise RuntimeError(
        f"Environment variable {name} must be a boolean "
        f"(1/0, true/false), got: {val}"
    )


def _require_path(name: str) -> Path:
    return Path(_require_env(name))


# ------------------------------------------------------------------
# Settings (ALL REQUIRED)
# ------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    # DuckDB
    db_path: Path = _require_path("QI_DB_PATH")
    schema_path: Path = _require_path("QI_SCHEMA_PATH")

    # Raw storage
    raw_dir: Path = _require_path("QI_RAW_DIR")
    longequity_dir: Path = _require_path("QI_LONGEQUITY_DIR")
    archive_dir: Path = _require_path("QI_ARCHIVE_DIR")
    gurufocus_dir: Path = _require_path("QI_GURUFOCUS_DIR")

    # Scheduler
    timezone: str = _require_env("QI_TIMEZONE")
    run_day: int = _require_int("QI_RUN_DAY")
    run_hour: int = _require_int("QI_RUN_HOUR")
    run_minute: int = _require_int("QI_RUN_MINUTE")

    # Streamlit
    streamlit_host: str = _require_env("QI_STREAMLIT_HOST")
    streamlit_port: int = _require_int("QI_STREAMLIT_PORT")

    # GURUFOCUS
    gurufocus_base_url: str = _require_env("GURUFOCUS_BASE_URL")
    gurufocus_api_key: str = _require_env("GURUFOCUS_API_KEY")

    # FISCAL.AI
    fiscal_ai_base_url: str = _require_env("FISCAL_AI_BASE_URL")
    fiscal_ai_api_key: str = _require_env("FISCAL_AI_API_KEY")

    # cache
    cache_dir: str = _require_env("CACHE_DIR")

    # URLs
    longequity_base_url: str = _require_env("LONGEQUITY_BASE_URL")
    longequity_filename_template: str = _require_env(
        "LONGEQUITY_FILENAME_TEMPLATE"
    )

    def __post_init__(self) -> None:
        # ---- Validate LONGEQUITY_BASE_URL ----
        base = self.longequity_base_url

        required_base_placeholders = {"{year}", "{month}"}
        missing_base = [p for p in required_base_placeholders if p not in base]

        if missing_base:
            raise RuntimeError(
                "LONGEQUITY_BASE_URL is invalid. Missing placeholders: "
                + ", ".join(missing_base)
            )

        # ---- Validate LONGEQUITY_FILENAME_TEMPLATE ----
        template = self.longequity_filename_template

        required_filename_placeholders = {"{year}", "{month_name_capitalized}"}
        missing_filename = [
            p for p in required_filename_placeholders if p not in template
        ]

        if missing_filename:
            raise RuntimeError(
                "LONGEQUITY_FILENAME_TEMPLATE is invalid. Missing placeholders: "
                + ", ".join(missing_filename)
            )



# Instantiate immediately → fail at import time
settings = Settings()

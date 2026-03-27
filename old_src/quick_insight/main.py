from quick_insight.config.config import settings
from quick_insight.db import ensure_schema

ensure_schema(settings.db_path, settings.schema_path)

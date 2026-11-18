"""Reviewed GuruFocus listings keyed by the security's ISIN."""
import json
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def reviewed_listings():
    path = Path(__file__).resolve().parents[1] / 'config' / 'gurufocus_listing_overrides.json'
    return json.loads(path.read_text(encoding='utf-8'))

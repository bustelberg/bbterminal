# src/quick_insight/ingest/gurufocus/stock_indicator/list_indicators.py
from __future__ import annotations

from quick_insight.config.config import settings
from quick_insight.ingest.gurufocus.utils import (
    normalize_base_url,
    mask_url,
    request_json,
)


def main() -> None:
    base  = normalize_base_url(settings.gurufocus_base_url)
    token = settings.gurufocus_api_key

    url = f"{base}/public/user/{token}/stock/indicators"
    print(f"Fetching: {mask_url(url)}\n")

    resp = request_json(url, timeout=30)

    if not resp.ok:
        print(f"[FAILED] status={resp.status_code} gf_error={resp.gf_error}")
        return

    data = resp.data

    if isinstance(data, dict):
        keys = sorted(data.keys())
        print(f"Found {len(keys)} indicators:\n")
        for k in keys:
            print(f"  {k}")

    elif isinstance(data, list):
        print(f"Found {len(data)} indicators:\n")
        for item in data:
            print(f"  {item}")

    else:
        print(f"Unexpected response type: {type(data)}")
        print(data)


if __name__ == "__main__":
    main()
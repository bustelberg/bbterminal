"""Build saved benchmark charts from stored company data, without vendor API requests.

Run from backend: .venv/Scripts/python.exe scripts/rebuild_benchmark_snapshots.py ACWI
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


async def rebuild(label, cadence):
    from routers import _blend_prewarm as warm
    from routers._benchmark_snapshots import read

    started_at = datetime.now(timezone.utc).isoformat()
    start = time.perf_counter()
    await warm._warm_one(label, cadence, warm._generation)
    saved = await asyncio.to_thread(read, label, cadence)
    if saved is None or saved['built_at'] < started_at:
        raise RuntimeError(f'{label}/{cadence} snapshot was not published; previous data was kept')
    build_seconds = time.perf_counter() - start
    start = time.perf_counter()
    again = await asyncio.to_thread(read, label, cadence)
    read_ms = (time.perf_counter() - start) * 1000
    raw = json.dumps(again, separators=(',', ':')).encode()
    print(json.dumps({'label': label, 'cadence': cadence, 'built_at': saved['built_at'],
                      'build_seconds': round(build_seconds, 2), 'read_ms': round(read_ms, 2),
                      'json_bytes': len(raw), 'gzip_bytes': len(gzip.compress(raw)),
                      'series': sum(len(v) for v in saved['series'].values())}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('label', choices=['ACWI', 'SP500', 'AEX'])
    parser.add_argument('--cadence', choices=['annual', 'quarterly', 'both'], default='both')
    args = parser.parse_args()
    for cadence in (['annual', 'quarterly'] if args.cadence == 'both' else [args.cadence]):
        asyncio.run(rebuild(args.label, cadence))

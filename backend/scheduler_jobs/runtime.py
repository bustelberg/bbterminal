"""Explicit dispatch capabilities supplied to domain jobs; no scheduler import."""
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class JobRuntime:
    reporter: Callable
    run_job: Callable
    relative_momentum: Callable
    model_prices: Callable
    asset_refresh: Callable
    await_db_ready: Callable
    sources_busy: Callable
    wait_for_yahoo_queue: Callable
    fundamental_ids: Callable
    record_run: Callable
    active_jobs: Callable

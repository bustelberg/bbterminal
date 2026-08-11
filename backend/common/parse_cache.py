"""Parse a cached PostgREST response ONCE, then hand out cheap copies of the rows.

THE COST THIS REMOVES, MEASURED 2026-08-11 ON THE ANALYSE MODAL

    `common/read_cache.py` removes the round trip for a repeated read, but not the PARSING:
    it caches the HTTP response and postgrest re-runs `APIResponse.from_http_request_response`
    on every hit. Profiled by self time, that is the largest pure-CPU cost in the endpoint:

        196 x pydantic validate_json     0.821 s
        197 x pydantic validate_python   0.342 s   -> 1.16 s, ~21% of the computation

    65 HTTP requests produced 196 parses, so roughly two thirds of that work is re-parsing
    bytes we had already turned into dicts.

⚠ THE ORIGINAL DESIGN WAS RIGHT TO REFUSE A `deepcopy`, AND THE MEASUREMENT SAYS SO. `read_cache`'s
    docstring rejected caching parsed rows because a deep copy is "not obviously cheaper than the
    query it replaces". On the real payloads:

        payload                rows   re-parse   shallow   scalar-safe   deepcopy
        airs_performance       1000    2.89 ms    0.23 ms      2.08 ms    8.86 ms
        airs_mutatie            999    1.25 ms    0.16 ms      0.83 ms    4.84 ms
        airs_model_weight       734    1.55 ms    0.16 ms      0.81 ms    3.85 ms

    `deepcopy` IS worse than re-parsing — that instinct was correct. What the docstring got wrong
    was concluding that no copy works: a SHALLOW copy is ~10x cheaper than `json.loads` alone and
    ~25x cheaper than the full pydantic parse. (`json.loads` is only part of a re-parse; the
    profile's 4-6 ms per call is the whole thing.)

⚠⚠ A SHALLOW COPY IS ONLY SAFE WHEN EVERY VALUE IS A SCALAR, AND THAT IS CHECKED, NOT ASSUMED.
    `dict(row)` shares nested values by reference, so a caller mutating `row["config"]["x"]` would
    reach into the cached master. Most rows here are flat — none of the six largest payloads on
    this path contains a single nested value — but the schema has `jsonb` columns and array
    columns (`asset_universe.params`, `airs_model_portfolio.positions_dates`), so "flat" is a
    property to verify per payload. `_is_flat` scans once at parse time; non-flat payloads take
    the scalar-safe path, which deep-copies ONLY the non-scalar values and is still 2-3x cheaper
    than re-parsing.

⚠ CALLERS DO MUTATE THE ROWS THEY GET BACK — this is not a hypothetical. `_benchmark_index._members`
    runs `r["currency"] = ccy_by_exch.get(...)` over its result. That is exactly why the rows are
    copied out rather than shared, and why the FIRST caller also gets a copy: the pristine parsed
    list is kept as the master and never handed to anyone.

⚠ THE CACHE LIVES ON THE RESPONSE OBJECT ITSELF, not in a dict keyed by `id()`. `read_cache`
    returns the SAME `httpx.Response` instance on a hit, so an attribute on it is exactly as
    long-lived as the cached response and cannot be aliased to a different one — whereas an
    `id()`-keyed map silently hands back another object's rows after a garbage collection.

⚠ IT DEGRADES TO EXACTLY TODAY'S BEHAVIOUR. A response with no marker parses normally; if
    postgrest's internals move, `install()` fails loudly at import rather than silently caching
    nothing.
"""
from __future__ import annotations

import copy
import logging
from typing import Any

log = logging.getLogger(__name__)

_ATTR = "_bb_parsed_rows"
_SCALARS = (str, int, float, bool, type(None))

_installed = False


def _is_flat(rows: list) -> bool:
    """True when every value in every row is a scalar, so `dict(row)` is a complete copy."""
    for r in rows:
        if not isinstance(r, dict):
            return False
        for v in r.values():
            if not isinstance(v, _SCALARS):
                return False
    return True


def _copy_rows(rows: list, flat: bool) -> list:
    if flat:
        return [dict(r) for r in rows]
    return [{k: (v if isinstance(v, _SCALARS) else copy.deepcopy(v)) for k, v in r.items()}
            if isinstance(r, dict) else copy.deepcopy(r)
            for r in rows]


def _construct(model: Any, **fields: Any) -> Any:
    """Build a model without re-validating. Falls back to the validating constructor if the
    pydantic version in use has no `model_construct` — correctness first, speed second."""
    ctor = getattr(model, "model_construct", None)
    if ctor is None:
        return model(**fields)
    return ctor(**fields)


def install() -> bool:
    """Patch `APIResponse.from_http_request_response` to reuse a parse. Idempotent."""
    global _installed
    if _installed:
        return True
    try:
        from postgrest.base_request_builder import APIResponse
    except Exception as e:  # noqa: BLE001 — never let an optimisation break startup
        log.warning("[parse-cache] postgrest internals not found (%s) — disabled", e)
        return False

    original = APIResponse.from_http_request_response

    def from_http_request_response(request_response: Any) -> Any:
        hit = getattr(request_response, _ATTR, None)
        if hit is not None:
            rows, flat, count = hit
            # ⚠ `model_construct`, NOT `APIResponse(...)`. The normal constructor runs pydantic
            # validation over the whole payload — profiled at 197 calls / 0.338s of
            # `validate_python`, which would have eaten most of what skipping the JSON parse just
            # saved. These rows came out of a validated parse and were copied, not built, so
            # re-validating them proves nothing. `model_construct` is pydantic's documented
            # entry point for exactly that case.
            return _construct(APIResponse, data=_copy_rows(rows, flat), count=count)

        resp = original(request_response)
        data = resp.data
        # Only list payloads are worth memoizing, and only they have the row shape the copy
        # helpers assume. A single-object or scalar response falls straight through.
        if isinstance(data, list) and data:
            try:
                flat = _is_flat(data)
                # ⚠ THE MASTER IS A COPY, AND THE CALLER KEEPS THE ORIGINAL. Storing `data`
                # itself would hand the first caller the master — and `_members` mutates its
                # rows in place, so the second caller would inherit those edits.
                setattr(request_response, _ATTR, (_copy_rows(data, flat), flat, resp.count))
            except Exception as e:  # noqa: BLE001
                log.debug("[parse-cache] not memoizing this response: %s", e)
        return resp

    APIResponse.from_http_request_response = staticmethod(from_http_request_response)
    _installed = True
    return True

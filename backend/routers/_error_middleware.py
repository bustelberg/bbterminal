"""Turn an unhandled server error back into a readable server error.

THE PROBLEM IT SOLVES
    Starlette's stack, outermost first, is:

        ServerErrorMiddleware  →  CORSMiddleware  →  [our middlewares]  →  routes

    `CORSMiddleware` adds `Access-Control-Allow-Origin` to a RESPONSE. It does not catch
    exceptions. So when a route raises, nothing produces a response at that layer — the exception
    sails straight past CORS to `ServerErrorMiddleware`, which is OUTSIDE it and answers with a
    bare 500 carrying no CORS headers at all.

⚠ THE BROWSER THEN REPORTS THE WRONG FAULT, AND IT POINTS AT THE WRONG FILE. A missing header on a
    cross-origin response is a CORS block, so the console says:

        No 'Access-Control-Allow-Origin' header is present on the requested resource

    …for an origin that IS allow-listed and a backend that IS up. Measured 2026-07-31 on
    `/api/airs/portfolios/overview`: the real fault was a table the hosted database did not have
    yet, and both deployments read as a CORS misconfiguration. Every minute spent in `main.py`'s
    allow-list was spent in the wrong place, because the one message the browser can give you
    describes the symptom of the 500 rather than the 500.

    `main.py` already fixed the same class of bug for auth REJECTIONS by ordering CORS outermost —
    but ordering cannot help here, since the failure is the ABSENCE of a response, not a response
    without headers. The only fix is to catch the exception INSIDE CORS and return a real one.

⚠ `app.add_exception_handler(Exception, …)` IS NOT THIS, AND IS THE OBVIOUS WRONG ANSWER. Starlette
    installs that handler on `ServerErrorMiddleware` itself — the layer that is already outside
    CORS. It produces a prettier body with the identical missing header, i.e. it changes nothing a
    browser can see.

⚠ IT DOES NOT — AND MUST NOT — RESCUE A FAILURE MID-STREAM. `call_next` returns as soon as the
    response STARTS, so an SSE generator that dies on its tenth frame raises during body iteration,
    long after this returned. That is correct: the status line is already on the wire and cannot be
    rewritten into a 500. This layer is for failures that happen BEFORE a byte is sent, which is
    where an unhandled route exception lives.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse

_log = logging.getLogger(__name__)


async def cors_safe_errors(
    request: Request,
    call_next: Callable[[Request], Awaitable],
):
    """Convert an unhandled exception into a 500 RESPONSE, so CORS can label it.

    Registered between the auth gate and `CORSMiddleware` (see `main.py`) so it wraps the gate as
    well as the routes — a gate that raises is exactly as invisible to a browser as a route that
    does.

    Handled exceptions never reach here: `HTTPException` and request-validation errors are turned
    into responses by FastAPI's inner `ExceptionMiddleware`. What arrives is the genuinely
    unexpected — and `BaseException` (`CancelledError` on a client disconnect) is deliberately NOT
    caught, since a cancelled request has nobody left to answer.
    """
    try:
        return await call_next(request)
    except Exception:
        # ⚠ `logging.exception`, and the traceback is the entire point. This response is
        # deliberately opaque to the caller — an internal traceback is not something to ship to a
        # browser — so the deploy log is now the ONLY place the real cause exists. Losing it here
        # would trade a misleading CORS error for a truthful but equally uninformative 500.
        _log.exception("[error] unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse({"detail": "Internal Server Error"}, status_code=500)

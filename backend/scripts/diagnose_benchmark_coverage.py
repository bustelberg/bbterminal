"""Why does an index show fewer constituents in one environment than another?

Read-only. Answers ONE question and does not guess at it: is the shortfall a SILENT TRUNCATION, or
does this environment genuinely hold less data?

    uv run python scripts/diagnose_benchmark_coverage.py            # ACWI, this environment
    uv run python scripts/diagnose_benchmark_coverage.py SP500 AEX

    uv run python scripts/diagnose_benchmark_coverage.py --prod ACWI   # read-only, production

⚠⚠ `--prod` READS `backend/.env` DIRECTLY, WHICH IS THE ONLY WAY TO REACH PRODUCTION FROM HERE.
`deps` loads `.env` and then `.env.local` with **override=True**, so by the time any script runs,
`SUPABASE_URL` is the LOCAL one — and setting it on the command line does not help, because the
same override wins again. A script that "targeted prod" that way would cheerfully report on local
while printing prod in its header; that contamination has already cost a debugging session here.
So `--prod` parses `.env` into a dict WITHOUT touching `os.environ` and builds the client from it.
(`PROD_SUPABASE_URL` / `PROD_SUPABASE_SERVICE_KEY` still work if you want to point somewhere else.)

⚠ NOTHING IS WRITTEN. Every statement below is a SELECT or a count.

⚠⚠ THE TWO CAUSES LOOK IDENTICAL ON SCREEN AND HAVE OPPOSITE FIXES. "994 of 1,998 priced" is what
you see whether PostgREST cut a read at its 1,000-row cap or whether the asset pipeline in that
environment has only ever resolved 994 of them. The first is a bug to fix in code; the second is a
resolve/price run to schedule. Guessing wrong costs a day.

⚠ SO EVERY COUNT IS TAKEN TWICE — once PAGED and once deliberately UNPAGED. PostgREST caps a
response at 1,000 rows on Supabase cloud and 10,000 locally, and truncates SILENTLY, so:

    paged == unpaged                  -> no truncation anywhere; the data really is this size
    unpaged pinned at 1000 (or 10000) -> that read WOULD truncate; the paged number is the truth
    paged ALSO pinned at ~1000        -> a real bug: something in the chain is not paging

Run it against production and against local and put the two outputs side by side. The row that
differs is the answer.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402  (loads .env / .env.local before anything reads a key)


def _client():
    """This environment's client, or production's when `PROD_*` is set.

    ⚠ REBUILT EXPLICITLY RATHER THAN VIA ENV. See the module docstring: `.env.local` is loaded with
    `override=True`, so setting `SUPABASE_URL` on the command line cannot reach `deps`.
    """
    url, key = os.environ.get("PROD_SUPABASE_URL"), os.environ.get("PROD_SUPABASE_SERVICE_KEY")
    if "--prod" in sys.argv and not (url and key):
        # ⚠ `dotenv_values`, NOT `load_dotenv` — it returns a dict and leaves `os.environ` alone.
        # Loading it would put prod's URL where `.env.local` has already put local's, and which one
        # won would depend on the order two libraries happened to run in.
        from dotenv import dotenv_values  # noqa: PLC0415

        env = dotenv_values(Path(__file__).resolve().parents[1] / ".env")
        url, key = env.get("SUPABASE_URL"), env.get("SUPABASE_SERVICE_KEY")
        if not (url and key):
            sys.exit("--prod: backend/.env has no SUPABASE_URL + SUPABASE_SERVICE_KEY to use.")
        if "127.0.0.1" in url or "localhost" in url:
            # ⚠ REFUSED RATHER THAN RUN. A `--prod` that silently reports on local is the exact
            # failure this flag exists to prevent.
            sys.exit(f"--prod: backend/.env points at {url}, which is not production.")
    if not (url and key):
        return deps.supabase, os.environ.get("SUPABASE_URL")
    from supabase import create_client  # noqa: PLC0415

    client = create_client(url, key)
    # `members()` reads the module-level client, so point that at prod too — otherwise the counts
    # above would be prod's and the app-path line below would be local's, in one table.
    deps.supabase = client
    import routers._asset_benchmark as ab  # noqa: PLC0415

    ab.supabase = client
    # ⚠⚠ AND DISARM THE COPY TRANSPORT, WHICH WOULD OTHERWISE STILL BE POINTED AT LOCAL.
    # `common/pg.py` builds its own direct-Postgres connection from `SUPABASE_DB_URL` /
    # `DATABASE_URL` — env this script cannot repoint with a service key. Left alone, `members()`
    # would read PROD's membership and LOCAL's `asset_grid`, and print the mixture as one number:
    # a wrong answer produced confidently, which is worse than no script. Cleared, the loader
    # returns None and falls back to the (paged, chunked) PostgREST path against prod.
    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        os.environ.pop(var, None)
    return client, url


supabase, _TARGET = _client()


def _paged(table: str, select: str, universe_id: int, order: str) -> int:
    """Count via the SAME paging discipline the app uses — advance by what came back, break empty."""
    n, off = 0, 0
    while True:
        rows = (supabase.table(table).select(select)
                .eq("universe_id", universe_id).order(order)
                .range(off, off + 999).execute().data or [])
        if not rows:
            return n
        n += len(rows)
        off += len(rows)


def _unpaged(table: str, select: str, universe_id: int) -> int:
    """One bare `.execute()` — the shape that truncates. Here on purpose, as the control."""
    return len(supabase.table(table).select(select).eq("universe_id", universe_id)
               .execute().data or [])


def report(label: str) -> None:
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        print(f"\n=== {label}: no universe with that label here ===")
        return
    uid = uni[0]["universe_id"]
    print(f"\n=== {label}  (universe_id={uid}) ===")

    rows = [
        ("universe_membership   (the index itself)", "universe_membership", "company_id"),
        ("universe_asset_membership (bridged to assets)", "universe_asset_membership", "analysis_id"),
    ]
    paged_asset = 0
    for title, table, col in rows:
        p = _paged(table, col, uid, col)
        u = _unpaged(table, col, uid)
        if table == "universe_asset_membership":
            paged_asset = p
        flag = ""
        if u in (1000, 10000) and p > u:
            flag = f"   <- an unpaged read here would stop at {u}"
        elif p == u:
            flag = "   (no truncation — same both ways)"
        print(f"  {title:<46} paged {p:>6}   unpaged {u:>6}{flag}")

    # ⚠ THE BRIDGE LOSS IS NOT A BUG. A company with no ISIN — 189 ACWI members, mostly Indian and
    # British — cannot reach the asset world at all, and GuruFocus cannot supply one either. It is
    # reported so it is not mistaken for the truncation this script is looking for.
    company_ids = _paged("universe_membership", "company_id", uid, "company_id")
    if company_ids and paged_asset:
        print(f"  {'bridge loss (no ISIN / no asset row)':<46} "
              f"{company_ids - paged_asset:>6} members cannot reach the asset world")

    # What actually gets DRAWN: the app's own path, so this line is the panel's number.
    from routers._asset_benchmark import members  # noqa: PLC0415

    mem, cov = members(label)
    print(f"  {'priced by the app (members())':<46} {len(mem):>6}   "
          f"of {cov['universe_members']} members "
          f"({cov['covered_pct']:.1f}%)" if cov.get("covered_pct") is not None else "")
    # ⚠ A ROUND ~1000 HERE, WITH THE PAGED COUNTS ABOVE ALL HEALTHY, IS THE SIGNATURE TO CHASE.
    if 990 <= len(mem) <= 1010:
        print("  ⚠ that is suspiciously close to PostgREST's 1,000-row cloud cap — if the paged "
              "counts above are larger, something between them is not paging.")


def _reachable(url: str) -> None:
    """Fail with a sentence instead of a stack trace when the host does not exist.

    ⚠⚠ A DEAD PROJECT REF LOOKS LIKE A NETWORK OUTAGE AND IS NOT ONE. `backend/.env` can outlive the
    project it names — the old hosted dev project was deleted and the file was never updated — and
    the only symptom is `getaddrinfo failed` forty frames deep in httpx. That reads as "my internet
    is broken", which sends you to the wrong place entirely.
    """
    import socket  # noqa: PLC0415
    from urllib.parse import urlparse  # noqa: PLC0415

    host = urlparse(url).hostname or url
    try:
        socket.getaddrinfo(host, 443)
    except OSError:
        sys.exit("\n".join([
            "",
            f"{host} does not resolve — that Supabase project does not exist (any more).",
            "  It came from backend/.env, which can outlive the project it names.",
            "  Take the LIVE values from Railway (backend service -> Variables:",
            "  SUPABASE_URL / SUPABASE_SERVICE_KEY) and pass them explicitly:",
            "",
            "    $env:PROD_SUPABASE_URL='https://<ref>.supabase.co'",
            "    $env:PROD_SUPABASE_SERVICE_KEY='<service-key>'",
            "    uv run python scripts/diagnose_benchmark_coverage.py ACWI",
            "",
        ]))


if __name__ == "__main__":
    print(f"[env] reading {_TARGET}")
    _reachable(_TARGET or "")
    labels = [a for a in sys.argv[1:] if not a.startswith("-")]
    for lab in (labels or ["ACWI"]):
        report(lab)

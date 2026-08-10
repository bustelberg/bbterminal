"""No test may open a real Supabase connection.

⚠ THIS EXISTS BECAUSE THE SUITE WAS QUIETLY QUERYING PRODUCTION. Fourteen tests in
`test_lookthrough.py` called `compute_attribution(2089, ...)` — a hardcoded PROD portfolio id —
with no fake, and nine more in `test_fundamental_coverage.py` / `test_airs_portfolio_analysis.py`
patched one module's `supabase` handle while the code had grown a hop through another module that
holds its own. All 23 passed locally and only failed in CI (2026-07-29).

That asymmetry is the whole problem, and it is structural rather than careless:

    `deps.py` load_dotenv(backend/.env) then load_dotenv(backend/.env.local, override=True)

so a developer machine ALWAYS has credentials and a missing fake is invisible — the test goes
green, having read the production database. CI has no credentials, so the same test raises
`KeyError: 'SUPABASE_URL'` and the gap surfaces there, days later, as someone else's red build.

The guard inverts that: the failure now lands on the machine of whoever wrote the test, on the
first run, with a message naming the fix. It replaces an environment difference — which nobody can
see — with an assertion, which everybody can.

⚠ IT PATCHES `deps.create_client`, NOT THE ENV VARS. Emptying `SUPABASE_URL` would not work:
`deps` re-reads the dotenv files at import and puts them straight back. `create_client` is the one
place `_LazySupabase._build()` goes through, so it is the only chokepoint that cannot be bypassed
by a module reaching for its own handle.

A test that needs to act on data uses `tests/_fake_supabase.py` (extend it — that is its purpose),
or monkeypatches the specific function it calls. A test that genuinely cannot be written either
way is a probe, not a test: run it by hand and say so. Do not add a credentialed CI job — see the
box at the top of CLAUDE.md for why the last two were deleted.
"""
from __future__ import annotations

import pytest


def pytest_collection_modifyitems(items):
    """Give every unmarked test the `fast` tier.

    ⚠ THE DEFAULT IS `fast`, NOT "UNTIERED", AND THAT DIRECTION IS THE WHOLE POINT. Requiring an
    explicit `@pytest.mark.fast` on 2,100 tests would mean a new test written without one is
    silently absent from the loop that is supposed to be everybody's default — a test that never
    runs, which is worse than no test because it reads as coverage. Inverting it makes the failure
    mode loud instead: forget to mark a slow test and the fast tier gets slower, which somebody
    notices the same day.

    A test opts OUT by carrying `slow` (see pyproject's `markers` for what earns it) — so
    `-m fast` and `-m "not fast"` partition the suite with nothing falling between them.
    """
    for item in items:
        if not any(item.get_closest_marker(m) for m in ("fast", "integration", "slow")):
            item.add_marker(pytest.mark.fast)


@pytest.fixture(autouse=True)
def _no_live_supabase(monkeypatch, request):
    """Turn "reaches the database" from an environment-dependent pass into a hard failure."""
    import deps  # noqa: PLC0415

    def _refuse(*_a, **_kw):
        raise RuntimeError(
            f"{request.node.nodeid} tried to open a REAL Supabase connection.\n"
            "Unit tests must not touch a live database — on a developer machine this would "
            "have silently queried whatever backend/.env.local points at (production).\n"
            "Fix it by passing tests/_fake_supabase.py's FakeSupabase in, or by monkeypatching "
            "the helper that reads the DB. If neither is possible, it is a hand-run probe, not "
            "a test."
        )

    monkeypatch.setattr(deps, "create_client", _refuse)


@pytest.fixture(autouse=True)
def _no_live_copy(monkeypatch):
    """⚠⚠ THE COPY TRANSPORT IS A SECOND DOOR TO THE SAME DATABASE, AND THE GUARD ABOVE CANNOT SEE
    IT. `common.pg._run_copy` opens its own `psycopg.connect(SUPABASE_DB_URL)` — it never goes
    through `deps.create_client`, so patching that chokepoint fences PostgREST and leaves direct
    Postgres wide open.

    ⚠ AND IT IS QUIETER THAN THE HOLE IT REOPENED. `_run_copy` catches `Exception` by design (any
    failure → fall back, never raise), so a test reaching the database this way cannot even fail
    loudly; it just returns whatever that database happens to hold. Measured 2026-08-10: three
    readers grew a COPY fast path in front of a PostgREST read the tests fake, and the fast path
    won — `_bulk_blend_rows` asked the live database for company ids 1 and 2, got nothing, and
    returned an authoritative empty list while the fake's rows sat unread; `_fx_to_eur` came back
    with 256 real days of 2024 against the fixture's 200. Same class of bug as the incident in the
    module docstring, one layer down, and with the pass/fail inverted: these went RED on the
    developer machine (which has `SUPABASE_DB_URL`) and stayed green in CI (which does not).

    The fix is to give every test CI's environment: no connection string, therefore
    `copy_path_enabled()` is False and `_run_copy` returns None, therefore each caller takes the
    PostgREST fallback that the fakes actually serve. Patching `_db_url` rather than `_run_copy`
    keeps those two answers consistent — code that branches on `copy_path_enabled()` must not be
    told the path is available and then handed None.

    ⚠ THIS IS NOT "COPY IS UNTESTED". A test that wants the COPY path monkeypatches `_run_copy` (or
    `_db_url`) with its own fixture bytes; that patch is applied after this autouse one and wins.
    What is refused is the *unconfigured, accidental* use — reaching a real server.
    """
    import common.pg as pg  # noqa: PLC0415

    monkeypatch.setattr(pg, "_db_url", lambda: None)

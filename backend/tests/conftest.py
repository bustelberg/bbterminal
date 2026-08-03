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

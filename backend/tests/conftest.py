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

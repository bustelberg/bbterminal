# Verifying changes — tests, tiers, CI

> Split out of `CLAUDE.md` (2026-08-17) to keep the always-loaded file small. Content is verbatim; `CLAUDE.md` carries a one-line index entry pointing here.

**No git hooks** — there are intentionally no pre-commit / pre-push hooks (husky was removed); commits and pushes stay fast, and all gating runs in **CI** on push (see below). Run the local checks yourself when you want fast feedback before pushing:

> ## ⚠ UNIT TESTS ONLY — NOTHING ELSE, ANYWHERE (2026-07-23)
>
> Every test in this repo is a **fast unit test**: `backend/tests/` pytest and colocated
> `frontend/**/*.test.ts` vitest, running in milliseconds against in-memory fakes
> (`tests/_fake_supabase.py`) or frozen fixtures. Nothing else.
>
> **Deleted, do not re-add**: the Playwright e2e suite (`frontend/e2e/`, `playwright.config.ts`,
> the `@playwright/test` dep, the `E2E_BYPASS_AUTH` proxy short-circuit and `lib/authBypass.ts`),
> the CI `backend-stack-smoke` job and `supabase/ci_seed.sql`, a permanently-skipped
> network test in `test_aex_template.py`, and — 2026-07-28 — the **`prod-smoke` workflow**
> (`.github/workflows/prod-smoke.yml`), which curled ~8 live production endpoints on every push to
> `main`, every 6 hours and on demand. It survived the 2026-07-23 sweep because it lived in its own
> workflow file rather than in `ci.yml`, so a grep for the deleted job never found it.
>
> ⚠ **That one was a PRODUCTION MONITOR, not a test of the code being pushed** — it is the only
> thing that was watching whether prod actually answers. Nothing replaces it. If prod breaking
> silently matters, that is an uptime/alerting job (Railway healthcheck, an external pinger), not a
> CI job — do not bring it back into this pipeline.
>
> **Banned**: browser/e2e tests · anything booting Postgres, PostgREST, uvicorn or a Next build ·
> anything hitting the network or a live Supabase · any fixture needing a seeded database ·
> anything probing a deployed environment.
>
> The e2e suite earned this: 5 `/portfolios` specs sat red for weeks against a fixture that no
> longer matched the backend, and a red suite nobody trusts stops being read at all. **When
> something needs verifying, extract a pure function and unit-test it.** If it genuinely cannot be
> unit-tested, verify it by hand and say so — do not build a slow harness.

- **Frontend** (`cd frontend`): `npx tsc --noEmit` (typecheck) · `npm test` (vitest, `*.test.ts` colocated) · `npx eslint <files>`.
- **Backend** (`cd backend`): `uv run pytest tests/` (momentum-engine units) · `uv run ruff check .`. After changing a route/Pydantic model, regenerate the API contract — see **API contract pipeline**.

### The default local loop: `npm run test:fast` (repo root)

One command, both halves: `npm --prefix frontend run test:fast` then `cd backend && uv run pytest -m fast`. **~23s** (frontend ~6s / backend ~17s) against ~34s for everything. Per half: `npm run test:fast` in `frontend/`, `uv run pytest -m fast` in `backend/`.

**Three tiers, declared in `backend/pyproject.toml` (`markers`) and `frontend/vitest.config.ts` (`projects`).** Backend tier = a pytest marker, and `tests/conftest.py::pytest_collection_modifyitems` gives every UNMARKED test `fast` — so a new test is in the default loop by default, and opting out is the deliberate act. Frontend tier = the **filename** (`*.integration.test.ts` / `*.slow.test.ts`, everything else fast), because a marker inside the file would have to be loaded and transformed to discover it should not have run.

- **`fast`** — everything except the below. 2,068 backend + 535 frontend.
- **`integration`** — DB or live API. ⚠ **EMPTY, PERMANENTLY.** The box above bans it and `tests/conftest.py` enforces the DB half at runtime by making `deps.create_client` raise. The tier exists so the word has one fixed meaning to point at when someone proposes one; it is not a slot to fill. `-m integration` / `--project integration` selecting nothing is the right answer forever.
- **`slow`** — ⚠ **NOT "Playwright and scraper", because there are none.** The e2e suite was deleted 2026-07-23, and `playwright` is a runtime dep of `airs_scanner.py` that no test imports. **Measured 2026-08-03, this suite does ZERO I/O** — re-run with every non-loopback `socket.connect` raising, it gave byte-identical results: no DB, no network, no vendor API, and the four `time.sleep` references are already monkeypatched away. So `slow` means the only thing that is slow here: **CPU**. One member — `tests/test_golden_rebalance.py`, ~20s of `run_current_portfolio` genuinely replaying 1,479 companies (loading both 6.8MB `.npz` fixtures is 0.24s of that).

⚠ **A GREEN FAST TIER IS NOT A GREEN SUITE, and the gap is not hypothetical:** the one module `-m fast` skips is the only test that catches sector-aggregation `mean()`→`median()` — all 2,068 others pass under that mutation. Run the full suite before pushing; **CI is unchanged and still runs every tier** (`uv run pytest tests/`, `vitest run` — both select everything, and an empty tier only errors when selected *by name*).

**Parallelism is on by default** — `addopts = "-n auto --dist loadfile --maxprocesses=4"` (pytest-xdist). ⚠ `loadfile` is load-bearing, not a preference: the default `load` scatters `test_golden_rebalance.py`'s 35 tests across workers and each one re-pays the ~7.5s fixture replay, which is why `-n auto` measured *slower* than serial. And ⚠ **more workers is not faster** (each re-imports pandas + FastAPI + every router, ~4s, and just collecting the 2,111 tests costs 8.5s across 4 workers): serial 40.1s · 3 workers 26.6s · 4 workers 28.5s · 14 workers 37.3s. **On a single file this costs ~2s of worker startup** (2.6s → 4.8s) — pass `-n 0` when iterating on one file.

⚠ **Benchmark this suite INTERLEAVED (a, b, a, b), never in blocks.** Run-to-run spread is ~15% and the machine warms under the benchmarking itself: measured back to back, `-n auto --maxprocesses=4` looked a consistent 3s slower than a hardcoded `-n 4` across three runs each, and interleaved the two are identical — as they must be, since both spawn 4/4 workers. A blocked A/B here will hand you a stable-looking difference that does not exist.

⚠ **The fast tier is ~17s, not the 10s it was aimed at, and it cannot get there by tuning.** 8.5s is xdist startup + collection, and the rest is 2,068 tests doing real numpy/pandas work with no I/O to strip out. Reaching 10s would mean tiering the backtest-engine tests out of the default loop — i.e. buying the number by running less of what matters. Not done.

**CI** (`.github/workflows/ci.yml`) gates every push, and is **two jobs, both unit-only**: `backend` (ruff + pytest) and `frontend` (tsc + eslint + vitest), each path-gated. That is the whole pipeline — see the box above for what was removed and why. Nothing in CI boots a database, a browser or a build.


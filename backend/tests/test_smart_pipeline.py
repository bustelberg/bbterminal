"""Smart-pipeline orchestrator (`ingest/phases/pipeline._run_smart_pipeline_sync`).

The planner + schedule-math + momentum phase are unit-tested in isolation
(`test_planner`, `test_next_due_at`, the phase suites). What was missing was a
test of the ORCHESTRATION itself — the conditional wiring that decides which
phases run on a rebalance day vs a quiet day, threads the derived plan through,
keeps a failing phase from aborting the rest, and finalizes the run row.

Every phase the orchestrator calls is a heavy external dependency (template
reconstruction, GuruFocus price fetches, the pandas momentum stream), so we
mock each at its seam in `ingest.phases.pipeline` and assert the orchestrator's
own logic: call order, the `only_keys` / `companies_override` it passes, the
plan enrichment it persists, and the final status.
"""
from __future__ import annotations

import ingest.phases.pipeline as pipeline_mod
from ingest.phases.planner import SmartPlan, StrategyPlan


def _sp(sid, *, due, tkey="ACWI", uid=60):
    """A StrategyPlan; `due` drives the rebalance decision the plan carries."""
    return StrategyPlan(
        strategy_id=sid,
        strategy_name=f"S{sid}",
        frequency="monthly",
        rebalance_weekday=0,
        label=tkey,
        resolved_template_key=tkey,
        resolved_universe_id=uid,
        is_due=due,
        due_reason="due" if due else "not_due",
    )


def _plan(strategies, *, needed=("ACWI",), unresolved=()):
    return SmartPlan(
        as_of="2024-06-01T02:00:00+00:00",
        needed_template_keys=list(needed),
        unresolved_labels=list(unresolved),
        due_strategy_ids=[sp.strategy_id for sp in strategies if sp.is_due],
        strategies=list(strategies),
    )


class _Harness:
    """Records the orchestrator's calls and the final run-row state.

    `events` is the ordered list of phase invocations (with the salient
    argument); `run` is the accumulated `_update_run` field dict (last write
    wins, mirroring the real best-effort checkpoint writes)."""

    def __init__(self):
        self.events: list[tuple] = []
        self.run: dict = {}


def _install(
    monkeypatch,
    *,
    plan: SmartPlan | None,
    plan_raises: bool = False,
    held: list[dict] | None = None,
    universe: list[dict] | None = None,
    fail: set[str] | None = None,
) -> _Harness:
    h = _Harness()
    fail = fail or set()

    def _update_run(_run_id, **fields):
        h.run.update(fields)

    def _build_plan(_now):
        h.events.append(("build_plan",))
        if plan_raises:
            raise RuntimeError("boom-plan")
        return plan

    def _templates(_run_id, only_keys=None):
        h.events.append(("templates", only_keys))
        if "templates" in fail:
            raise RuntimeError("boom-templates")

    def _collect_held(_run_id):
        h.events.append(("collect_held",))
        if "held_collect" in fail:
            raise RuntimeError("boom-held-collect")
        return held or []

    def _collect_universe(due):
        h.events.append(("collect_universe", [sp.strategy_id for sp in due]))
        return universe or []

    def _prices(_run_id, _errors, companies_override=None):
        h.events.append(("prices", [c["cid"] for c in (companies_override or [])]))

    def _momentum(_run_id, plan_arg):
        h.events.append(("momentum", plan_arg))
        if "momentum" in fail:
            raise RuntimeError("boom-momentum")

    monkeypatch.setattr(pipeline_mod, "_update_run", _update_run)
    monkeypatch.setattr(pipeline_mod, "build_plan", _build_plan)
    monkeypatch.setattr(pipeline_mod, "_run_templates_phase", _templates)
    monkeypatch.setattr(pipeline_mod, "_collect_held_companies", _collect_held)
    monkeypatch.setattr(pipeline_mod, "collect_universe_companies", _collect_universe)
    monkeypatch.setattr(pipeline_mod, "_run_prices_phase", _prices)
    monkeypatch.setattr(pipeline_mod, "_run_smart_momentum_phase", _momentum)
    return h


def _names(h: _Harness) -> list[str]:
    return [e[0] for e in h.events]


class TestRebalanceDay:
    """≥1 due strategy + a needed template → full rebalance-day pipeline."""

    def test_runs_every_phase_in_order(self, monkeypatch):
        plan = _plan([_sp(10, due=True), _sp(11, due=False)], needed=("ACWI",))
        h = _install(
            monkeypatch,
            plan=plan,
            held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}],
            universe=[{"cid": 2, "ticker": "B", "exchange": "LSE"},
                      {"cid": 3, "ticker": "C", "exchange": "HKSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        assert _names(h) == [
            "build_plan", "templates", "collect_held", "prices",
            "collect_universe", "prices", "momentum",
        ]

    def test_templates_scoped_to_needed_keys(self, monkeypatch):
        plan = _plan([_sp(10, due=True)], needed=("ACWI",))
        h = _install(monkeypatch, plan=plan, held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}])
        pipeline_mod._run_smart_pipeline_sync(1)

        tmpl = next(e for e in h.events if e[0] == "templates")
        assert tmpl[1] == {"ACWI"}

    def test_universe_collected_from_due_strategies_only(self, monkeypatch):
        plan = _plan([_sp(10, due=True), _sp(11, due=False)], needed=("ACWI",))
        h = _install(
            monkeypatch,
            plan=plan,
            held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}],
            universe=[{"cid": 2, "ticker": "B", "exchange": "LSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        cu = next(e for e in h.events if e[0] == "collect_universe")
        assert cu[1] == [10]  # only the due strategy, not #11
        # Two distinct price refreshes: held first, then the due universe.
        prices = [e[1] for e in h.events if e[0] == "prices"]
        assert prices == [[1], [2]]

    def test_finalizes_ok_and_enriches_plan(self, monkeypatch):
        plan = _plan([_sp(10, due=True)], needed=("ACWI",))
        h = _install(
            monkeypatch,
            plan=plan,
            held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}],
            universe=[{"cid": 2, "ticker": "B", "exchange": "LSE"},
                      {"cid": 3, "ticker": "C", "exchange": "HKSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        assert h.run["status"] == "ok"
        assert h.run["current_phase"] == "done"
        assert h.run.get("finished_at")
        assert h.run.get("error_summary") is None
        # Post-run enrichment the UI reads back off plan_summary.
        assert plan.universes_refreshed == ["ACWI"]
        assert plan.held_company_count == 1
        assert plan.universe_company_count == 2
        assert h.run.get("plan_summary") == plan.to_summary()

    def test_momentum_receives_the_plan(self, monkeypatch):
        plan = _plan([_sp(10, due=True)], needed=("ACWI",))
        h = _install(monkeypatch, plan=plan, held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}])
        pipeline_mod._run_smart_pipeline_sync(1)

        mom = next(e for e in h.events if e[0] == "momentum")
        assert mom[1] is plan


class TestQuietDay:
    """No due strategy → no universe-side work, just held-price + momentum."""

    def test_skips_templates_and_universe_prices(self, monkeypatch):
        plan = _plan([_sp(10, due=False)], needed=("ACWI",))
        h = _install(monkeypatch, plan=plan, held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}])
        pipeline_mod._run_smart_pipeline_sync(1)

        assert _names(h) == ["build_plan", "collect_held", "prices", "momentum"]
        assert "templates" not in _names(h)
        assert "collect_universe" not in _names(h)
        assert plan.universes_refreshed == []  # nothing refreshed
        assert h.run["status"] == "ok"

    def test_no_held_companies_still_finalizes_ok(self, monkeypatch):
        plan = _plan([_sp(10, due=False)], needed=("ACWI",))
        h = _install(monkeypatch, plan=plan, held=[])  # nothing held yet
        pipeline_mod._run_smart_pipeline_sync(1)

        # collect_held runs but the price refresh is skipped on an empty set.
        assert _names(h) == ["build_plan", "collect_held", "momentum"]
        assert h.run["status"] == "ok"
        assert h.run["current_phase"] == "done"


class TestFailureIsolation:
    """A failing phase is captured but never aborts the rest of the run."""

    def test_plan_build_failure_skips_plan_dependent_phases(self, monkeypatch):
        h = _install(
            monkeypatch,
            plan=None,
            plan_raises=True,
            held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        # Held-price refresh is plan-independent and still runs; templates,
        # universe-prices, and momentum are all gated on a plan and skipped.
        assert _names(h) == ["build_plan", "collect_held", "prices"]
        assert "momentum" not in _names(h)
        assert h.run["status"] == "error"
        assert "Plan phase failed" in h.run["error_summary"]

    def test_held_phase_failure_does_not_abort_momentum(self, monkeypatch):
        plan = _plan([_sp(10, due=True)], needed=("ACWI",))
        h = _install(
            monkeypatch,
            plan=plan,
            fail={"held_collect"},
            universe=[{"cid": 2, "ticker": "B", "exchange": "LSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        # held_collect blows up → no held price refresh, but the due universe
        # refresh and the momentum rebalance still run.
        assert _names(h) == [
            "build_plan", "templates", "collect_held",
            "collect_universe", "prices", "momentum",
        ]
        assert h.run["status"] == "error"

    def test_momentum_failure_marks_run_error(self, monkeypatch):
        plan = _plan([_sp(10, due=True)], needed=("ACWI",))
        h = _install(
            monkeypatch,
            plan=plan,
            fail={"momentum"},
            held=[{"cid": 1, "ticker": "A", "exchange": "NYSE"}],
        )
        pipeline_mod._run_smart_pipeline_sync(1)

        assert "momentum" in _names(h)
        assert h.run["status"] == "error"
        assert "Momentum phase failed" in h.run["error_summary"]
        # Even on a failed phase the run is still finalized + plan enriched.
        assert h.run["current_phase"] == "done"
        assert h.run.get("finished_at")

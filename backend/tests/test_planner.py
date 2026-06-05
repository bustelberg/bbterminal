"""Smart-pipeline dependency planner (`ingest/phases/planner.py`).

Pins the two things the orchestrator + UI depend on:
  1. Universe resolution from each strategy's config (template_key first,
     then static `universe.label`, else unresolved) + derived-template
     parent expansion, deduped across strategies.
  2. The per-strategy due decision off `next_due_at`.
Plus `collect_universe_companies` pooling the latest-month membership of
the due strategies' universes (deduped by universe).

Driven against the in-memory `FakeSupabase` + a monkeypatched template
registry — no Postgres, no real templates.
"""
from __future__ import annotations

from datetime import datetime, timezone

import index_universe.templates as templates_mod
import ingest.phases.planner as planner_mod
from ingest.phases.planner import StrategyPlan, build_plan, collect_universe_companies

from tests._fake_supabase import FakeSupabase

_NOW = datetime(2024, 6, 1, 2, 0, tzinfo=timezone.utc)
_PAST = "2024-05-01T02:00:00+00:00"
_FUTURE = "2024-07-01T02:00:00+00:00"


def _fake_templates():
    """A registry whose templates just report a fixed universe_id."""
    def _mk(key, uid):
        return lambda: type("T", (), {"template_key": key, "universe_id": lambda self, _sb: uid})()
    return {
        "LEONTEQ": _mk("LEONTEQ", 50),
        "ACWI": _mk("ACWI", 60),
        "ACWI_LEONTEQ": _mk("ACWI_LEONTEQ", 70),
    }


def _strat(sid, label, next_due, *, field="index_universe", weekday=0, freq="monthly"):
    return {
        "id": sid,
        "name": f"S{sid}",
        "frequency": freq,
        "config": {field: label, "rebalance_weekday": weekday},
        "next_due_at": next_due,
        "enabled": True,
        "created_at": f"2024-01-0{sid}",
    }


class TestBuildPlan:
    def _run(self, monkeypatch, strategies, universe_rows=None):
        fake = FakeSupabase(tables={
            "scheduled_strategy": strategies,
            "universe": universe_rows or [],
        })
        monkeypatch.setattr(planner_mod, "supabase", fake)
        monkeypatch.setattr(templates_mod, "TEMPLATES", _fake_templates())
        return build_plan(_NOW)

    def test_template_resolution_and_due_decision(self, monkeypatch):
        plan = self._run(monkeypatch, [
            _strat(1, "LEONTEQ", _PAST),     # due
            _strat(2, "LEONTEQ", _FUTURE),   # not due, shares universe
            _strat(3, "LEONTEQ", None),      # first_run → due
        ])
        assert plan.needed_template_keys == ["LEONTEQ"]  # deduped
        assert plan.unresolved_labels == []
        assert set(plan.due_strategy_ids) == {1, 3}
        by_id = {sp.strategy_id: sp for sp in plan.strategies}
        assert by_id[1].is_due and by_id[1].due_reason == "due"
        assert not by_id[2].is_due and by_id[2].due_reason == "not_due"
        assert by_id[3].is_due and by_id[3].due_reason == "first_run"
        assert by_id[1].resolved_template_key == "LEONTEQ"
        assert by_id[1].resolved_universe_id == 50

    def test_static_universe_label_resolves_without_template(self, monkeypatch):
        plan = self._run(
            monkeypatch,
            [_strat(1, "longequity", _PAST, field="universe_label")],
            universe_rows=[{"label": "longequity", "universe_id": 99}],
        )
        sp = plan.strategies[0]
        assert sp.resolved_template_key is None
        assert sp.resolved_universe_id == 99
        assert plan.needed_template_keys == []  # static universe is never refreshed
        assert plan.unresolved_labels == []

    def test_unresolved_label_is_surfaced_not_fatal(self, monkeypatch):
        plan = self._run(monkeypatch, [_strat(1, "NOPE", _PAST)])
        sp = plan.strategies[0]
        assert sp.resolved_template_key is None and sp.resolved_universe_id is None
        assert plan.unresolved_labels == ["NOPE"]
        assert sp.due_reason == "unresolved"  # still due → still runs (errors per-strategy)

    def test_derived_template_pulls_in_parents(self, monkeypatch):
        plan = self._run(monkeypatch, [_strat(1, "ACWI_LEONTEQ", _PAST)])
        assert plan.needed_template_keys == ["ACWI", "ACWI_LEONTEQ", "LEONTEQ"]

    def test_no_enabled_strategies(self, monkeypatch):
        plan = self._run(monkeypatch, [])
        assert plan.strategies == []
        assert plan.needed_template_keys == []
        assert plan.due_strategy_ids == []


class TestCollectUniverseCompanies:
    def test_pools_latest_month_deduped_by_universe(self, monkeypatch):
        membership = [
            # universe 50 — older month (ignored) + latest month
            {"universe_id": 50, "target_month": "2024-04", "company_id": 1},
            {"universe_id": 50, "target_month": "2024-05", "company_id": 1},
            {"universe_id": 50, "target_month": "2024-05", "company_id": 2},
            # universe 60 — latest month
            {"universe_id": 60, "target_month": "2024-05", "company_id": 3},
        ]
        company = [
            {"company_id": 1, "gurufocus_ticker": "AAA", "gurufocus_exchange": {"exchange_code": "NYSE"}},
            {"company_id": 2, "gurufocus_ticker": "BBB", "gurufocus_exchange": {"exchange_code": "HKSE"}},
            {"company_id": 3, "gurufocus_ticker": "CCC", "gurufocus_exchange": {"exchange_code": "LSE"}},
            {"company_id": 9, "gurufocus_ticker": "ZZZ", "gurufocus_exchange": {"exchange_code": "NYSE"}},
        ]
        fake = FakeSupabase(tables={"universe_membership": membership, "company": company})
        monkeypatch.setattr(planner_mod, "supabase", fake)

        due = [
            StrategyPlan(1, "A", "monthly", 0, "LEONTEQ", "LEONTEQ", 50, True, "due"),
            StrategyPlan(2, "B", "monthly", 0, "LEONTEQ", "LEONTEQ", 50, True, "due"),  # same universe
            StrategyPlan(3, "C", "monthly", 0, "ACWI", "ACWI", 60, True, "due"),
        ]
        out = collect_universe_companies(due)
        cids = sorted(c["cid"] for c in out)
        assert cids == [1, 2, 3]  # latest month of u50 (1,2) + u60 (3); old month's rows excluded
        assert {c["cid"]: c["exchange"] for c in out}[2] == "HKSE"

    def test_no_resolvable_universe_returns_empty(self, monkeypatch):
        fake = FakeSupabase(tables={"universe_membership": [], "company": []})
        monkeypatch.setattr(planner_mod, "supabase", fake)
        due = [StrategyPlan(1, "A", "monthly", 0, "NOPE", None, None, True, "unresolved")]
        assert collect_universe_companies(due) == []

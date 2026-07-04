"""Shared-selection clone helpers in the momentum rebalance phase.

Multiple scheduled strategies that run the SAME base strategy and differ only in
their ETF overlay / cash share one momentum selection (`strategy_hash` excludes
overlay + cash). The phase computes that selection ONCE and clones it for the
rest via `_clone_rebalance_snapshot`, applying each strategy's overlay/cash on
top. These pin the clone mechanics: the base stock picks + dates + strategy_hash
are copied verbatim, the clone is tagged to the RIGHT strategy + run, and it
carries THAT strategy's own config (so the re-pricer's cash fallback is correct).
"""
from __future__ import annotations

import ingest.phases.momentum as momentum


class _Result:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, store):
        self._store = store
        self._mode = "select"
        self._payload = None
        self._eq: dict = {}

    def select(self, *_a, **_k):
        self._mode = "select"
        return self

    def insert(self, payload):
        self._mode, self._payload = "insert", payload
        return self

    def eq(self, col, val):
        self._eq[col] = val
        return self

    def limit(self, _n):
        return self

    def execute(self):
        if self._mode == "insert":
            row = dict(self._payload)
            row["snapshot_id"] = self._store.next_id()
            self._store.rows.append(row)
            return _Result([dict(row)])
        rows = [r for r in self._store.rows
                if all(r.get(c) == v for c, v in self._eq.items())]
        return _Result([dict(r) for r in rows])


class _Fake:
    def __init__(self):
        self.rows: list[dict] = []
        self._id = 100

    def next_id(self) -> int:
        self._id += 1
        return self._id

    def table(self, _name):
        return _Query(self)


def _seed_base(fake):
    fake.rows.append({
        "snapshot_id": 1,
        "holdings": [{"company_id": 10, "weight": 0.5, "entry_date": "2026-07-02"},
                     {"company_id": 11, "weight": 0.5, "entry_date": "2026-07-02"}],
        "as_of_date": "2026-07-06",
        "latest_price_date": "2026-07-02",
        "daily_picks": [{"date": "2026-07-02"}],
        "strategy_hash": "abc123",
        "name": "Momentum base",
        "config": {"etf_overlay": [], "cash_pct": 0.0},   # the FIRST strategy's config
    })


def test_read_base_snapshot_returns_selection_fields(monkeypatch):
    fake = _Fake()
    _seed_base(fake)
    monkeypatch.setattr(momentum, "supabase", fake)
    base = momentum._read_base_snapshot(1)
    assert base["strategy_hash"] == "abc123"
    assert base["as_of_date"] == "2026-07-06"
    assert base["latest_price_date"] == "2026-07-02"
    assert len(base["holdings"]) == 2


def test_clone_copies_selection_and_tags_this_strategy(monkeypatch):
    fake = _Fake()
    _seed_base(fake)
    monkeypatch.setattr(momentum, "supabase", fake)
    base = momentum._read_base_snapshot(1)

    # This strategy adds a 30% ETF sleeve + 10% cash — its OWN config.
    this_config = {"etf_overlay": [{"benchmark_id": 7, "weight_pct": 30}], "cash_pct": 0.1}
    new_id = momentum._clone_rebalance_snapshot(base, strategy_id=20, run_id=5, config=this_config)

    assert new_id != 1  # a distinct snapshot, not the base
    cloned = next(r for r in fake.rows if r["snapshot_id"] == new_id)
    # Same selection cloned verbatim.
    assert cloned["holdings"] == base["holdings"]
    assert cloned["strategy_hash"] == "abc123"
    assert cloned["as_of_date"] == "2026-07-06"
    assert cloned["daily_picks"] == base["daily_picks"]
    # Tagged to THIS strategy + run, as a rebalance.
    assert cloned["scheduled_strategy_id"] == 20
    assert cloned["ingest_run_id"] == 5
    assert cloned["kind"] == "rebalance"
    assert cloned["triggered_by"] == "auto"
    # Carries THIS strategy's config (not the base's) so the re-pricer reads the
    # right cash % / overlay for it.
    assert cloned["config"] == this_config
    assert cloned["config"]["cash_pct"] == 0.1

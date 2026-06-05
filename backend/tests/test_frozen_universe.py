"""Frozen (static-snapshot) universes: prune must protect their members so a
pinned, reproducible snapshot can't rot when companies leave the live template
it was copied from."""
from __future__ import annotations

from ingest.prune_companies import (
    _kept_union,
    _load_frozen_universe_company_ids,
)
from tests._fake_supabase import FakeSupabase


def _make_fake() -> FakeSupabase:
    universe = [
        {"universe_id": 1, "label": "LEONTEQ", "template_key": "LEONTEQ", "frozen_at": None},
        {"universe_id": 2, "label": "LEONTEQ (as of 2026-06-05)", "template_key": None,
         "frozen_at": "2026-06-05T00:00:00Z"},
        {"universe_id": 3, "label": "MyScreen", "template_key": None, "frozen_at": None},
    ]
    membership = [
        # Frozen snapshot (uid 2) holds 100 + 101...
        {"universe_id": 2, "company_id": 100, "target_month": "2026-05"},
        {"universe_id": 2, "company_id": 101, "target_month": "2026-05"},
        {"universe_id": 2, "company_id": 100, "target_month": "2026-06"},  # dup across months
        # ...but the LIVE LEONTEQ (uid 1) has since dropped 101.
        {"universe_id": 1, "company_id": 100, "target_month": "2026-06"},
        # A user-criteria (non-frozen) universe member.
        {"universe_id": 3, "company_id": 200, "target_month": "2026-06"},
    ]
    return FakeSupabase(tables={"universe": universe, "universe_membership": membership})


class TestFrozenUniverseProtection:
    def test_loads_distinct_members_of_frozen_universes_only(self):
        fake = _make_fake()
        # uid 2 (frozen) → {100, 101}; uid 1 (live template) + uid 3 (user) excluded.
        assert _load_frozen_universe_company_ids(fake) == {100, 101}

    def test_kept_union_protects_names_that_left_the_live_template(self):
        fake = _make_fake()
        kept = _kept_union(fake)
        # 101 is gone from live LEONTEQ but still in the frozen snapshot → kept,
        # so prune won't delete it (and cascade-drop its prices).
        assert 101 in kept
        assert 100 in kept
        # A non-frozen user universe member isn't protected by the frozen path.
        assert 200 not in kept

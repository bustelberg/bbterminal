"""Smart-pipeline dependency planner.

Derives, from the set of ENABLED scheduled strategies, exactly what the
daily tick needs to do — so the pipeline refreshes only the universes
those strategies use and rebalances only the strategies that are due,
instead of refreshing the whole world on a fixed calendar.

`build_plan(now)` returns a `SmartPlan`:
  * `needed_template_keys` — the template-managed universes any enabled
    strategy uses (deduped; derived templates pull in their parents).
  * `unresolved_labels` — universe labels we couldn't resolve to either a
    template or a static `universe` row (the strategy still runs and
    errors per-strategy, exactly as before — we just surface it).
  * per-strategy `StrategyPlan` with the resolved universe + the due
    decision (`is_due`/`due_reason`), so the smart momentum phase and the
    /schedule UI read one consistent plan.

`collect_universe_companies(due)` returns the `[{cid,ticker,exchange}]`
list (the shape `_run_prices_phase(companies_override=...)` expects) for
the union of the DUE strategies' universes — the full-universe refresh
that runs just before a rebalance so newly-eligible names have price
history before they're scored.

Universe resolution mirrors
`routers.momentum.backtest_stream.universe_loader._load_index_universe`
exactly (template_key first, then static `universe.label`) so the plan
can never diverge from what the compute path actually reads.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime

from deps import fetch_in_chunks, paginate, supabase

_log = logging.getLogger(__name__)

# Derived templates whose refresh reads from parent universes. The planner
# expands needed keys to include the parents (and orders them first in the
# templates phase) so an ACWI_LEONTEQ strategy refreshes ACWI + LEONTEQ
# before the intersection is rebuilt. The only derived template today.
_TEMPLATE_PARENTS: dict[str, tuple[str, ...]] = {
    "ACWI_LEONTEQ": ("ACWI", "LEONTEQ"),
}


@dataclass
class StrategyPlan:
    strategy_id: int
    strategy_name: str
    frequency: str | None
    rebalance_weekday: int
    label: str | None  # raw index_universe/universe_label from the config
    resolved_template_key: str | None
    resolved_universe_id: int | None
    is_due: bool
    due_reason: str  # "first_run" | "due" | "not_due" | "unresolved"


@dataclass
class SmartPlan:
    as_of: str
    needed_template_keys: list[str] = field(default_factory=list)
    unresolved_labels: list[str] = field(default_factory=list)
    due_strategy_ids: list[int] = field(default_factory=list)
    strategies: list[StrategyPlan] = field(default_factory=list)
    # Filled in by the orchestrator after the phases run, for the UI.
    universes_refreshed: list[str] = field(default_factory=list)
    held_company_count: int | None = None
    universe_company_count: int | None = None

    def to_summary(self) -> dict:
        """JSON-serializable dict for `ingest_run.plan_summary`."""
        return asdict(self)


def _config_label(config: dict) -> str | None:
    """The universe label a strategy's config selects, mirroring
    BacktestRequest precedence: `index_universe` first, then
    `universe_label`."""
    return config.get("index_universe") or config.get("universe_label")


def _resolve_universe(label: str) -> tuple[str | None, int | None]:
    """Resolve a universe label to (template_key, universe_id).

    Mirrors `_load_index_universe`: try `template_key == label` first (the
    canonical template-managed row), then fall back to `label == label`
    (static universes like 'longequity'/SP500). Returns (None, None) when
    neither matches.
    """
    from index_universe.templates import TEMPLATES  # noqa: PLC0415

    if label in TEMPLATES:
        # Template-managed: its universe row may not exist yet in a fresh
        # env (universe_id None) — that's fine, the templates phase ensures
        # it on refresh.
        try:
            uid = TEMPLATES[label]().universe_id(supabase)
        except Exception as e:
            _log.warning("[planner] universe_id(%s) failed: %s: %s", label, type(e).__name__, e)
            uid = None
        return label, uid

    # Static universe by label (no template → never refreshed, read-only).
    try:
        resp = (
            supabase.table("universe")
            .select("universe_id")
            .eq("label", label)
            .limit(1)
            .execute()
        )
        if resp.data:
            return None, int(resp.data[0]["universe_id"])
    except Exception as e:
        _log.warning("[planner] universe lookup for label=%r failed: %s: %s", label, type(e).__name__, e)
    return None, None


def build_plan(now: datetime) -> SmartPlan:
    """Derive the smart plan from every enabled scheduled strategy."""
    now_iso = now.isoformat()
    plan = SmartPlan(as_of=now_iso)

    resp = (
        supabase.table("scheduled_strategy")
        .select("id, name, frequency, config, next_due_at")
        .eq("enabled", True)
        .order("created_at")
        .execute()
    )
    rows = resp.data or []

    needed: set[str] = set()
    unresolved: set[str] = set()
    due_ids: list[int] = []

    for r in rows:
        sid = int(r["id"])
        config = dict(r.get("config") or {})
        label = _config_label(config)
        weekday = int(config.get("rebalance_weekday", 0) or 0)
        next_due_iso = r.get("next_due_at")

        template_key: str | None = None
        universe_id: int | None = None
        if label:
            template_key, universe_id = _resolve_universe(label)
            if template_key is not None:
                needed.add(template_key)
                for parent in _TEMPLATE_PARENTS.get(template_key, ()):  # parents first
                    needed.add(parent)
            elif universe_id is None:
                unresolved.add(label)

        # Due decision — pure timestamp check (all weekday/anchor math lives
        # in `compute_next_due_at`, which stamped `next_due_at`).
        if next_due_iso is None:
            is_due, reason = True, "first_run"
        elif str(next_due_iso) <= now_iso:
            is_due, reason = True, "due"
        else:
            is_due, reason = False, "not_due"
        if label and template_key is None and universe_id is None:
            # Strategy still runs (errors per-strategy in momentum), but its
            # universe can't be refreshed/collected.
            reason = "unresolved" if is_due else reason
        if is_due:
            due_ids.append(sid)

        plan.strategies.append(StrategyPlan(
            strategy_id=sid,
            strategy_name=r.get("name") or f"Strategy #{sid}",
            frequency=r.get("frequency"),
            rebalance_weekday=weekday,
            label=label,
            resolved_template_key=template_key,
            resolved_universe_id=universe_id,
            is_due=is_due,
            due_reason=reason,
        ))

    plan.needed_template_keys = sorted(needed)
    plan.unresolved_labels = sorted(unresolved)
    plan.due_strategy_ids = due_ids
    return plan


def _latest_membership_company_ids(universe_id: int) -> set[int]:
    """Company ids in the LATEST captured `target_month` of a universe.
    Paginated to bypass the PostgREST max-rows cap (ACWI's latest month is
    ~2k rows — see `project_postgrest_max_rows_trap`)."""
    try:
        latest = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", universe_id)
            .order("target_month", desc=True)
            .limit(1)
            .execute()
        )
        if not latest.data:
            return set()
        month = latest.data[0]["target_month"]
    except Exception as e:
        _log.warning("[planner] latest-month probe for universe=%s failed: %s: %s", universe_id, type(e).__name__, e)
        return set()

    cids: set[int] = set()
    for r in paginate(
        lambda lo, hi: supabase.table("universe_membership")
        .select("company_id")
        .eq("universe_id", universe_id)
        .eq("target_month", month)
        .range(lo, hi)
        .execute()
    ):
        cid = r.get("company_id")
        if cid is not None:
            cids.add(int(cid))
    return cids


def collect_universe_companies(due: list[StrategyPlan]) -> list[dict]:
    """Pool the latest-month membership of every DUE strategy's universe
    into the `[{cid,ticker,exchange}]` list `_run_prices_phase` expects.

    Universes are deduped (strategies sharing one universe collect it
    once). Strategies with no resolvable universe are skipped silently —
    they'll error in the momentum phase as before."""
    universe_ids: set[int] = set()
    for sp in due:
        if sp.resolved_universe_id is not None:
            universe_ids.add(sp.resolved_universe_id)

    if not universe_ids:
        return []

    company_ids: set[int] = set()
    for uid in universe_ids:
        company_ids |= _latest_membership_company_ids(uid)
    if not company_ids:
        return []

    out: list[dict] = []
    for r in fetch_in_chunks(
        list(company_ids),
        lambda chunk: supabase.table("company")
        .select(
            "company_id, gurufocus_ticker, "
            "gurufocus_exchange:gurufocus_exchange(exchange_code)"
        )
        .in_("company_id", chunk)
        .execute(),
    ):
        exch = (r.get("gurufocus_exchange") or {}).get("exchange_code") or ""
        ticker = r.get("gurufocus_ticker") or ""
        if not ticker or not exch:
            continue
        out.append({
            "cid": int(r["company_id"]),
            "ticker": ticker,
            "exchange": exch,
        })
    return out

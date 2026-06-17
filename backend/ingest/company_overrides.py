"""Manual company-resolution overrides — the dedup layer for cases the ISIN +
name passes can't catch, re-applied on every ingest (prune phase + after the
ISIN backfill). See migration `20260617002000_company_override.sql`.

  - `alias`   — two listings of the same issuer with DIFFERENT ISINs (an ADR vs
                its home-market line). `dedupe_by_isin` can't see them (different
                ISIN) and the name passes can't (both have ISINs / names differ).
                The pass merges the `isin` company INTO the `canonical_isin`
                company: memberships + market cap move, but the secondary's
                PRICES are dropped (different listing/currency — the keeper keeps
                its own). Auto-recorded via `record_isin_alias` when you
                consolidate cross-ISIN, so the decision sticks across index
                reconstructions.
  - `exclude` — a real-but-unwanted constituent (e.g. an NSE listing with no
                ISIN). Matched by `isin`, else `(ticker, exchange)`. The pass
                marks it `out_of_scope_at` (idempotent), so it stays suppressed
                even after an index reconstruction re-creates it.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from supabase import Client

log = logging.getLogger(__name__)

_MC_COLS = ("market_cap_eur", "market_cap_native", "market_cap_currency",
           "market_cap_fx_rate", "market_cap_date")


@dataclass
class OverrideReport:
    aliases_merged: int = 0
    rows_deleted: int = 0
    excluded_marked: int = 0
    actions: list[str] = field(default_factory=list)


def record_isin_alias(supabase: Client, secondary_isin: str | None,
                      canonical_isin: str | None, note: str | None = None) -> None:
    """Remember that `secondary_isin` is the SAME issuer as `canonical_isin`
    (keep the canonical). Idempotent upsert — call this whenever you merge two
    companies that carry different ISINs, so the next ingest re-applies it if the
    secondary listing is re-created."""
    s = (secondary_isin or "").strip()
    c = (canonical_isin or "").strip()
    if not s or not c or s == c:
        return
    existing = (
        supabase.table("company_override").select("id")
        .eq("kind", "alias").eq("isin", s).limit(1).execute()
    ).data
    payload = {"kind": "alias", "isin": s, "canonical_isin": c, "note": note}
    if existing:
        supabase.table("company_override").update(payload).eq("id", existing[0]["id"]).execute()
    else:
        supabase.table("company_override").insert(payload).execute()


def add_exclusion(supabase: Client, *, isin: str | None = None, ticker: str | None = None,
                  exchange: str | None = None, note: str | None = None) -> None:
    """Record an unwanted constituent so the override pass keeps it out of scope.
    Match by `isin` if it has one, else by `(ticker, exchange)`."""
    supabase.table("company_override").insert({
        "kind": "exclude", "isin": isin, "ticker": ticker, "exchange": exchange, "note": note,
    }).execute()


def _company_by_isin(supabase: Client, isin: str | None) -> dict | None:
    if not isin:
        return None
    cols = "company_id, company_name, out_of_scope_at, " + ", ".join(_MC_COLS)
    r = supabase.table("company").select(cols).eq("isin", isin).limit(2).execute().data
    return r[0] if r else None


def _company_by_ticker_exchange(supabase: Client, ticker: str | None, exchange: str | None) -> dict | None:
    if not ticker:
        return None
    rows = (
        supabase.table("company")
        .select("company_id, out_of_scope_at, gurufocus_exchange:gurufocus_exchange(exchange_code)")
        .eq("gurufocus_ticker", ticker).execute()
    ).data or []
    for r in rows:
        if (r.get("gurufocus_exchange") or {}).get("exchange_code") == exchange:
            return r
    return None


def _merge_alias(supabase: Client, loser: dict, winner: dict, report: OverrideReport) -> None:
    """Fold the secondary listing into the canonical: move memberships + side
    FKs + (if the keeper lacks one) the market cap, then DROP the secondary's
    prices (wrong listing/currency) and delete it. Not `_reassign_and_delete` —
    that would move the secondary's prices onto the keeper."""
    from ingest.dedupe import _move_simple_fk  # noqa: PLC0415
    lid, wid = loser["company_id"], winner["company_id"]
    if not winner.get("market_cap_eur") and loser.get("market_cap_eur"):
        supabase.table("company").update({k: loser.get(k) for k in _MC_COLS}).eq("company_id", wid).execute()
    _move_simple_fk(supabase, "universe_membership", lid, wid, dedup_keys=["universe_id", "target_month"])
    _move_simple_fk(supabase, "portfolio_weight", lid, wid, dedup_keys=["portfolio_id"])
    try:
        _move_simple_fk(supabase, "company_source", lid, wid, dedup_keys=["source_code"])
    except Exception:  # noqa: BLE001
        pass
    try:
        supabase.table("leonteq_equity").update({"company_id": wid}).eq("company_id", lid).execute()
    except Exception:  # noqa: BLE001
        pass
    for t in ("portfolio_weight", "metric_data", "universe_membership"):
        supabase.table(t).delete().eq("company_id", lid).execute()
    supabase.table("company").delete().eq("company_id", lid).execute()
    report.rows_deleted += 1


def merge_companies(supabase: Client, loser_id: int, winner_id: int, *,
                    drop_loser_prices: bool = False, record_alias: bool = True,
                    note: str | None = None) -> None:
    """Canonical company merge: rewire every reference from `loser_id` onto
    `winner_id`, delete the loser, and — when both carry (different) ISINs and
    `record_alias` — record the loser->winner ISIN alias so a re-created loser is
    auto-folded on the next ingest. THE entry point for consolidating two rows
    (use it instead of hand-rolling FK moves) so cross-ISIN decisions stick.

    `drop_loser_prices=True` discards the loser's `metric_data` instead of moving
    it — use when the two are DIFFERENT listings (an ADR vs its home line) whose
    prices/currencies must not mix; the keeper repopulates from its own listing."""
    pair = (
        supabase.table("company").select("company_id, isin, " + ", ".join(_MC_COLS) +
                                          ", company_name, out_of_scope_at")
        .in_("company_id", [loser_id, winner_id]).execute()
    ).data or []
    by_id = {c["company_id"]: c for c in pair}
    loser, winner = by_id.get(loser_id), by_id.get(winner_id)
    if not loser or not winner or loser_id == winner_id:
        return
    if record_alias:
        record_isin_alias(supabase, loser.get("isin"), winner.get("isin"), note)
    if drop_loser_prices:
        _merge_alias(supabase, loser, winner, OverrideReport())
    else:
        from ingest.dedupe import _reassign_and_delete, MergeReport  # noqa: PLC0415
        _reassign_and_delete(supabase, loser_id, winner_id, MergeReport())


def apply_company_overrides(supabase: Client, *, dry_run: bool = False) -> OverrideReport:
    """Apply every `company_override` row. Idempotent: alias merges no-op once
    the secondary is gone; excludes no-op once `out_of_scope_at` is set."""
    report = OverrideReport()
    rows = (supabase.table("company_override").select("*").execute()).data or []
    now_iso = datetime.now(timezone.utc).isoformat()

    for o in rows:
        if o["kind"] == "alias":
            loser = _company_by_isin(supabase, o.get("isin"))
            winner = _company_by_isin(supabase, o.get("canonical_isin"))
            if not loser or not winner or loser["company_id"] == winner["company_id"]:
                continue
            report.actions.append(
                f'alias: merge cid={loser["company_id"]} (isin {o.get("isin")}) -> '
                f'cid={winner["company_id"]} (isin {o.get("canonical_isin")})'
            )
            report.aliases_merged += 1
            if dry_run:
                report.rows_deleted += 1
                continue
            _merge_alias(supabase, loser, winner, report)

        elif o["kind"] == "exclude":
            target = (_company_by_isin(supabase, o.get("isin")) if o.get("isin")
                      else _company_by_ticker_exchange(supabase, o.get("ticker"), o.get("exchange")))
            if not target or target.get("out_of_scope_at"):
                continue
            report.actions.append(
                f'exclude: mark cid={target["company_id"]} out_of_scope '
                f'({o.get("isin") or f"{o.get('ticker')}/{o.get('exchange')}"})'
            )
            report.excluded_marked += 1
            if dry_run:
                continue
            supabase.table("company").update({
                "out_of_scope_at": now_iso,
                "out_of_scope_reason": o.get("note") or "excluded via company_override",
                # out-of-scope supersedes the "wrong exchange, go fix it" flag.
                "gurufocus_lookup_failed_at": None,
            }).eq("company_id", target["company_id"]).execute()

    return report

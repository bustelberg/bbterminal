"""Universe template abstract base class.

A `UniverseTemplate` is a self-updating universe definition: one
canonical `universe` row (keyed by `template_key`) that the pipeline
continuously refreshes by walking the template's data source and
reconstructing per-month membership. The template owns its hard-stop
earliest date — older months are by definition unavailable.

Templates are stateless; instantiate freely. Each instance can read
from / write to Supabase via the passed-in client.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable

from supabase import Client

from deps import IN_CHUNK_SIZE
from ._cache import (
    invalidate_template as _invalidate_template_cache,
    membership_cache as _membership_cache,
)

# `(message, pct)` — pct may be None when the phase isn't tracking
# progress numerically (e.g. inside a long reconstruction loop where
# only message-style status makes sense).
ProgressCallback = Callable[[str, "int | None"], None]


@dataclass
class TemplateDiff:
    """The per-month diff a refresh produces, serialized into
    `ingest_run.templates_summary` as one array entry. Same field shape
    the old `acwi_summary` had, plus `template_key` and `universe_id` so
    the UI can find the right universe to query for membership."""
    template_key: str
    universe_id: int
    this_month: str
    prev_month: str | None
    additions_count: int
    removals_count: int
    renames_count: int
    additions: list[dict] = field(default_factory=list)
    removals: list[dict] = field(default_factory=list)
    renames: list[dict] = field(default_factory=list)

    def to_summary_entry(self) -> dict:
        return {
            "template_key": self.template_key,
            "universe_id": self.universe_id,
            "this_month": self.this_month,
            "prev_month": self.prev_month,
            "additions_count": self.additions_count,
            "removals_count": self.removals_count,
            "renames_count": self.renames_count,
            "additions": self.additions,
            "removals": self.removals,
            "renames": self.renames,
        }


@dataclass
class RefreshResult:
    """Returned from `UniverseTemplate.refresh()`. Carries the diff (for
    `templates_summary`) plus aggregates the pipeline phase may want to
    log (`months_written`)."""
    template_key: str
    universe_id: int
    months_written: int
    diff: TemplateDiff


class UniverseTemplate(ABC):
    """Subclass and override `template_key`, `label`, `description`,
    `earliest_date`, and implement `refresh()`. The default
    `membership_at()` / `available_months()` implementations work for
    any subclass that persists to the standard `universe_membership`
    table — override only if your template uses a different store."""

    # Class attributes — set by subclasses.
    template_key: str = ""
    label: str = ""
    description: str = ""
    earliest_date: date = date(1900, 1, 1)

    # ── Override in subclass ───────────────────────────────────────

    @abstractmethod
    def refresh(
        self,
        supabase: Client,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> RefreshResult:
        """Bring the canonical universe up to today. Idempotent: calling
        multiple times converges on the same final state.

        Implementations must ensure the canonical `universe` row exists
        (with `template_key` set) before writing membership."""
        raise NotImplementedError

    # ── Default read paths ─────────────────────────────────────────

    def ensure_universe_row(self, supabase: Client) -> int:
        """Get-or-create the canonical universe row keyed by
        `template_key`. Returns the universe_id. Idempotent."""
        resp = (
            supabase.table("universe")
            .select("universe_id")
            .eq("template_key", self.template_key)
            .limit(1)
            .execute()
        )
        if resp.data:
            return int(resp.data[0]["universe_id"])
        # Fall back: a row with this label may pre-exist without the
        # template_key set (e.g. from a partial migration). Adopt it.
        resp = (
            supabase.table("universe")
            .select("universe_id")
            .eq("label", self.label)
            .limit(1)
            .execute()
        )
        if resp.data:
            uid = int(resp.data[0]["universe_id"])
            supabase.table("universe").update({
                "template_key": self.template_key,
                "description": self.description,
            }).eq("universe_id", uid).execute()
            return uid
        ins = supabase.table("universe").insert({
            "label": self.label,
            "description": self.description,
            "template_key": self.template_key,
        }).execute()
        return int(ins.data[0]["universe_id"])

    def universe_id(self, supabase: Client) -> int | None:
        """Read-only lookup — returns the canonical universe_id or None
        if the template hasn't been refreshed yet (no row exists)."""
        resp = (
            supabase.table("universe")
            .select("universe_id")
            .eq("template_key", self.template_key)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        return int(resp.data[0]["universe_id"])

    def last_refreshed_at(self, supabase: Client) -> str | None:
        """ISO timestamp of the most recent successful `refresh()`. Used
        as the cache-version + HTTP-ETag input. Returns None when the
        template has never been refreshed (no row, or row predates the
        column)."""
        resp = (
            supabase.table("universe")
            .select("last_refreshed_at")
            .eq("template_key", self.template_key)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        return resp.data[0].get("last_refreshed_at")

    def mark_refreshed(self, supabase: Client, universe_id: int) -> str:
        """Bump `universe.last_refreshed_at` to now() and invalidate the
        in-process caches for this template. Called by subclass
        `refresh()` implementations as their last step."""
        now_iso = datetime.now(timezone.utc).isoformat()
        supabase.table("universe").update(
            {"last_refreshed_at": now_iso}
        ).eq("universe_id", universe_id).execute()
        _invalidate_template_cache(self.template_key)
        return now_iso

    def available_months(self, supabase: Client) -> list[str]:
        """Distinct `target_month` values for this template's canonical
        universe, ascending. Empty list when no membership exists yet
        (template not yet refreshed). One RPC round-trip via
        `universe_available_months` — the previous Python paginate-then-dedupe
        was O(holdings) when only the O(months) distinct values matter."""
        uid = self.universe_id(supabase)
        if uid is None:
            return []
        resp = supabase.rpc(
            "universe_available_months", {"p_universe_id": uid}
        ).execute()
        return [(r.get("target_month") or "")[:7] for r in (resp.data or []) if r.get("target_month")]

    def membership_at(
        self, supabase: Client, target_month: str,
    ) -> list[dict]:
        """Returns all holdings active in `target_month` (format
        'YYYY-MM'), each row joined with company info so the caller can
        render ticker / name / exchange / sector / GuruFocus link
        without an extra fetch.

        Cached: repeat calls for the same month return the cached list
        with no DB roundtrip until the next `refresh()` (or 60s TTL
        expiry, whichever fires first)."""
        result = self.membership_at_with_meta(supabase, target_month)
        return result[1]

    def membership_at_with_meta(
        self, supabase: Client, target_month: str,
    ) -> tuple[str | None, list[dict]]:
        """Same as `membership_at` but also returns the universe's
        `last_refreshed_at` ISO string (or None) so callers can derive
        an HTTP ETag without a second DB lookup. Mostly used by the
        HTTP layer; `membership_at` is the simpler signature for
        Python-side callers."""
        cache_key = (self.template_key, target_month)
        cached = _membership_cache.get(cache_key)
        if cached is not None:
            return cached  # (last_refreshed_at, rows)

        uid = self.universe_id(supabase)
        if uid is None:
            return None, []
        last_refreshed = self.last_refreshed_at(supabase)

        out: list[dict] = []
        offset = 0
        page = 1000
        while True:
            resp = (
                supabase.table("universe_membership")
                .select(
                    "company_id, universe_ticker, sector, "
                    "company:company(company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code))"
                )
                .eq("universe_id", uid)
                .eq("target_month", target_month)
                .range(offset, offset + page - 1)
                .execute()
            )
            from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
            batch = resp.data or []
            for r in batch:
                company = r.get("company") or {}
                exchange = (
                    (company.get("gurufocus_exchange") or {}).get("exchange_code")
                ) or ""
                ticker = r.get("universe_ticker") or company.get("gurufocus_ticker") or ""
                out.append({
                    "company_id": r.get("company_id"),
                    "ticker": ticker,
                    "company_name": company.get("company_name") or "",
                    "exchange": exchange,
                    "sector": r.get("sector"),
                    "gurufocus_url": gurufocus_url(
                        company.get("gurufocus_ticker"), exchange,
                    ),
                })
            if len(batch) < page:
                break
            offset += page
        out.sort(key=lambda r: (r.get("ticker") or "").upper())

        value = (last_refreshed, out)
        _membership_cache.put(cache_key, value)
        return value

    def all_companies_ever(self, supabase: Client) -> list[dict]:
        """Every company that has appeared in this template's universe
        at any point, with first/last month, count, and whether still
        in the latest captured month. One row per unique company.
        Server-side aggregation via the `universe_all_companies_ever`
        SQL function (single round-trip; see migration
        `20260519030000_universe_all_companies_ever_fn.sql`)."""
        uid = self.universe_id(supabase)
        if uid is None:
            return []
        resp = supabase.rpc(
            "universe_all_companies_ever",
            {"p_universe_id": uid},
        ).execute()
        from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
        rows = resp.data or []
        # Decorate with the GuruFocus URL so the CSV is "complete" —
        # callers don't have to rebuild URLs from ticker+exchange.
        out: list[dict] = []
        for r in rows:
            r["gurufocus_url"] = gurufocus_url(
                r.get("gurufocus_ticker"), r.get("exchange_code"),
            )
            out.append(r)
        return out

    # ── Diff helper (used by subclass `refresh()` implementations) ──

    def compute_month_diff(
        self,
        supabase: Client,
        universe_id: int,
        prev_month: str | None,
        this_month: str,
    ) -> TemplateDiff:
        """Diff `this_month` against `prev_month` for the given
        universe. Same shape the old `_compute_acwi_diff` produced. When
        `prev_month` is None (first month ever), returns a diff whose
        `additions` is the full membership of `this_month` and removals
        are empty."""
        def _load(month: str) -> dict[int, dict]:
            if not month:
                return {}
            out: dict[int, dict] = {}
            offset = 0
            page = 1000
            while True:
                r = (
                    supabase.table("universe_membership")
                    .select("company_id, universe_ticker, sector")
                    .eq("universe_id", universe_id)
                    .eq("target_month", month)
                    .range(offset, offset + page - 1)
                    .execute()
                )
                batch = r.data or []
                for row in batch:
                    out[int(row["company_id"])] = row
                if len(batch) < page:
                    break
                offset += page
            return out

        old = _load(prev_month or "")
        new = _load(this_month)

        added = sorted(set(new) - set(old))
        removed = sorted(set(old) - set(new))
        renamed = sorted(
            cid for cid in (set(old) & set(new))
            if old[cid].get("universe_ticker") != new[cid].get("universe_ticker")
        )

        # One batch fetch for names. Chunked at IN_CHUNK_SIZE to stay
        # under PostgREST URL limits (same pattern used elsewhere).
        all_cids = list(set(added) | set(removed) | set(renamed))
        names: dict[int, str] = {}
        for chunk_start in range(0, len(all_cids), IN_CHUNK_SIZE):
            chunk = all_cids[chunk_start : chunk_start + IN_CHUNK_SIZE]
            n_resp = (
                supabase.table("company")
                .select("company_id, company_name")
                .in_("company_id", chunk)
                .execute()
            )
            for r in (n_resp.data or []):
                names[int(r["company_id"])] = r.get("company_name") or ""

        return TemplateDiff(
            template_key=self.template_key,
            universe_id=universe_id,
            this_month=this_month,
            prev_month=prev_month,
            additions_count=len(added),
            removals_count=len(removed),
            renames_count=len(renamed),
            additions=[
                {
                    "company_id": cid,
                    "ticker": new[cid].get("universe_ticker"),
                    "name": names.get(cid),
                    "sector": new[cid].get("sector"),
                }
                for cid in added
            ],
            removals=[
                {
                    "company_id": cid,
                    "ticker": old[cid].get("universe_ticker"),
                    "name": names.get(cid),
                    "sector": old[cid].get("sector"),
                }
                for cid in removed
            ],
            renames=[
                {
                    "company_id": cid,
                    "old_ticker": old[cid].get("universe_ticker"),
                    "new_ticker": new[cid].get("universe_ticker"),
                    "name": names.get(cid),
                }
                for cid in renamed
            ],
        )

"""Cumulative-LongEquity universe rebuilder.

The `longequity` universe used to be a per-month snapshot — one
membership row per (company, snapshot_month) — and a separate
`longequity_cumulative` was built manually by the user from a button
on the LongEquity Insight page. That manual step drifted out of sync
(2025 cumulative built from 35 cids; 387 cids actually ingested) and
required two universes the user had to choose between.

This module replaces both with a single `longequity` universe that's
always the cumulative union ("every company ever in any LongEquity
snapshot") replicated across every month from `EARLIEST_MONTH` to
today. The backtester sees the same set on every period, which is
what the user wants for momentum runs over the full history.

Called automatically at the end of the LongEquity ingest pipeline so
the universe never drifts from the metric_data it's derived from.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Callable

from supabase import Client

log = logging.getLogger(__name__)

# The LongEquity universe covers 2002-01 onward. Same backstop ACWI
# uses, since users typically want to backtest "LongEquity stocks"
# over the same long history as the index universes.
EARLIEST_MONTH = date(2002, 1, 1)


@dataclass
class RebuildResult:
    universe_id: int
    companies: int
    months: int
    rows_written: int
    deleted_old_cumulative: bool


def _company_ids_with_longequity_metrics(supabase: Client) -> set[int]:
    """Every company_id that has at least one `metric_data` row with
    `source_code='longequity'`. This is the source-of-truth for "ever
    appeared in a LongEquity snapshot" — the universe_membership table
    is derived from this, not the other way around."""
    out: set[int] = set()
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table('metric_data')
            .select('company_id')
            .eq('source_code', 'longequity')
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get('company_id')
            if cid is not None:
                out.add(int(cid))
        if len(batch) < page:
            break
        offset += page
    return out


def _latest_sector_per_company(supabase: Client, cids: set[int]) -> dict[int, str | None]:
    """Carry forward the most-recently-seen sector per company so the
    sector-based backtest selection still works on the cumulative
    universe. Reads from the existing `longequity` universe_membership
    rows (if any) — those carry the sector that arrived alongside each
    monthly snapshot."""
    if not cids:
        return {}
    u = (
        supabase.table('universe')
        .select('universe_id')
        .eq('label', 'LongEquity')
        .limit(1)
        .execute()
    )
    if not u.data:
        return {cid: None for cid in cids}
    uid = u.data[0]['universe_id']
    out: dict[int, str | None] = {cid: None for cid in cids}
    latest_month: dict[int, str] = {}
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table('universe_membership')
            .select('company_id, target_month, sector')
            .eq('universe_id', uid)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            cid = r.get('company_id')
            if cid not in out:
                continue
            sec = r.get('sector')
            m = r.get('target_month') or ''
            if sec and (cid not in latest_month or m > latest_month[cid]):
                latest_month[cid] = m
                out[cid] = sec
        if len(batch) < page:
            break
        offset += page
    return out


def _months_from(start: date, end: date) -> list[str]:
    """`YYYY-MM` strings for every month in [start, end], inclusive."""
    out: list[str] = []
    cur = date(start.year, start.month, 1)
    end_m = date(end.year, end.month, 1)
    while cur <= end_m:
        out.append(cur.strftime('%Y-%m'))
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else date(cur.year, cur.month + 1, 1)
    return out


def _delete_universe_memberships(supabase: Client, universe_id: int) -> None:
    """Wipe every membership row for a universe. Loops to outlast any
    PostgREST per-request row cap so we never leave stragglers behind."""
    for _ in range(20):
        supabase.table('universe_membership').delete().eq('universe_id', universe_id).execute()
        # Existence check, not count -- `SELECT 1 LIMIT 1` short-circuits as
        # soon as one row is found, whereas `count="exact"` runs a full
        # COUNT(*) over the matched set even with head=True.
        check = (
            supabase.table('universe_membership')
            .select('company_id')
            .eq('universe_id', universe_id)
            .limit(1)
            .execute()
        )
        if not check.data:
            return


def _drop_legacy_cumulative(supabase: Client) -> bool:
    """Drop the standalone `longequity_cumulative` universe + its
    memberships if it still exists. Idempotent — does nothing when
    already absent."""
    u = (
        supabase.table('universe')
        .select('universe_id')
        .eq('label', 'longequity_cumulative')
        .limit(1)
        .execute()
    )
    if not u.data:
        return False
    uid = u.data[0]['universe_id']
    _delete_universe_memberships(supabase, uid)
    try:
        supabase.table('universe').delete().eq('universe_id', uid).execute()
    except Exception as e:
        log.warning("[longequity_universe] couldn't delete legacy cumulative: %s", e)
        return False
    log.info('[longequity_universe] dropped legacy `longequity_cumulative` universe (id=%s)', uid)
    return True


def rebuild_cumulative_longequity_universe(
    supabase: Client,
    *,
    earliest: date = EARLIEST_MONTH,
    end: date | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> RebuildResult:
    """Wipe + rewrite the `longequity` universe with cumulative semantics.

    Source: every `company_id` that has any `metric_data` with
    `source_code='longequity'`. The set is replicated as one membership
    row per (company, target_month) for every month in
    [earliest, end].

    Also drops the legacy `longequity_cumulative` universe so the
    table converges on a single LongEquity universe.

    Idempotent — safe to call on every ingest."""
    def emit(msg: str) -> None:
        log.info('[longequity_universe] %s', msg)
        if on_progress is not None:
            try:
                on_progress(msg)
            except Exception:
                pass

    end_date = end or date.today().replace(day=1)

    # Ensure the canonical row exists.
    u_resp = (
        supabase.table('universe')
        .select('universe_id')
        .eq('label', 'LongEquity')
        .limit(1)
        .execute()
    )
    if u_resp.data:
        universe_id = u_resp.data[0]['universe_id']
    else:
        ins = supabase.table('universe').insert({
            'label': 'LongEquity',
            'description': (
                'Cumulative universe: every company that has ever appeared in '
                'any LongEquity snapshot. Replicated across every month from '
                f"{earliest.strftime('%Y-%m')} onward so the momentum backtester "
                'sees it on every period.'
            ),
        }).execute()
        universe_id = ins.data[0]['universe_id']

    cids = _company_ids_with_longequity_metrics(supabase)
    emit(f'Found {len(cids)} distinct companies in longequity metric_data')
    if not cids:
        # Nothing to write. Still wipe + drop legacy so we leave a
        # consistent empty universe behind.
        _delete_universe_memberships(supabase, universe_id)
        legacy = _drop_legacy_cumulative(supabase)
        return RebuildResult(
            universe_id=universe_id, companies=0, months=0,
            rows_written=0, deleted_old_cumulative=legacy,
        )

    sectors = _latest_sector_per_company(supabase, cids)
    months = _months_from(earliest, end_date)
    emit(f'Replicating across {len(months)} months ({months[0]} → {months[-1]})')

    # Wipe existing rows BEFORE the legacy drop — if the legacy delete
    # somehow grabs the wrong row, we still won't have orphan stale
    # per-month memberships hanging around.
    _delete_universe_memberships(supabase, universe_id)

    payload: list[dict] = []
    for cid in cids:
        sec = sectors.get(cid)
        for m in months:
            payload.append({
                'universe_id': universe_id,
                'company_id': cid,
                'target_month': m,
                'sector': sec,
            })

    written = 0
    batch_size = 500
    for i in range(0, len(payload), batch_size):
        chunk = payload[i:i + batch_size]
        try:
            resp = (
                supabase.table('universe_membership')
                .insert(chunk)
                .execute()
            )
            written += len(resp.data or [])
        except Exception as e:
            log.warning(
                '[longequity_universe] insert batch %s failed: %s. Retrying with upsert.',
                i // batch_size, e,
            )
            try:
                supabase.table('universe_membership').upsert(
                    chunk, on_conflict='universe_id,company_id,target_month',
                ).execute()
                written += len(chunk)
            except Exception as e2:
                log.warning('[longequity_universe] upsert fallback also failed: %s', e2)

    legacy = _drop_legacy_cumulative(supabase)
    emit(f'Wrote {written} membership rows; legacy cumulative dropped={legacy}')

    return RebuildResult(
        universe_id=universe_id, companies=len(cids), months=len(months),
        rows_written=written, deleted_old_cumulative=legacy,
    )


if __name__ == '__main__':
    from deps import supabase  # noqa: PLC0415
    res = rebuild_cumulative_longequity_universe(supabase)
    print(
        f'longequity universe_id={res.universe_id}: '
        f'{res.companies} companies x {res.months} months = '
        f'{res.rows_written} rows. '
        f'legacy cumulative dropped: {res.deleted_old_cumulative}'
    )

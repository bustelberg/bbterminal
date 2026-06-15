"""LongEquity universe builder — true per-month membership + frozen union.

Two universes are produced from the LongEquity report data (`metric_data`,
`source_code='longequity'`):

1. **`LongEquity`** (the canonical time-series universe, `is_monthly=true`) —
   `rebuild_longequity_universe` writes TRUE point-in-time membership: each
   month holds the companies that appear in that month's report. This is the
   per-month view (for the LongEquity page / earnings analysis). It replaced
   the old cumulative model (every-ever company broadcast across every month),
   which only existed so the momentum backtester saw the set on every period.

2. **`LongEquity (frozen as of …)`** (a frozen snapshot, `is_monthly=false`) —
   `freeze_longequity_union` snapshots the UNION of every company across every
   report month into a single dated set. This is the universe the user selects
   for backtests/earnings (frozen-only surfaces), created on demand from the
   LongEquity page. Static — the pipeline never refreshes it.

`rebuild_longequity_universe` runs automatically at the end of the LongEquity
ingest so the per-month universe never drifts from the metric_data it derives
from; the frozen union is user-triggered.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable

from supabase import Client

from deps import chunked, paginate

log = logging.getLogger(__name__)

# Retained for call-site compatibility (an old default range backstop); the
# per-month rebuild now takes its months from the report data, not a range.
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
    for r in paginate(
        lambda lo, hi: supabase.table('metric_data')
        .select('company_id')
        .eq('source_code', 'longequity')
        .range(lo, hi)
        .execute()
    ):
        cid = r.get('company_id')
        if cid is not None:
            out.add(int(cid))
    return out


def _membership_by_month(supabase: Client) -> dict[str, set[int]]:
    """True point-in-time membership: `{YYYY-MM: {company_id, ...}}` from the
    actual monthly LongEquity reports (`metric_data` source `longequity`),
    one entry per report month. Backed by the `longequity_membership_by_month`
    RPC, which returns ~one row per month (company_ids as an int[]) so the
    whole panel fits in a single round-trip under the PostgREST row cap."""
    resp = supabase.rpc('longequity_membership_by_month').execute()
    out: dict[str, set[int]] = {}
    for r in (resp.data or []):
        month = (r.get('target_month') or '')[:7]
        if not month:
            continue
        cids = r.get('company_ids') or []
        out[month] = {int(c) for c in cids if c is not None}
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
    for r in paginate(
        lambda lo, hi: supabase.table('universe_membership')
        .select('company_id, target_month, sector')
        .eq('universe_id', uid)
        .range(lo, hi)
        .execute()
    ):
        cid = r.get('company_id')
        if cid not in out:
            continue
        sec = r.get('sector')
        m = r.get('target_month') or ''
        if sec and (cid not in latest_month or m > latest_month[cid]):
            latest_month[cid] = m
            out[cid] = sec
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


def rebuild_longequity_universe(
    supabase: Client,
    *,
    earliest: date = EARLIEST_MONTH,
    end: date | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> RebuildResult:
    """Wipe + rewrite the `LongEquity` universe with TRUE per-month membership.

    LongEquity is the one time-series universe (`is_monthly=true`): each month
    holds the companies that actually appear in THAT month's report, sourced
    from `metric_data` rows with `source_code='longequity'` (one report date
    per month, ~2025-08 onward). This is real point-in-time membership — it
    replaces the old cumulative model (all-ever companies broadcast across
    every month 2002→today), which existed only so a backtest saw the set on
    every period. Backtests now select a FROZEN universe instead (see
    `freeze_longequity_union`), so the broadcast is no longer needed and the
    per-month view can be honest.

    `earliest` / `end` are retained for call-site compatibility but ignored —
    the months come from the report data itself, not a synthetic range.

    Also drops the legacy `longequity_cumulative` universe so the table
    converges on a single LongEquity universe. Idempotent."""
    def emit(msg: str) -> None:
        log.info('[longequity_universe] %s', msg)
        if on_progress is not None:
            try:
                on_progress(msg)
            except Exception:
                pass

    # Ensure the canonical row exists. LongEquity is the ONE time-series
    # universe (is_monthly=true) — it keeps per-month membership while every
    # other universe is a single frozen snapshot. The DB enforces that
    # invariant (universe_membership_frozen_single_month trigger), so this
    # multi-month writer MUST target an is_monthly=true row or the write is
    # rejected. We set the flag explicitly here so a freshly-created row (or
    # one left non-monthly by older code) is always trigger-safe.
    u_resp = (
        supabase.table('universe')
        .select('universe_id, is_monthly')
        .eq('label', 'LongEquity')
        .limit(1)
        .execute()
    )
    if u_resp.data:
        universe_id = u_resp.data[0]['universe_id']
        if not u_resp.data[0].get('is_monthly'):
            supabase.table('universe').update(
                {'is_monthly': True}
            ).eq('universe_id', universe_id).execute()
    else:
        ins = supabase.table('universe').insert({
            'label': 'LongEquity',
            'template_key': 'LONGEQUITY',
            'is_monthly': True,
            'description': (
                'Cumulative universe: every company that has ever appeared in '
                'any LongEquity snapshot. Replicated across every month from '
                f"{earliest.strftime('%Y-%m')} onward so the momentum backtester "
                'sees it on every period.'
            ),
        }).execute()
        universe_id = ins.data[0]['universe_id']

    by_month = _membership_by_month(supabase)
    all_cids: set[int] = set()
    for member_set in by_month.values():
        all_cids |= member_set
    emit(f'LongEquity reports: {len(by_month)} month(s), {len(all_cids)} distinct companies')
    if not by_month:
        # No report data. Still wipe + drop legacy so we leave a
        # consistent empty universe behind.
        _delete_universe_memberships(supabase, universe_id)
        legacy = _drop_legacy_cumulative(supabase)
        return RebuildResult(
            universe_id=universe_id, companies=0, months=0,
            rows_written=0, deleted_old_cumulative=legacy,
        )

    # Sector is carried forward per company (the classification is stable
    # month-to-month; the report doesn't restate it every period).
    sectors = _latest_sector_per_company(supabase, all_cids)
    months = sorted(by_month.keys())
    emit(f'Writing true per-month membership ({months[0]} → {months[-1]})')

    # Wipe existing rows BEFORE the legacy drop — if the legacy delete
    # somehow grabs the wrong row, we still won't have orphan stale
    # per-month memberships hanging around.
    _delete_universe_memberships(supabase, universe_id)

    payload: list[dict] = []
    for m in months:
        for cid in sorted(by_month[m]):
            payload.append({
                'universe_id': universe_id,
                'company_id': cid,
                'target_month': m,
                'sector': sectors.get(cid),
            })

    written = 0
    for ci, chunk in enumerate(chunked(payload, 500)):
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
                ci, e,
            )
            try:
                supabase.table('universe_membership').upsert(
                    chunk, on_conflict='universe_id,company_id,target_month',
                ).execute()
                written += len(chunk)
            except Exception as e2:
                log.warning('[longequity_universe] upsert fallback also failed: %s', e2)

    # Stamp as_of_date = latest captured report month (the column's meaning
    # for is_monthly universes) so it tracks the newest report, not a stale
    # value from an earlier migration.
    supabase.table('universe').update(
        {'as_of_date': f'{months[-1]}-01'}
    ).eq('universe_id', universe_id).execute()

    legacy = _drop_legacy_cumulative(supabase)
    emit(f'Wrote {written} membership rows; legacy cumulative dropped={legacy}')

    return RebuildResult(
        universe_id=universe_id, companies=len(all_cids), months=len(months),
        rows_written=written, deleted_old_cumulative=legacy,
    )


# Back-compat alias. The universe is now true per-month (not "cumulative"),
# but external callers still import the old name — keep it working.
rebuild_cumulative_longequity_universe = rebuild_longequity_universe


def _membership_count(supabase: Client, universe_id: int) -> int:
    """Count membership rows for a universe (paginated; union is ~hundreds)."""
    n = 0
    for _ in paginate(
        lambda lo, hi: supabase.table('universe_membership')
        .select('company_id')
        .eq('universe_id', universe_id)
        .range(lo, hi)
        .execute()
    ):
        n += 1
    return n


def freeze_longequity_union(
    supabase: Client,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> dict:
    """Snapshot the UNION of every company across every LongEquity report
    month into a single dated FROZEN universe (`is_monthly=false`).

    This is the LongEquity universe the user selects on frozen-only surfaces
    (backtest / earnings): one set, every company that has ever appeared in a
    monthly report, as of today. Idempotent per calendar day — re-freezing the
    same day returns the existing snapshot; a later day creates a new dated
    one. Static: the pipeline never refreshes it.

    A single `target_month` (today's) makes this trigger-safe under the
    frozen-single-month invariant; the backtest loader treats it as a constant
    basket via `broadcast_constant`."""
    def emit(msg: str) -> None:
        log.info('[longequity_universe] %s', msg)
        if on_progress is not None:
            try:
                on_progress(msg)
            except Exception:
                pass

    today = date.today()
    target_month = today.strftime('%Y-%m')
    as_of_iso = today.isoformat()
    label = f'LongEquity (as of {as_of_iso})'

    # Idempotent: if we already froze today, return that snapshot.
    existing = (
        supabase.table('universe')
        .select('universe_id, label, as_of_date')
        .eq('label', label)
        .limit(1)
        .execute()
    )
    if existing.data:
        uid = existing.data[0]['universe_id']
        n = _membership_count(supabase, uid)
        emit(f'Already frozen today: "{label}" ({n} companies)')
        return {
            'created': False, 'universe_id': uid, 'label': label,
            'companies': n, 'as_of_date': existing.data[0].get('as_of_date'),
        }

    by_month = _membership_by_month(supabase)
    union: set[int] = set()
    for member_set in by_month.values():
        union |= member_set
    if not union:
        raise ValueError('No LongEquity report data to freeze.')
    emit(f'Freezing union of {len(union)} companies across {len(by_month)} report month(s)…')

    sectors = _latest_sector_per_company(supabase, union)

    ins = supabase.table('universe').insert({
        'label': label,
        'template_key': None,
        'is_monthly': False,
        'as_of_date': as_of_iso,
        'frozen_at': datetime.now(timezone.utc).isoformat(),
        'frozen_from': 'LongEquity',
        'description': (
            f'Frozen union of every company that has appeared in any LongEquity '
            f'monthly report, as of {as_of_iso} ({len(union)} companies across '
            f'{len(by_month)} report months). Static — the pipeline never refreshes it.'
        ),
    }).execute()
    universe_id = ins.data[0]['universe_id']

    payload = [
        {
            'universe_id': universe_id,
            'company_id': cid,
            'target_month': target_month,
            'sector': sectors.get(cid),
        }
        for cid in sorted(union)
    ]
    written = 0
    for ci, chunk in enumerate(chunked(payload, 500)):
        try:
            resp = supabase.table('universe_membership').insert(chunk).execute()
            written += len(resp.data or [])
        except Exception as e:
            log.warning('[longequity_universe] freeze insert batch %s failed: %s. Retrying with upsert.', ci, e)
            supabase.table('universe_membership').upsert(
                chunk, on_conflict='universe_id,company_id,target_month',
            ).execute()
            written += len(chunk)

    emit(f'Froze "{label}": {written} companies.')
    return {
        'created': True, 'universe_id': universe_id, 'label': label,
        'companies': written, 'as_of_date': as_of_iso,
    }


if __name__ == '__main__':
    from deps import supabase  # noqa: PLC0415
    res = rebuild_longequity_universe(supabase)
    print(
        f'longequity universe_id={res.universe_id}: '
        f'{res.companies} companies x {res.months} months = '
        f'{res.rows_written} rows. '
        f'legacy cumulative dropped: {res.deleted_old_cumulative}'
    )

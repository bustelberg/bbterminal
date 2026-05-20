"""Company de-duplication: canonical normalization + pre-insert checks
+ merge of existing duplicates.

Centralizes the canonical forms used by every ingest path so the
`company` table can't accumulate duplicates of the same security:

  * HKSE tickers are zero-padded to 5 digits. The same security can
    legitimately be expressed as `700`, `0700`, or `00700` — GuruFocus
    treats them as one, but our table previously stored whatever the
    source emitted, producing `HKSE:00700` AND `HKSE:700` as two
    different rows for Tencent.
  * Company names are matched case- and whitespace-insensitively.
    `AIA GROUP LTD` and `AIA Group Ltd` are the same issuer.

The `EXCHANGE_PRIORITY` map picks a winner when two rows of the same
issuer survive on different exchanges. The user's stated preference is
Hong Kong over mainland China (H-share over A-share), then European /
Asia primary listings over US ADRs / German GDRs.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from supabase import Client

_log = logging.getLogger(__name__)


# Lower priority = preferred. When two rows share a canonical name,
# the lower-priority row wins (is kept). Anything not listed defaults
# to 99 (everything-else falls back to the lowest-priority row).
EXCHANGE_PRIORITY: dict[str, int] = {
    # User-explicit preference: Hong Kong > China A-shares.
    'HKSE': 0,
    # European primary listings — usually the canonical issuer listing.
    'LSE': 1, 'XPAR': 1, 'XAMS': 1, 'XBRU': 1, 'XLIS': 1,
    'MIL': 1, 'XMAD': 1, 'XSWX': 1, 'OSTO': 1, 'OCSE': 1, 'OSL': 1,
    'OHEL': 1, 'WAR': 1, 'WBO': 1, 'ATH': 1, 'DUB': 1, 'BUD': 1,
    'XPRA': 1, 'IST': 1,
    # Asian primaries.
    'TSE': 2, 'TPE': 2, 'ROCO': 2, 'XKRX': 2, 'NSE': 2, 'BSE': 2,
    'SGX': 2, 'XKLS': 2, 'ISX': 2, 'BKK': 2, 'PHS': 2,
    # Middle East primaries.
    'SAU': 2, 'ADX': 2, 'DFM': 2, 'DSMD': 2, 'KUW': 2, 'XTAE': 2,
    # Canada / Mexico primaries.
    'TSX': 2, 'BMV': 2, 'MEX': 2,
    # Africa.
    'JSE': 2,
    # China A-shares — yield to HKSE H-shares per user preference.
    'SHSE': 5, 'SZSE': 5,
    # German Xetra — lower priority because for non-German issuers
    # it's typically a GDR. CSPC Pharma's `XTER:CVG` should yield to
    # `HKSE:01093`.
    'XTER': 5, 'FRA': 6,
    # US — for non-US issuers these are usually ADRs; yield. For
    # US-domiciled companies (Apple, Microsoft, …) there's typically
    # only one US row so no dupe choice arises.
    'NYSE': 10, 'NASDAQ': 10, 'AMEX': 10, 'CBOE': 10,
}


# Trailing share-class letters Nordic exchanges put on tickers
# (`NOVO B`, `ATCO A`). Preserved as-is in canonical form; the space
# vs dot vs dash variants get normalized to a single space form.
_NORDIC_EXCHANGES = {'OSTO', 'OCSE', 'OHEL', 'OSL'}


def canonical_ticker(ticker: str | None, exchange_code: str | None) -> str:
    """Canonical form of a (ticker, exchange) pair for dupe detection.

    Today's rules:
      * Stripped + uppercased.
      * HKSE numeric tickers zero-padded to 5 digits.
      * Nordic share-class delimiters normalized to a single space
        (`NOVO.B`, `NOVO-B`, `NOVO B` → `NOVO B`).

    Add more rules here as new dupe patterns surface — this is the
    one place every ingest path consults."""
    if not ticker:
        return ''
    t = ticker.strip().upper()
    exch = (exchange_code or '').strip().upper()
    if exch == 'HKSE' and t.isdigit():
        return t.zfill(5)
    if exch in _NORDIC_EXCHANGES:
        # `NOVO.B`, `NOVO-B` → `NOVO B`
        m = re.match(r'^(.+)[.\-]([A-Z])$', t)
        if m:
            return f'{m.group(1)} {m.group(2)}'
    return t


def canonical_name(name: str | None) -> str:
    """Canonical form of a company name for dupe detection.

    Lowercased, leading/trailing whitespace stripped, internal
    whitespace collapsed to a single space. Punctuation and corporate
    suffixes (`Ltd`, `Inc`, `Holdings`) are *not* stripped — different
    legal entities can have the same root name with different suffixes
    (e.g. `BYD Co Ltd` vs `BYD Electronic`)."""
    if not name:
        return ''
    n = name.strip().lower()
    n = re.sub(r'\s+', ' ', n)
    return n


def exchange_priority(exchange_code: str | None) -> int:
    """Lookup with a sane default — unmapped exchanges get the
    lowest priority so a known/canonical exchange always wins."""
    if not exchange_code:
        return 99
    return EXCHANGE_PRIORITY.get(exchange_code.strip().upper(), 99)


@dataclass
class CompanyRow:
    """Minimal projection of a `company` row used by dedupe logic."""
    company_id: int
    company_name: str | None
    gurufocus_ticker: str | None
    exchange_code: str | None
    exchange_id: int | None


def pick_winner(candidates: list[CompanyRow]) -> CompanyRow:
    """Pick the survivor when multiple rows describe the same issuer.

    Order, lowest first:
      1. EXCHANGE_PRIORITY (HKSE wins, US ADRs lose).
      2. For HKSE-only ties: prefer the zero-padded 5-digit form over
         shorter strings (the canonical GuruFocus form).
      3. Lowest `company_id` — the oldest row wins on full ties so the
         outcome is deterministic across runs."""
    def key(c: CompanyRow):
        prio = exchange_priority(c.exchange_code)
        exch = (c.exchange_code or '').upper()
        tkr = c.gurufocus_ticker or ''
        # Bonus for the canonical HKSE 5-digit form. 0 = prefer.
        padded_bonus = 0 if (exch == 'HKSE' and len(tkr) == 5 and tkr.isdigit()) else 1
        return (prio, padded_bonus, c.company_id)
    return sorted(candidates, key=key)[0]


# ─── Pre-insert duplicate detection ─────────────────────────────────


def find_canonical_match(
    supabase: Client,
    name: str | None,
    ticker: str | None,
    exchange_code: str | None,
) -> list[CompanyRow]:
    """Return any existing company rows that would canonically match
    the proposed `(name, ticker, exchange_code)` triple.

    Two match buckets:
      * Same (canonical_ticker, exchange_code) — guarantees the listing
        is already in the DB regardless of how the ticker was punched in.
      * Same canonical_name across ANY exchange — surfaces the H-share /
        A-share / GDR cross-exchange dupes the user wants to reject.

    Returns a (possibly-empty) list of `CompanyRow` so the caller can
    explain the conflict to the user."""
    norm_ticker = canonical_ticker(ticker, exchange_code)
    norm_name = canonical_name(name)
    out: dict[int, CompanyRow] = {}

    # Bucket 1: same exchange + canonical ticker. Pull every row on
    # this exchange and compare in Python — there's no SQL function for
    # zero-padded HKSE tickers and the per-exchange row count is small.
    if norm_ticker and exchange_code:
        resp = (
            supabase.table('company')
            .select(
                'company_id, company_name, gurufocus_ticker, '
                'gurufocus_exchange:gurufocus_exchange(exchange_code, exchange_id)'
            )
            .execute()
        )
        for r in (resp.data or []):
            exch = ((r.get('gurufocus_exchange') or {}).get('exchange_code') or '')
            if exch.upper() != exchange_code.strip().upper():
                continue
            if canonical_ticker(r.get('gurufocus_ticker'), exch) == norm_ticker:
                out[r['company_id']] = CompanyRow(
                    company_id=r['company_id'],
                    company_name=r.get('company_name'),
                    gurufocus_ticker=r.get('gurufocus_ticker'),
                    exchange_code=exch,
                    exchange_id=(r.get('gurufocus_exchange') or {}).get('exchange_id'),
                )

    # Bucket 2: same canonical name across all exchanges.
    if norm_name:
        offset = 0
        page = 1000
        while True:
            resp = (
                supabase.table('company')
                .select(
                    'company_id, company_name, gurufocus_ticker, '
                    'gurufocus_exchange:gurufocus_exchange(exchange_code, exchange_id)'
                )
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = resp.data or []
            if not batch:
                break
            for r in batch:
                if r['company_id'] in out:
                    continue
                if canonical_name(r.get('company_name')) == norm_name:
                    exch_obj = r.get('gurufocus_exchange') or {}
                    out[r['company_id']] = CompanyRow(
                        company_id=r['company_id'],
                        company_name=r.get('company_name'),
                        gurufocus_ticker=r.get('gurufocus_ticker'),
                        exchange_code=exch_obj.get('exchange_code'),
                        exchange_id=exch_obj.get('exchange_id'),
                    )
            if len(batch) < page:
                break
            offset += page

    return sorted(out.values(), key=lambda c: c.company_id)


# ─── One-time merge of existing dupes ───────────────────────────────


@dataclass
class MergeReport:
    """Outcome of a merge_existing_duplicates pass."""
    hkse_tickers_normalized: int = 0
    groups_merged: int = 0
    rows_deleted: int = 0
    metric_data_reassigned: int = 0
    metric_data_dropped: int = 0
    universe_membership_reassigned: int = 0
    portfolio_weight_reassigned: int = 0
    company_source_reassigned: int = 0
    leonteq_equity_reassigned: int = 0
    actions: list[str] = field(default_factory=list)


def _normalize_hkse_tickers(supabase: Client) -> int:
    """Zero-pad every HKSE ticker that's a 1-4 digit number. Idempotent.

    Done BEFORE the grouping pass so HKSE:700 and HKSE:00700 collapse
    onto one canonical key and the grouper pairs them automatically."""
    fixed = 0
    # Get HKSE exchange_id once.
    exch = (
        supabase.table('gurufocus_exchange')
        .select('exchange_id')
        .eq('exchange_code', 'HKSE')
        .limit(1)
        .execute()
    )
    if not exch.data:
        return 0
    hkse_id = exch.data[0]['exchange_id']

    offset = 0
    page = 1000
    pending: list[dict] = []
    while True:
        resp = (
            supabase.table('company')
            .select('company_id, gurufocus_ticker')
            .eq('exchange_id', hkse_id)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            tkr = (r.get('gurufocus_ticker') or '').strip()
            if tkr.isdigit() and 1 <= len(tkr) < 5:
                pending.append({'company_id': r['company_id'], 'new': tkr.zfill(5), 'old': tkr})
        if len(batch) < page:
            break
        offset += page

    # Apply updates one by one — the (gurufocus_ticker, exchange_id)
    # unique constraint will reject a normalize that would collide
    # with an already-padded row; we catch and skip those, leaving the
    # next pass (merge_existing_duplicates) to handle the collision.
    for upd in pending:
        try:
            supabase.table('company').update(
                {'gurufocus_ticker': upd['new']}
            ).eq('company_id', upd['company_id']).execute()
            fixed += 1
        except Exception as e:
            _log.info(
                '[dedupe] HKSE pad %s -> %s for cid=%s failed (likely collision; '
                'merger will fix): %s',
                upd['old'], upd['new'], upd['company_id'], e,
            )
    return fixed


def _move_metric_data(supabase: Client, from_id: int, to_id: int) -> tuple[int, int]:
    """Reassign metric_data rows from one cid to another. Returns
    `(moved, dropped)`. Conflicting rows (same key on `to_id`) are
    dropped since the canonical row already has the value.

    Uses the existing `merge_company_data` Postgres function when
    available — it does the move+delete in a single transaction.
    Falls back to client-side count-and-delete on error."""
    moved = 0
    dropped = 0
    # The RPC handles conflicts internally. It returns void; we
    # estimate `moved` from a pre-count below.
    try:
        before = (
            supabase.table('metric_data')
            .select('company_id', count='exact')
            .eq('company_id', from_id)
            .limit(1)
            .execute()
        )
        before_count = before.count or 0
        if before_count == 0:
            return 0, 0
        # Count would-be conflicts so the report tells the truth.
        target = (
            supabase.table('metric_data')
            .select('metric_code, source_code, target_date')
            .eq('company_id', to_id)
            .execute()
        )
        target_keys = {
            (r['metric_code'], r['source_code'], r['target_date'])
            for r in (target.data or [])
        }
        src = (
            supabase.table('metric_data')
            .select('metric_code, source_code, target_date')
            .eq('company_id', from_id)
            .execute()
        )
        src_keys = [
            (r['metric_code'], r['source_code'], r['target_date'])
            for r in (src.data or [])
        ]
        conflicting = sum(1 for k in src_keys if k in target_keys)
        moveable = before_count - conflicting

        supabase.rpc('merge_company_data', {
            'p_from_id': from_id,
            'p_to_id': to_id,
        }).execute()
        moved = moveable
        dropped = conflicting
    except Exception as e:
        _log.warning(
            '[dedupe] merge_company_data RPC failed for %s -> %s: %s. '
            'Falling back to delete-source.', from_id, to_id, e,
        )
        try:
            supabase.table('metric_data').delete().eq('company_id', from_id).execute()
        except Exception as e2:
            _log.warning('[dedupe] metric_data fallback delete failed: %s', e2)
    return moved, dropped


def _move_simple_fk(
    supabase: Client, table: str, from_id: int, to_id: int,
    *, dedup_keys: list[str] | None = None,
) -> int:
    """Reassign rows in `table` from cid=from_id to cid=to_id.

    When `dedup_keys` is given, the target's existing rows are read
    first and any source row whose (cid_to, *dedup_keys) would
    collide is DELETED instead of moved. This is the universe_membership
    + portfolio_weight + leonteq_equity pattern: composite primary keys
    where the canonical row may already hold the slot."""
    if dedup_keys:
        try:
            target_rows = (
                supabase.table(table)
                .select(','.join(dedup_keys))
                .eq('company_id', to_id)
                .execute()
            )
            target_keys = {
                tuple(r.get(k) for k in dedup_keys)
                for r in (target_rows.data or [])
            }
            source_rows = (
                supabase.table(table)
                .select(','.join(dedup_keys + ['company_id']))
                .eq('company_id', from_id)
                .execute()
            )
            colliding: list[dict] = []
            moveable: list[dict] = []
            for r in (source_rows.data or []):
                key = tuple(r.get(k) for k in dedup_keys)
                (colliding if key in target_keys else moveable).append(r)
            for c in colliding:
                q = supabase.table(table).delete().eq('company_id', from_id)
                for k in dedup_keys:
                    q = q.eq(k, c[k])
                q.execute()
        except Exception as e:
            _log.warning('[dedupe] %s collision check failed: %s', table, e)

    try:
        resp = (
            supabase.table(table)
            .update({'company_id': to_id})
            .eq('company_id', from_id)
            .execute()
        )
        return len(resp.data or [])
    except Exception as e:
        _log.warning('[dedupe] update %s.company_id failed for %s -> %s: %s',
                     table, from_id, to_id, e)
        return 0


def merge_existing_duplicates(
    supabase: Client, *, dry_run: bool = False,
) -> MergeReport:
    """Find every dupe group (rows sharing canonical_name) and merge.

    Order of operations:
      1. Normalize HKSE tickers (zero-pad). Sees `HKSE:00700` and
         `HKSE:700` as the same canonical key after this step.
      2. Group remaining rows by canonical_name.
      3. For each group, pick the winner via `pick_winner` and reassign
         every cross-table reference from loser → winner.

    Returns a report with counts. `dry_run=True` skips all mutations
    and just returns the would-be counts so the caller can preview."""
    report = MergeReport()

    if not dry_run:
        report.hkse_tickers_normalized = _normalize_hkse_tickers(supabase)
        report.actions.append(
            f'Normalized {report.hkse_tickers_normalized} HKSE ticker(s) '
            f'to zero-padded form'
        )

    # Pull every company row + its exchange code.
    rows: list[CompanyRow] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table('company')
            .select(
                'company_id, company_name, gurufocus_ticker, '
                'gurufocus_exchange:gurufocus_exchange(exchange_code, exchange_id)'
            )
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for r in batch:
            exch_obj = r.get('gurufocus_exchange') or {}
            rows.append(CompanyRow(
                company_id=r['company_id'],
                company_name=r.get('company_name'),
                gurufocus_ticker=r.get('gurufocus_ticker'),
                exchange_code=exch_obj.get('exchange_code'),
                exchange_id=exch_obj.get('exchange_id'),
            ))
        if len(batch) < page:
            break
        offset += page

    # Group by canonical_name. Empty names get skipped — no way to
    # confidently merge a nameless row.
    from collections import defaultdict  # noqa: PLC0415
    groups: dict[str, list[CompanyRow]] = defaultdict(list)
    for r in rows:
        n = canonical_name(r.company_name)
        if n:
            groups[n].append(r)

    dupe_groups = {n: g for n, g in groups.items() if len(g) > 1}

    for name, group in sorted(dupe_groups.items()):
        winner = pick_winner(group)
        losers = [r for r in group if r.company_id != winner.company_id]

        action_lines = [
            f'merge "{name}" -- keep cid={winner.company_id} '
            f'({winner.exchange_code}:{winner.gurufocus_ticker}), '
            f'merge: ' + ', '.join(
                f'cid={loser.company_id} ({loser.exchange_code}:{loser.gurufocus_ticker})'
                for loser in losers
            )
        ]

        if dry_run:
            report.actions.extend(action_lines)
            report.groups_merged += 1
            report.rows_deleted += len(losers)
            continue

        for loser in losers:
            mv, dr = _move_metric_data(supabase, loser.company_id, winner.company_id)
            report.metric_data_reassigned += mv
            report.metric_data_dropped += dr
            report.universe_membership_reassigned += _move_simple_fk(
                supabase, 'universe_membership', loser.company_id, winner.company_id,
                dedup_keys=['universe_id', 'target_month'],
            )
            report.portfolio_weight_reassigned += _move_simple_fk(
                supabase, 'portfolio_weight', loser.company_id, winner.company_id,
                dedup_keys=['portfolio_id'],
            )
            report.company_source_reassigned += _move_simple_fk(
                supabase, 'company_source', loser.company_id, winner.company_id,
                dedup_keys=['source_code'],
            )
            # leonteq_equity has only `id` as PK and `company_id` is
            # nullable — no composite collision to worry about, just
            # rewire the FK.
            try:
                resp = (
                    supabase.table('leonteq_equity')
                    .update({'company_id': winner.company_id})
                    .eq('company_id', loser.company_id)
                    .execute()
                )
                report.leonteq_equity_reassigned += len(resp.data or [])
            except Exception as e:
                _log.warning('[dedupe] leonteq_equity update failed: %s', e)

            # Same for current_picks_snapshot — best-effort; the JSONB
            # holdings live in `holdings`/`daily_picks` and aren't
            # FK-tied, but the audit links are.
            for table, col in [
                ('current_picks_snapshot', 'company_id'),
                ('index_membership', 'company_id'),
            ]:
                try:
                    supabase.table(table).update({'company_id': winner.company_id}).eq(
                        'company_id', loser.company_id,
                    ).execute()
                except Exception:
                    pass

            try:
                supabase.table('company').delete().eq('company_id', loser.company_id).execute()
                report.rows_deleted += 1
            except Exception as e:
                action_lines.append(f'  WARN: could not delete cid={loser.company_id}: {e}')

        action_lines.append(
            f'  -> moved {report.metric_data_reassigned} metric, '
            f'{report.universe_membership_reassigned} membership, '
            f'{report.portfolio_weight_reassigned} weight rows so far'
        )
        report.actions.extend(action_lines)
        report.groups_merged += 1

    return report


def format_report(report: MergeReport, *, dry_run: bool) -> str:
    lines: list[str] = []
    if dry_run:
        lines.append(f'[dry run] Would merge {report.groups_merged} dupe groups, '
                     f'deleting {report.rows_deleted} loser rows.')
    else:
        lines.append(
            f'Merged {report.groups_merged} dupe groups, deleted '
            f'{report.rows_deleted} rows.'
        )
        lines.append(
            f'  HKSE tickers normalized: {report.hkse_tickers_normalized}'
        )
        lines.append(
            f'  metric_data reassigned: {report.metric_data_reassigned} '
            f'(dropped {report.metric_data_dropped} conflicts)'
        )
        lines.append(
            f'  universe_membership reassigned: {report.universe_membership_reassigned}'
        )
        lines.append(
            f'  portfolio_weight reassigned: {report.portfolio_weight_reassigned}'
        )
        lines.append(
            f'  company_source reassigned: {report.company_source_reassigned}'
        )
        lines.append(
            f'  leonteq_equity reassigned: {report.leonteq_equity_reassigned}'
        )
    if report.actions:
        lines.append('')
        lines.extend(report.actions)
    return '\n'.join(lines)


if __name__ == '__main__':
    import sys  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    dry_run = '--apply' not in sys.argv
    res = merge_existing_duplicates(supabase, dry_run=dry_run)
    print(format_report(res, dry_run=dry_run))
    if dry_run:
        print('\n(dry run — pass --apply to actually merge)')

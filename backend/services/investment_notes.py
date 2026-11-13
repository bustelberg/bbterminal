"""Private notes, shared across verified issuer aliases without requiring fundamentals."""
from datetime import datetime, timezone

from fastapi import HTTPException

from asset_pipeline.isin_util import is_valid_isin
from services.company_notes_identity import REVIEWED_ISINS, canonical_isin, load


def identity(client, isin):
    isin = isin.strip().upper()
    if not is_valid_isin(REVIEWED_ISINS.get(isin, isin)):
        raise HTTPException(422, 'A valid stock ISIN is required for Notes.')
    grid = (client.table('asset_grid').select('isin,company_id').eq('isin', isin).limit(1).execute().data or [])
    links, aliases = load(grid or [{'isin': isin}], client)
    target = canonical_isin(isin, aliases)
    if not target:
        raise HTTPException(409, 'The issuer alias contains a cycle. Review the company identity before saving Notes.')
    cid = links[isin].get('company_id')
    keys = {isin, target} | {key for key in aliases if canonical_isin(key, aliases) == target}
    return target, cid, sorted(keys)


def read(client, isin, actor):
    _, cid, keys = identity(client, isin)
    notes = []
    if cid is not None:
        notes += (client.table('company_investment_note').select('thesis,pillars,updated_at')
                  .eq('company_id', cid).eq('user_id', actor['id']).limit(1).execute().data or [])
    try:
        notes += (client.table('instrument_investment_note').select('thesis,pillars,updated_at')
                  .in_('isin', keys).eq('user_id', actor['id']).execute().data or [])
    except Exception as exc:
        # Keep established company Notes readable during a migration rollout.
        if not notes and cid is None:
            raise HTTPException(503, 'Stock Notes storage is unavailable. Apply migration '
                                '20261113180000_instrument_investment_notes.sql and retry.') from exc
    return max(notes, key=lambda row: str(row.get('updated_at') or '')) if notes else {
        'thesis': '', 'pillars': [], 'updated_at': None}


def save(client, isin, actor, thesis, pillars):
    thesis = thesis.strip()
    pillars = [pillar.strip() for pillar in pillars if pillar.strip()]
    if len(thesis) > 12000 or len(pillars) > 12 or any(len(pillar) > 1500 for pillar in pillars):
        raise HTTPException(422, 'Keep the thesis and pillars concise.')
    target, cid, _ = identity(client, isin)
    table, key, value = ('company_investment_note', 'company_id', cid) if cid is not None else (
        'instrument_investment_note', 'isin', target)
    payload = {key: value, 'user_id': actor['id'], 'user_email': actor.get('email'),
               'thesis': thesis, 'pillars': pillars, 'updated_at': datetime.now(timezone.utc).isoformat()}
    try:
        rows = client.table(table).upsert(payload, on_conflict=f'{key},user_id').execute().data or []
    except Exception as exc:
        raise HTTPException(503, 'Notes could not be saved. Check database availability and the instrument Notes migration, then retry; your editor text has been retained.') from exc
    return rows[0] if rows else payload

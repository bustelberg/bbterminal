"""Issuer links for annotations only. Never use these links to select a price series."""
from index_universe.acwi.identity import issuer_key

# Reviewed: the USD ADR and Taiwan ordinary represent the same company.
REVIEWED_ISINS = {
    'US8740391003': 'TW0002330008',  # TSMC ADR / Taiwan ordinary.
    'US0567521085': 'KYG070341048',  # Baidu ADR / Hong Kong ordinary.
    'INE040A01034': 'US40415F1012',   # HDFC ordinary / existing issuer record.
    'KR7005930003': 'US7960508882',  # Samsung ordinary / existing issuer record.
    'US4385162056': 'US4385161066',  # Honeywell's current / stored issuer identifier.
    'JP3786800000': 'JP3837800006',  # Legacy AIRS HOYA identifier; not a price alias.
}
# Read-only production audit, 13 November 2026: these company records have no
# ISIN. Identify them by reviewed exchange + ticker + issuer, never a local ID.
REVIEWED_TICKERS = {
    'GB0004052071': ('LSE', 'HLMA', 'halma'),
    'GB0032398678': ('LSE', 'JDG', 'judges scientific'),
    'GB00B5ZN1N88': ('LSE', 'SGRO', 'segro'),
}


def canonical_isin(isin, aliases):
    seen = set()
    while isin in aliases:
        if isin in seen:
            return None
        seen.add(isin)
        isin = aliases[isin]
    return isin


def resolve(rows, companies, aliases):
    """Return exact, unambiguous annotation identities and actionable unresolved reasons."""
    aliases = {**aliases, **REVIEWED_ISINS}
    by_isin = {}
    by_ticker = {}
    for company in companies:
        if company.get('out_of_scope_at') is None and company.get('company_id') is not None:
            by_isin.setdefault(company.get('isin'), set()).add(company['company_id'])
            exchange = company.get('exchange_code') or (company.get('gurufocus_exchange') or {}).get('exchange_code')
            anchor = (exchange, company.get('gurufocus_ticker'), issuer_key(company.get('company_name')))
            by_ticker.setdefault(anchor, set()).add(company['company_id'])
    result = {}
    for row in rows:
        isin = row.get('isin')
        if not isin:
            continue
        target = canonical_isin(isin, aliases)
        if row.get('company_id') is not None:
            result[isin] = {'company_id': row['company_id'], 'method': 'existing', 'matched_isin': isin}
            continue
        if target is None:
            result[isin] = {'company_id': None, 'reason': 'The configured ISIN aliases contain a cycle; review the company link.'}
            continue
        matched_isin = target if by_isin.get(target) else isin
        matches = by_isin.get(matched_isin, set())
        method = 'alias' if matched_isin != isin else 'exact_isin'
        if not matches and isin in REVIEWED_TICKERS:
            matches = by_ticker.get(REVIEWED_TICKERS[isin], set())
            method = 'reviewed_exchange_ticker'
        if len(matches) == 1:
            result[isin] = {'company_id': next(iter(matches)), 'method': method,
                            'matched_isin': matched_isin}
        else:
            reason = (f'Multiple active company records match {target}; review duplicates before saving notes.' if matches
                      else f'No active company record matches {isin}' + (f' or its canonical ISIN {target}' if target != isin else '')
                      + '. Add a verified issuer link or company record; prices alone do not establish a notes identity.')
            result[isin] = {'company_id': None, 'reason': reason}
    return result


def load(rows, client):
    """Batch the database reads for all requested stocks, including alias chains."""
    from deps import fetch_in_chunks
    aliases = {}
    offset = 0
    while True:
        page = (client.table('asset_isin_alias').select('isin,canonical_isin').order('isin')
                .range(offset, offset + 499).execute().data or [])
        aliases.update({row['isin']: row['canonical_isin'] for row in page})
        if len(page) < 500:
            break
        offset += len(page)
    aliases = {**aliases, **REVIEWED_ISINS}
    isins = {row['isin'] for row in rows if row.get('isin')}
    targets = {canonical_isin(isin, aliases) for isin in isins}
    columns = 'company_id,isin,out_of_scope_at,company_name,gurufocus_ticker,gurufocus_exchange:gurufocus_exchange(exchange_code)'
    companies = fetch_in_chunks(sorted(isins | (targets - {None})), lambda chunk:
        client.table('company').select(columns).in_('isin', chunk).execute())
    tickers = sorted({REVIEWED_TICKERS[isin][1] for isin in isins if isin in REVIEWED_TICKERS})
    if tickers:
        companies += fetch_in_chunks(tickers, lambda chunk:
            client.table('company').select(columns).in_('gurufocus_ticker', chunk).execute())
    return resolve(rows, companies, aliases), aliases

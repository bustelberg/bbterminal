"""Read-only audit of Notes coverage across current accounts and model holdings.

Run from backend: uv run python scripts/audit_portfolio_notes.py --db-env-file ../scripts/.env.local
Uses a read-only transaction. Does not read note contents or write production data.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import dotenv_values
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.company_notes_identity import REVIEWED_ISINS, canonical_isin, resolve


def summarize(rows, companies, aliases):
    from asset_pipeline.isin_util import is_valid_isin
    from routers._airs_holding_isin import _looks_like_bond
    from routers._airs_portfolio_analysis import _class_from_categorie, _is_fund
    links = resolve(rows, companies, aliases)
    stocks = [row for row in rows if (row.get('asset_class') or '').lower() == 'equity'
              and not _looks_like_bond(row.get('holding_name'), row.get('name'))
              and not _is_fund(row, row.get('holding_name') or '')
              and 'topselectie' not in (row.get('holding_name') or '').casefold()
              and 'leonteq secs' not in (row.get('name') or '').lower()
              and not any(_class_from_categorie(category) == 'Bonds' for category in row.get('categories', []))]
    def item(row):
        return {'isin': row['isin'], 'name': row.get('holding_name') or row.get('name'),
                'portfolios': row.get('portfolios', [])}
    repaired, fallback, blocked = [], [], []
    for row in stocks:
        link = links[row['isin']]
        if row.get('company_id') is not None:
            continue
        if link.get('company_id') is not None:
            repaired.append({**item(row), 'link': link})
        elif is_valid_isin(REVIEWED_ISINS.get(row['isin'], row['isin'])) and canonical_isin(row['isin'], {**aliases, **REVIEWED_ISINS}):
            fallback.append({**item(row), 'reason': link.get('reason')})
        else:
            blocked.append({**item(row), 'reason': link.get('reason') or 'Invalid or missing ISIN'})
    return {'checked_at': datetime.now(timezone.utc).isoformat(), 'unique_holdings': len(rows),
            'direct_stocks': len(stocks), 'existing_company_links': len(stocks)-len(repaired)-len(fallback)-len(blocked),
            'resolved_by_bridge': repaired, 'isin_notes_fallback': fallback, 'blocked': blocked,
            'unclassified_holdings': [item(row) for row in rows if not row.get('asset_class')]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-env-file')
    parser.add_argument('--output')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    config = dotenv_values(args.db_env_file) if args.db_env_file else {
        **dotenv_values(root/'.env'), **dotenv_values(root/'.env.local'), **os.environ}
    url = config.get('PROD_DB_URL') or config.get('SUPABASE_DB_URL') or config.get('DATABASE_URL')
    if not url:
        parser.error('No database URL configured. Supply --db-env-file with PROD_DB_URL or SUPABASE_DB_URL.')
    with psycopg.connect(url, connect_timeout=8,
                          options='-c default_transaction_read_only=on -c statement_timeout=30000', row_factory=dict_row) as conn:
        rows = conn.execute("""WITH latest AS (
            SELECT portefeuille,max(as_of_date) as_of_date FROM airs_holding GROUP BY portefeuille
        ), held AS (
            SELECT h.isin,h.holding_name name,h.portefeuille portfolio,NULL::text category
              FROM airs_holding h JOIN latest l USING(portefeuille,as_of_date) WHERE h.isin IS NOT NULL
            UNION ALL SELECT p.isin,p.fonds,'model:'||m.name,p.categorie
              FROM airs_model_portfolio_position p JOIN airs_model_portfolio m ON m.id=p.portfolio_id WHERE p.isin IS NOT NULL
        ) SELECT h.isin,min(h.name) holding_name,array_agg(DISTINCT portfolio ORDER BY portfolio) portfolios,
            array_remove(array_agg(DISTINCT category),NULL) categories,
            g.name,g.company_id,g.asset_class,g.status,g.yahoo_symbol,g.sector
          FROM held h LEFT JOIN asset_grid g ON g.isin=h.isin
          GROUP BY h.isin,g.name,g.company_id,g.asset_class,g.status,g.yahoo_symbol,g.sector ORDER BY h.isin""").fetchall()
        companies = conn.execute("""SELECT c.company_id,c.isin,c.company_name,c.gurufocus_ticker,c.out_of_scope_at,g.exchange_code
            FROM company c LEFT JOIN gurufocus_exchange g USING(exchange_id) ORDER BY c.company_id""").fetchall()
        aliases = {row['isin']: row['canonical_isin'] for row in conn.execute('SELECT isin,canonical_isin FROM asset_isin_alias').fetchall()}
        missing = conn.execute("""WITH latest AS (SELECT portefeuille,max(as_of_date) as_of_date FROM airs_holding GROUP BY portefeuille), missing AS (
            SELECT h.holding_name name,h.portefeuille portfolio FROM airs_holding h JOIN latest l USING(portefeuille,as_of_date)
              WHERE h.isin IS NULL OR trim(h.isin)=''
            UNION ALL SELECT p.fonds,'model:'||m.name FROM airs_model_portfolio_position p JOIN airs_model_portfolio m ON m.id=p.portfolio_id
              WHERE p.isin IS NULL OR trim(p.isin)='')
            SELECT name,array_agg(DISTINCT portfolio ORDER BY portfolio) portfolios FROM missing GROUP BY name ORDER BY name""").fetchall()
        from routers._airs_portfolio_analysis import _is_fund
        report = summarize(rows, companies, aliases)
        report['missing_stock_identifiers'] = [dict(row) for row in missing
            if (row.get('name') or '').strip().casefold() not in {'effectenrekening', 'liquiditeiten', 'cash'}
            and not _is_fund({}, row.get('name') or '')]
        conn.rollback()
    rendered = json.dumps(report, indent=2) + '\n'
    if args.output:
        Path(args.output).write_text(rendered, encoding='utf-8')
    print(rendered)


if __name__ == '__main__':
    try:
        main()
    except psycopg.Error as exc:
        print(f'Read-only Notes audit could not complete ({type(exc).__name__}). Check database URL, DNS and credentials. No data was written.', file=sys.stderr)
        raise SystemExit(1)

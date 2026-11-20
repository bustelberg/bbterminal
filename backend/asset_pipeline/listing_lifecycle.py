"""Reviewed end dates for exact securities; never infer delisting from empty Yahoo bars.

These records explain why a current close cannot exist. They do not provide
prices, convert holdings, or replace a retired security with its acquirer's stock.
"""

ENDED_LISTINGS = {
    ('DE0005190037', 'BMW3.DE'): {
        'last_trading_date': '2026-06-30',
        'event': 'Preferred shares converted 1:1 into ordinary shares DE0005190003',
        'source': 'https://www.bmwgroup.com/en/investor-relations/aktie.html',
    },
    ('US2855121099', 'EA'): {
        'last_trading_date': '2026-08-04',
        'event': 'Cash acquisition completed; Nasdaq suspended trading from August 5',
        'source': 'https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2026-545',
    },
    ('US0534841012', 'AVB'): {
        'last_trading_date': '2026-08-14',
        'event': 'AvalonBay merged into Vivmark on August 17; exchanged shares require corporate-action reconciliation',
        'source': 'https://www.sec.gov/Archives/edgar/data/915912/000119312526354182/xslF345X06/ownership.xml',
    },
    ('US49177J1025', 'KVUE'): {
        'last_trading_date': '2026-10-30',
        'event': 'Kenvue acquisition completed; trading ceased before the November 2 market open',
        'source': 'https://kimberlyclark.gcs-web.com/news-releases/news-release-details/kimberly-clark-completes-acquisition-kenvue-creating-preeminent',
    },
}


def ended_listing(isin, symbol, expected):
    record = ENDED_LISTINGS.get(((isin or '').strip().upper(), (symbol or '').strip().upper()))
    return record if record and expected > record['last_trading_date'] else None

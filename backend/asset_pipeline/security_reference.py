"""Reviewed ordinary-share references for issuer metadata and valuation inputs.

These relationships do not establish price parity or depositary-receipt ratios.
"""

# Reviewed depositary identities:
# https://depositaryreceipts.citi.com/adr/guides/pgm_d.aspx?cusip=Y5217N118
# https://depositaryreceipts.citi.com/adr/guides/pgm_d.aspx?cusip=78392B206
ORDINARY_SHARE_REFERENCES = {
    'US8740391003': 'TW0002330008',
    'USY5217N1183': 'INE018A01030',
    'US78392B2060': 'KR7000660001',
}

ISSUER_METADATA = ('company_id', 'name', 'sector', 'sector_default', 'sector_overridden',
                   'country', 'msci_region', 'domicile_country', 'asset_class')

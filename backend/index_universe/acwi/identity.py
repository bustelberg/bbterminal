"""Exact issuer identities. Listing repairs must not use merger aliases as price identities."""
import re

_SUFFIX = re.compile(
    r'[\s,.]+(inc|inc\.|corp|corporation|co|company|plc|ltd|limited|nv|n\.v\.|sa|s\.a\.|ag|se|ab|'
    r'as|a/s|oyj|spa|s\.p\.a\.|holding|holdings|group|the|reit|units|trust|usd|eur|hkd|inr|jpy|cad|gbp)\.?$', re.I)
_CLASS = re.compile(
    r'[\s,.]+(class\s+[a-c]|cl\s+[a-c]|series\s+[a-c]|[ab]\s+shares|shares\s+[ab]|'
    r'sponsored\s+adr|unsponsored\s+adr|adr|ads|reg\s*s|registered)\.?$', re.I)
_ALIASES = {
    'linde eu': 'linde',
    'capitaland integr comm tr': 'capitaland integrated commercial',
    'capitaland integrated commercial t': 'capitaland integrated commercial',
    'pure storage': 'everpure',  # Same security/CUSIP; PSTG became P on 2026-04-17.
    'equity resident': 'equity residential',
    'avalonbay comm': 'avalonbay communities',
}
# Source: https://investors.vivmarkresidential.com/news-events/press-releases/detail/113/
# Do not apply this to historical snapshots or to the listing used for price history.
_SUCCESSORS = {
    'equity residential': ('2026-08-18', 'vivmark residential'),
    'avalonbay communities': ('2026-08-18', 'vivmark residential'),
}


def issuer_key(name: str | None) -> str:
    text = re.sub(r'\s+', ' ', (name or '').strip().lower().replace('&', ' and ').replace('-', ' '))
    for _ in range(8):
        cleaned = _CLASS.sub('', _SUFFIX.sub('', text)).strip(' ,. ')
        if not cleaned or cleaned == text:
            break
        text = cleaned
    return _ALIASES.get(text, text)


def membership_key(name: str | None, as_of: str | None) -> str:
    key = issuer_key(name)
    event = _SUCCESSORS.get(key)
    return event[1] if event and as_of and as_of >= event[0] else key


def share_class(name: str | None) -> str | None:
    match = re.search(r'\b(?:class|cl|series)\s+([a-z])\b|\b([ab])\s+shares\b', name or '', re.I)
    return next((group.lower() for group in match.groups() if group), None) if match else None

"""Exact issuer identities. Listing repairs must not use merger aliases as price identities."""
import re
import unicodedata

_SUFFIX = re.compile(
    r'[\s,.]+(inc|inc\.|corp|corporation|co|company|plc|ltd|limited|nv|n\.v\.|sa|s\.a\.|ag|se|ab|'
    r'as|a/s|oyj|spa|s\.p\.a\.|holding|holdings|group|the|reit|units|trust|usd|eur|hkd|inr|jpy|cad|gbp|\(publ\))\.?$', re.I)
_CLASS = re.compile(
    r'[\s,.]+(class[\s.]+[a-c]|cl[\s.]*[a-c]|series[\s.]+[a-c]|[a-c]\s+shares|shares\s+[a-c]|'
    r'sponsored\s+adr|unsponsored\s+adr|adr|ads|reg\s*s|registered)\.?$', re.I)
# Only explicit class notation: never discard an arbitrary final letter/word.
# Keep this evidence available to listing resolution, which must distinguish A/B/C.
_SHORT_CLASS = re.compile(
    r'(?:\s+[-–—]\s*|\b(?:inc\.?|corp\.?|corporation|ab)\s+)([a-c])\.?$', re.I)
_ALIASES = {
    'linde eu': 'linde',
    'capitaland integr comm tr': 'capitaland integrated commercial',
    'capitaland integrated commercial t': 'capitaland integrated commercial',
    'pure storage': 'everpure',  # Same security/CUSIP; PSTG became P on 2026-04-17.
    'equity resident': 'equity residential',
    'avalonbay comm': 'avalonbay communities',
    'shopify subordinate voting': 'shopify',
    'shopify subordinate voting inc cla': 'shopify',
}
# Source: https://investors.vivmarkresidential.com/news-events/press-releases/detail/113/
# Do not apply this to historical snapshots or to the listing used for price history.
_SUCCESSORS = {
    'equity residential': ('2026-08-18', 'vivmark residential'),
    'avalonbay communities': ('2026-08-18', 'vivmark residential'),
}


def issuer_key(name: str | None) -> str:
    text = unicodedata.normalize('NFC', name or '').strip().rstrip('_').strip()
    shorthand = _SHORT_CLASS.search(text)
    if shorthand:
        text = text[:shorthand.start(1)].rstrip(' -–—')
    text = re.sub(r'\s+', ' ', text.lower().replace('&', ' and ').replace('-', ' '))
    for _ in range(8):
        cleaned = _CLASS.sub('', _SUFFIX.sub('', text)).strip(' ,. ')
        if not cleaned or cleaned == text:
            break
        text = cleaned
    # Reviewed spelling variants of this one issuer. AIRS exports may use a
    # backtick and a space; the iShares file uses LOREAL SA. Keep this exact,
    # rather than stripping punctuation/accents from every company's identity.
    if re.fullmatch(r"l[\s'`´’‘ʼ]*or[eé]al", text):
        return 'loreal'
    return _ALIASES.get(text, text)


def membership_key(name: str | None, as_of: str | None) -> str:
    key = issuer_key(name)
    event = _SUCCESSORS.get(key)
    return event[1] if event and as_of and as_of >= event[0] else key


def share_class(name: str | None) -> str | None:
    text = (name or '').strip().rstrip('_').strip()
    match = _SHORT_CLASS.search(text)
    if match:
        return match.group(1).lower()
    match = re.search(r'\b(?:class|cl|series)[\s.]*([a-z])\b|\b([a-c])\s+shares\b|\bshares\s+([a-c])\b', text, re.I)
    return next((group.lower() for group in match.groups() if group), None) if match else None

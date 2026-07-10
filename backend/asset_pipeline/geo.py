"""Country / continent / region for an asset — DERIVED, never fetched.

Two independent notions of "where an instrument is", deliberately kept apart:

  * LISTING country — the venue the security trades on. Derived from data we
    already store (`asset_execution.exchange`, or the analysis symbol's Yahoo
    suffix), so it costs ZERO Yahoo calls and it resolves for ETFs too.
  * DOMICILE country — where the ISSUER is headquartered. Only Yahoo's v10
    `assetProfile.country` knows this: one request per symbol, and it is ABSENT
    for ETFs, crypto and futures.

They disagree often: Linde (`LIN`) lists in the US and domiciles in the UK;
Alibaba's ADR lists in the US and domiciles in China; Tencent (`0700.HK`) lists
in Hong Kong and domiciles in China. Store both; prefer domicile when present.

`continent` is GEOGRAPHIC (Israel -> Asia, Turkey -> Asia). `msci_region` is
FINANCIAL, following MSCI's ACWI buckets (Israel -> Europe, South Korea ->
Emerging Markets), because the developed/emerging split drives correlated
returns in a way that landmass does not. Neither is fetched — Yahoo has no
continent or region field on any endpoint.

CAVEAT — an ETF's geography is a property of its HOLDINGS, not its listing or
its issuer. `EEM` lists in the US and holds nothing but emerging-market equity.
Nothing here fixes that: an ETF row carries its LISTING country, and that is all
it means. Region-bucketing ETFs needs holdings data we don't have.

Yahoo's country strings are the canonical spelling ("Czech Republic", not
"Czechia"; "South Korea", not "Korea, Republic of") — `normalize_country` folds
the common aliases onto them.
"""
from __future__ import annotations

import unicodedata

# Asset classes with no meaningful geography: a crypto pair, a futures contract,
# an FX cross and an index are not listed in a country in any useful sense.
_NON_GEO_CLASSES = frozenset({"crypto", "commodity", "fx", "index"})


def _ascii(s: str) -> str:
    """Fold accents + case so 'São Paulo' matches the ASCII map key."""
    n = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in n if not unicodedata.combining(c)).strip().casefold()


# ---------------------------------------------------------------------------
# Listing venue -> country.
#
# Keys are Yahoo `fullExchangeName` values, accent-folded + casefolded. Every
# one of the 58 values present in `asset_execution.exchange` today is pinned
# here (and in tests/test_geo.py) so a silent map gap can't classify a listing
# as unknown. Extras beyond those 58 are venues we don't hold yet but plausibly
# will.
# ---------------------------------------------------------------------------
_EXCHANGE_COUNTRY_RAW: dict[str, str] = {
    # United States
    "NYSE": "United States", "NYSE American": "United States",
    "NYSEArca": "United States", "NasdaqGS": "United States",
    "NasdaqGM": "United States", "NasdaqCM": "United States",
    "Nasdaq": "United States", "Cboe US": "United States",
    "Cboe BZX": "United States", "OTC Markets OTCPK": "United States",
    "OTC Markets OTCQX": "United States", "OTC Markets OTCQB": "United States",
    "OTC Markets OTCID": "United States",
    # Canada
    "Toronto": "Canada", "TSXV": "Canada", "Canadian Sec": "Canada",
    "Cboe CA": "Canada", "NEO": "Canada",
    # United Kingdom / Ireland
    "LSE": "United Kingdom", "Cboe UK": "United Kingdom",
    "IOB": "United Kingdom",              # LSE's International Order Book (GDRs)
    "Irish": "Ireland",
    # Germany — every regional bourse resolves to the same country
    "XETRA": "Germany", "Frankfurt": "Germany", "Munich": "Germany",
    "Stuttgart": "Germany", "Dusseldorf": "Germany", "Hamburg": "Germany",
    "Hanover": "Germany", "Berlin": "Germany",
    # Rest of Europe
    "Paris": "France", "Amsterdam": "Netherlands", "Brussels": "Belgium",
    "Milan": "Italy", "MCE": "Spain",     # Mercado Continuo Espanol
    "Swiss": "Switzerland", "Stockholm": "Sweden", "Oslo": "Norway",
    "Copenhagen": "Denmark", "Helsinki": "Finland", "Lisbon": "Portugal",
    "Vienna": "Austria", "Athens": "Greece", "Warsaw": "Poland",
    "Prague": "Czech Republic", "Budapest": "Hungary", "Istanbul": "Turkey",
    "Cboe Europe": "Netherlands",         # Cboe Europe re-domiciled to Amsterdam
    # Asia-Pacific
    "Tokyo": "Japan", "HKSE": "Hong Kong", "Shanghai": "China",
    "Shenzhen": "China", "SES": "Singapore", "KSE": "South Korea",
    "KOSDAQ": "South Korea", "Taiwan": "Taiwan", "Taipei Exchange": "Taiwan",
    "NSE": "India", "BSE": "India", "Jakarta": "Indonesia",
    "Bangkok": "Thailand", "KLSE": "Malaysia", "Philippines": "Philippines",
    "ASX": "Australia", "Cboe AU": "Australia", "NZSE": "New Zealand",
    # Middle East / Africa / Latin America
    "Tel Aviv": "Israel", "Saudi": "Saudi Arabia", "Johannesburg": "South Africa",
    "Sao Paulo": "Brazil", "Mexico": "Mexico", "Santiago": "Chile",
    "Buenos Aires": "Argentina",
}
EXCHANGE_COUNTRY: dict[str, str] = {_ascii(k): v for k, v in _EXCHANGE_COUNTRY_RAW.items()}


# ---------------------------------------------------------------------------
# Yahoo symbol suffix -> country. `""` (no dot) means a US listing, but ONLY for
# an equity/etf — `BTC-USD`, `GC=F` and `^GSPC` are suffix-less too, which is
# why `country_from_symbol` gates on asset_class first.
# ---------------------------------------------------------------------------
SUFFIX_COUNTRY: dict[str, str] = {
    "": "United States",
    ".TO": "Canada", ".V": "Canada", ".NE": "Canada", ".CN": "Canada",
    ".L": "United Kingdom", ".IR": "Ireland",
    # Pan-European MTFs + the LSE's depositary-receipt book. These list FOREIGN
    # issuers on a UK/NL venue (Atlas Copco on Cboe UK in SEK; Samsung's GDR on
    # the IOB in USD), so listing_country here is genuinely the venue's country
    # and the domicile backfill is what recovers Sweden / South Korea.
    ".IL": "United Kingdom",   # IOB — LSE International Order Book (GDRs)
    ".XC": "United Kingdom",   # CXE — Cboe UK
    ".XD": "Netherlands",      # DXE — Cboe Europe (Amsterdam)
    ".XA": "Australia",        # CXA — Cboe Australia
    ".DE": "Germany", ".F": "Germany", ".MU": "Germany", ".SG": "Germany",
    ".BE": "Germany", ".DU": "Germany", ".HM": "Germany", ".HA": "Germany",
    ".PA": "France", ".AS": "Netherlands", ".BR": "Belgium", ".MI": "Italy",
    ".MC": "Spain", ".SW": "Switzerland", ".ST": "Sweden", ".OL": "Norway",
    ".CO": "Denmark", ".HE": "Finland", ".LS": "Portugal", ".VI": "Austria",
    ".AT": "Greece", ".WA": "Poland", ".PR": "Czech Republic",
    ".BD": "Hungary", ".IS": "Turkey",
    ".T": "Japan", ".HK": "Hong Kong", ".SS": "China", ".SZ": "China",
    ".SI": "Singapore", ".KS": "South Korea", ".KQ": "South Korea",
    ".TW": "Taiwan", ".TWO": "Taiwan", ".NS": "India", ".BO": "India",
    ".JK": "Indonesia", ".BK": "Thailand", ".KL": "Malaysia",
    ".AX": "Australia", ".NZ": "New Zealand",
    ".TA": "Israel", ".SR": "Saudi Arabia", ".JO": "South Africa",
    ".SA": "Brazil", ".MX": "Mexico", ".SN": "Chile", ".BA": "Argentina",
}


# Yahoo mostly emits these exact strings; fold the variants seen in the wild.
_COUNTRY_ALIASES: dict[str, str] = {
    "usa": "United States", "u.s.": "United States", "us": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom", "great britain": "United Kingdom",
    "england": "United Kingdom", "scotland": "United Kingdom",
    "czechia": "Czech Republic",
    "korea, republic of": "South Korea", "republic of korea": "South Korea",
    "korea": "South Korea",
    "russian federation": "Russia",
    "viet nam": "Vietnam",
    "hong kong sar": "Hong Kong", "hong kong sar, china": "Hong Kong",
    "taiwan, province of china": "Taiwan",
    "uae": "United Arab Emirates",
    "the netherlands": "Netherlands",
    "macao": "Macau",
}


# --------------------------------------------------------------------------
# Country -> continent. GEOGRAPHIC, not financial: Israel and Turkey sit in
# Asia here even though MSCI buckets them with Europe (see MSCI_REGION).
# --------------------------------------------------------------------------
_CONTINENT_GROUPS: dict[str, tuple[str, ...]] = {
    "Europe": (
        "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic",
        "Denmark", "Estonia", "Finland", "France", "Germany", "Gibraltar",
        "Greece", "Guernsey", "Hungary", "Iceland", "Ireland", "Isle of Man",
        "Italy", "Jersey", "Latvia", "Liechtenstein", "Lithuania", "Luxembourg",
        "Malta", "Monaco", "Netherlands", "Norway", "Poland", "Portugal",
        "Romania", "Russia", "Serbia", "Slovakia", "Slovenia", "Spain",
        "Sweden", "Switzerland", "Ukraine", "United Kingdom",
    ),
    "Asia": (
        "Bahrain", "Bangladesh", "China", "Hong Kong", "India", "Indonesia",
        "Israel", "Japan", "Jordan", "Kazakhstan", "Kuwait", "Macau",
        "Malaysia", "Oman", "Pakistan", "Philippines", "Qatar", "Saudi Arabia",
        "Singapore", "South Korea", "Sri Lanka", "Taiwan", "Thailand",
        "Turkey", "United Arab Emirates", "Vietnam",
    ),
    "Africa": (
        "Egypt", "Ghana", "Kenya", "Mauritius", "Morocco", "Nigeria",
        "South Africa", "Tanzania", "Zambia",
    ),
    "North America": (
        "Bahamas", "Bermuda", "British Virgin Islands", "Canada",
        "Cayman Islands", "Costa Rica", "Dominican Republic", "Jamaica",
        "Mexico", "Panama", "Puerto Rico", "United States",
    ),
    "South America": (
        "Argentina", "Brazil", "Chile", "Colombia", "Ecuador", "Peru",
        "Uruguay", "Venezuela",
    ),
    "Oceania": ("Australia", "Fiji", "New Zealand", "Papua New Guinea"),
}
CONTINENT: dict[str, str] = {
    c: cont for cont, countries in _CONTINENT_GROUPS.items() for c in countries
}


# --------------------------------------------------------------------------
# Country -> MSCI ACWI region. FINANCIAL, not geographic.
#
#   North America / Europe / Pacific  = the three MSCI World (developed) regions
#   Emerging Markets                  = the MSCI EM constituent countries
#
# Israel is developed Europe (MSCI folds "Europe & Middle East" together). South
# Korea and Taiwan remain EM under MSCI's classification. A country absent from
# this map has no MSCI market (Cyprus, Ukraine, Bermuda, Luxembourg, …) and
# yields None, so `resolve_geo` falls back to the LISTING country's region.
# --------------------------------------------------------------------------
_MSCI_GROUPS: dict[str, tuple[str, ...]] = {
    "North America": ("Canada", "United States"),
    "Europe": (
        "Austria", "Belgium", "Denmark", "Finland", "France", "Germany",
        "Ireland", "Israel", "Italy", "Netherlands", "Norway", "Portugal",
        "Spain", "Sweden", "Switzerland", "United Kingdom",
    ),
    "Pacific": ("Australia", "Hong Kong", "Japan", "New Zealand", "Singapore"),
    "Emerging Markets": (
        "Brazil", "Chile", "China", "Colombia", "Czech Republic", "Egypt",
        "Greece", "Hungary", "India", "Indonesia", "Kuwait", "Malaysia",
        "Mexico", "Peru", "Philippines", "Poland", "Qatar", "Saudi Arabia",
        "South Africa", "South Korea", "Taiwan", "Thailand", "Turkey",
        "United Arab Emirates",
    ),
}
MSCI_REGION: dict[str, str] = {
    c: region for region, countries in _MSCI_GROUPS.items() for c in countries
}


def normalize_country(name: str | None) -> str | None:
    """Yahoo's country string -> the canonical spelling used by the maps."""
    if not name or not name.strip():
        return None
    raw = name.strip()
    alias = _COUNTRY_ALIASES.get(raw.casefold())
    if alias:
        return alias
    # Already canonical? (Cheap exact hit before the accent-folded scan.)
    if raw in CONTINENT:
        return raw
    folded = _ascii(raw)
    for known in CONTINENT:
        if _ascii(known) == folded:
            return known
    return raw  # unknown but real (a country we haven't mapped) — keep it visible


def country_from_exchange(exchange: str | None) -> str | None:
    """Yahoo `fullExchangeName` -> listing country. None when unmapped."""
    if not exchange:
        return None
    return EXCHANGE_COUNTRY.get(_ascii(exchange))


def country_from_symbol(symbol: str | None, asset_class: str | None = None) -> str | None:
    """Yahoo analysis symbol -> listing country, via its suffix.

    Returns None for crypto/commodity/fx/index (`BTC-USD`, `GC=F`, `^GSPC` have
    no suffix and would otherwise read as US listings)."""
    if not symbol or (asset_class or "").lower() in _NON_GEO_CLASSES:
        return None
    sym = symbol.strip().upper()
    if sym.startswith("^") or "=" in sym:      # index / futures / FX cross
        return None
    if "." in sym:
        return SUFFIX_COUNTRY.get("." + sym.rsplit(".", 1)[1])
    if "-" in sym and sym.rsplit("-", 1)[1] in ("USD", "EUR", "GBP"):
        return None                            # crypto pair mis-tagged upstream
    return SUFFIX_COUNTRY[""]                  # bare ticker = US listing


def continent_of(country: str | None) -> str | None:
    c = normalize_country(country)
    return CONTINENT.get(c) if c else None


def msci_region_of(country: str | None) -> str | None:
    c = normalize_country(country)
    return MSCI_REGION.get(c) if c else None


def resolve_geo(domicile: str | None, listing: str | None) -> dict[str, str | None]:
    """Fold the two country signals into the three stored fields.

    `country` prefers domicile (where the issuer actually is) over listing.
    `continent` and `msci_region` are resolved INDEPENDENTLY with the same
    domicile-first fallback, so an offshore/unmapped domicile still gets a region
    from the venue: a Cyprus-domiciled company listed in Athens is
    country=Cyprus, continent=Europe, msci_region=Emerging Markets (via Greece).
    """
    dom, lst = normalize_country(domicile), normalize_country(listing)
    return {
        "country": dom or lst,
        "continent": continent_of(dom) or continent_of(lst),
        "msci_region": msci_region_of(dom) or msci_region_of(lst),
    }

"""Detect inverse / short ETFs from their name and propose a Short sector.

For a LONG-ONLY book, inverse ETFs are the vehicle to express a downside view.
Tagging each with the sector it would belong to if held LONG — `Short Equity`,
`Short Real Estate`, `Short Bonds`, `Short Commodity`, `Short FX`, `Short Crypto`,
`Short Single Stock`, … — lets the regime-aware strategy short the RIGHT thing in
a bear/turbulent regime and keeps these out of the normal long rankings. Leverage
(-2x/-3x) is captured as a separate `multiplier`, not baked into the sector.

This is a HEURISTIC: it PROPOSES candidates for human review (see the
`/short-etfs/candidates` + `/apply` endpoints). It is deliberately not wired to
auto-apply — mislabeling direction (long vs short) would be catastrophic for a
strategy, so a person confirms each tag.
"""
from __future__ import annotations

import html
import re


def _clean(name: str | None) -> str:
    """Decode HTML entities in a stored name (some sources persist `&amp;`,
    `&#39;`, … literally) so keyword/override matching sees real characters —
    otherwise "Oil &amp; Gas" or "Brown &amp; Brown" silently fail to match."""
    return html.unescape(name) if name else ""

# The canonical sector taxonomy shared by BOTH equities (real Yahoo GICS sectors,
# normalized) and funds (classified below). "Equity" = a broad/diversified equity
# ETF; "Single Stock" = a single-name ETF; the rest are GICS sectors + the
# non-equity asset classes. Order here is also the review/dropdown order.
CATEGORIES = (
    "Equity", "Technology", "Financials", "Healthcare", "Consumer Cyclical",
    "Consumer Defensive", "Communication Services", "Industrials", "Energy",
    "Materials", "Utilities", "Real Estate", "Single Stock", "Bonds",
    "Commodity", "FX", "Crypto",
)
SHORT_SECTORS = tuple(f"Short {c}" for c in CATEGORIES)

# Yahoo's equity assetProfile uses two names that differ from our canonical set;
# everything else is identical. Used by the equity-sector backfill so a stock's
# sector lines up with the fund categories (a "Short Financials" ETF vs its
# "Financials" constituents).
_YAHOO_SECTOR_MAP = {"Financial Services": "Financials", "Basic Materials": "Materials"}


def normalize_sector(yahoo_sector: str | None) -> str | None:
    """Map a raw Yahoo equity sector onto the canonical taxonomy."""
    if not yahoo_sector:
        return yahoo_sector
    s = yahoo_sector.strip()
    return _YAHOO_SECTOR_MAP.get(s, s)


# Curated sector for specific securities that neither the name-heuristic NOR
# Yahoo assetProfile can place — foreign-listed operating companies (whose
# resolved listing Yahoo won't sector) + niche investment funds. Matched by a
# lowercase name substring; first hit wins. Extend as new ones surface (this is
# the manual-knowledge layer, like the `company_override` table for equities).
_KNOWN_SECTOR: tuple[tuple[str, str], ...] = (
    ("brown & brown", "Financials"),                 # insurance brokerage
    ("rwe ag", "Utilities"),                          # German electric utility
    ("lkq", "Consumer Cyclical"),                     # auto-parts distribution
    ("xtb", "Financials"),                            # online forex/CFD broker
    ("amedeo air", "Industrials"),                    # aircraft leasing fund
    ("fair oaks", "Bonds"),                           # CLO / structured credit
    ("mednation", "Healthcare"),                      # rehab / medical care
    ("procimmo", "Real Estate"),                      # Swiss real-estate fund
    ("solvalor", "Real Estate"),                      # Swiss real-estate fund
    ("space economy", "Industrials"),                 # space/aerospace & defence
    ("meme stock", "Equity"),                         # broad equity ETF
    ("international dynamic equity", "Equity"),        # diversified intl equity ETF
)


def known_sector(name: str | None) -> str | None:
    """Curated sector override for a security the heuristics can't place, else None."""
    low = _clean(name).lower()
    if not low:
        return None
    for sub, sec in _KNOWN_SECTOR:
        if sub in low:
            return sec
    return None

# Long category sectors (no "Short " prefix) — for the general ETF classifier.
SECTORS = CATEGORIES

# Fund-like classes to reclassify (equities keep their real Yahoo sectors and are
# left alone). Also the set gated for INVERSE detection.
CANDIDATE_CLASSES = ("etf", "crypto", "commodity", "fx", "index", "bond")

# The lazy asset-class fallback values a `sector` can hold — these are what we
# replace with a real category. A real Yahoo sector ("Technology", "Real Estate")
# is NOT in here, so it's preserved.
FALLBACK_SECTORS = frozenset({"etf", "crypto", "commodity", "fx", "index", "bond"})


def is_fallback_sector(sector: str | None, asset_class: str | None = None) -> bool:
    """True when `sector` is just the asset-class fallback (or empty) — i.e. safe
    to overwrite with a real category. False for a real Yahoo sector."""
    if not sector or not sector.strip():
        return True
    s = sector.strip().lower()
    return s in FALLBACK_SECTORS or (bool(asset_class) and s == asset_class.strip().lower())

# --- direction: is it an inverse product at all? ---
# Long/short & market-neutral funds are NOT directional shorts.
_LONG_SHORT_RE = re.compile(r"long[\s/_-]?short|market[\s-]?neutral|130[\s/-]?30", re.I)
# Umbrella family phrasing that says "inverse" even for the LONG twin
# (e.g. "CSOP Leveraged and Inverse Series - … (2x) Leveraged Product").
_FAMILY_RE = re.compile(r"leveraged\s+and\s+inverse\s+series(?:\s+ii)?", re.I)
# "short" used as bond DURATION (short-maturity fund like SHV/ICSH), NOT inverse.
_DURATION_RE = re.compile(
    r"\b(?:ultra[-\s]?)?short[-\s]?(?:term|dated|duration|maturity|treasur\w*|"
    r"bond|gilt|corporate|government|aggregate|credit|income|muni\w*)\b", re.I)
_INVERSE_RE = re.compile(
    r"\binverse\b|\bbear\b|\bultra[-\s]?short\b|-\s?\d+(?:\.\d+)?\s?x\b|\bshort\b", re.I)
_NEG_MULT_RE = re.compile(r"-\s?(\d+(?:\.\d+)?)\s?x\b", re.I)
_ANY_MULT_RE = re.compile(r"\(?-?\s?(\d+(?:\.\d+)?)\s?x\b", re.I)

# --- category detectors (first hit wins; specific/non-equity before broad) ---
_CCY = r"usd|eur|gbp|jpy|chf|aud|cad|nzd|sek|nok"
_DETECTORS: tuple[tuple[str, re.Pattern], ...] = (
    # Crypto: generic terms + crypto-only issuers (21Shares/Grayscale/CoinShares)
    # + a broad coin roster (full names — unambiguous — plus distinctive tickers).
    # Bare ambiguous tickers (SOL/DOT/ADA/LINK/…) are left OUT — the asset_class
    # == 'crypto' short-circuit in `_category` covers the *-USD rows anyway.
    ("Crypto", re.compile(
        r"\bcrypto\b|\bblockchain\b|\bstaking\b|digital asset|\b21shares\b|\bgrayscale\b|\bcoinshares\b|"
        r"\bbitcoin\b|\bbtc\b|\bethereum\b|\bether\b|\beth\b|\bsolana\b|\bripple\b|\bxrp\b|\bcardano\b|"
        r"\bavalanche\b|\bavax\b|\bdogecoin\b|\bpolkadot\b|\bchainlink\b|\blitecoin\b|\bpolygon\b|\btron\b|"
        r"\bstellar\b|\bxlm\b|\balgorand\b|\bcosmos\b|\btezos\b|\bxtz\b|\bmonero\b|\buniswap\b|\bbinance\b|"
        r"\bbnb\b|\bhyperliquid\b|\btoncoin\b|\bfilecoin\b|\bhedera\b|\bcronos\b|\bvechain\b|\baptos\b|"
        r"\barbitrum\b|\boptimism\b|\bshiba\b|\bpepe\b|\bworldcoin\b|\brender\b|\binjective\b|\bchainlink\b",
        re.I)),
    # Two currency CODES (word-bounded so "EURO STOXX" ≠ FX), or explicit fx wording.
    ("FX", re.compile(rf"\b({_CCY})\b.*\b({_CCY})\b|\bcurrenc|\bfx\b", re.I)),
    # Materials BEFORE Commodity so equity miners (Gold Miners → Materials) beat
    # the raw-metal names (Gold → Commodity). Materials keys on mining/materials
    # words, which a physical-commodity ETP never uses.
    ("Materials", re.compile(r"\bmaterials?\b|\bmining\b|\bminers?\b|\bchemicals?\b|\bsteel\b", re.I)),
    # Commodity = the raw material: metals (incl. base/industrial), energy raws,
    # and softs/agriculture. Bare "oil" matches UNLESS "Oil & Gas" (equity → Energy).
    ("Commodity", re.compile(
        r"\bgold\b|\bsilver\b|\bcrude\b|\bwti\b|\bbrent\b|\boil\b(?!\s*(?:&|and\s+gas))|natural gas|\bcopper\b|"
        r"platinum|palladium|\brhodium\b|\bnickel\b|\baluminium\b|\baluminum\b|\bzinc\b|\btin\b|\blead\b|"
        r"industrial metals|base metals|precious metals|agricultur|\bcoffee\b|\bcotton\b|\bsugar\b|\bwheat\b|"
        r"\bcorn\b|soybean|\bcocoa\b|\bcattle\b|\bhogs\b|\bgrains?\b|livestock|\blumber\b|\bcarbon\b|\bcommodit",
        re.I)),
    ("Bonds", re.compile(r"treasur|\bbond\b|\bbund\b|\bbtp\b|\bgilt\b|high[\s-]?yield|fallen angel|fixed income|\d+\s*[-+]?\s*year|\bcredit\b|aggregate|\bmuni", re.I)),
    ("Real Estate", re.compile(r"real estate|\breit|property fund|\bfoncière\b|foncier|immobili", re.I)),
    ("Utilities", re.compile(r"\butilit", re.I)),
    ("Energy", re.compile(r"\benergy\b|oil\s*&\s*gas|oil and gas|exploration|\be&p\b|\bmlp\b", re.I)),
    ("Industrials", re.compile(r"\bindustrial|aerospace|\bdefen[cs]e\b|\btransport|machinery|\bairlines?\b|railroad|\binfrastructure\b", re.I)),
    ("Financials", re.compile(r"\bbank|financ|insurance|\bbroker", re.I)),
    ("Healthcare", re.compile(r"biotech|pharmaceutic|health\s?care|\bhealth\b|\bmedical\b", re.I)),
    ("Consumer Defensive", re.compile(r"consumer staples|consumer defensive|\bstaples\b|\bfood\b|beverage|household product|\btobacco\b|grocery", re.I)),
    ("Consumer Cyclical", re.compile(r"consumer discretionary|consumer cyclical|\bretail\b|homebuild|\bautomobile|apparel|\bleisure\b|\bgaming\b|restaurant", re.I)),
    ("Communication Services", re.compile(r"communication services|\btelecom|\bmedia\b", re.I)),
    ("Technology", re.compile(r"semiconductor|technolog|\btech\b|software|\binternet\b", re.I)),
)
# Broad-index markers → generic Short Equity; otherwise a single-name short.
_INDEX_RE = re.compile(
    r"s&p|\b\d{3}\b|nasdaq|\bndx\b|\bqqq\b|\bdow\b|\bdj\b|russell|mid[\s-]?cap|small[\s-]?cap|"
    r"\bmsci\b|\bftse\b|\bdax\b|stoxx|nikkei|hang seng|\bhsi\b|\bcsi\b|topix|\bindex\b|"
    r"emerging market|\beafe\b|\beurope\b|\bjapan\b|\bchina\b|brazil|\bworld\b|\bfang",
    re.I,
)


def _multiplier(name: str) -> int:
    """Leverage factor: an explicit `Nx` / `-Nx` / `(2x)`, else ProShares wording
    (UltraPro→3, UltraShort→2), else 1."""
    m = _NEG_MULT_RE.search(name) or _ANY_MULT_RE.search(name)
    if m:
        try:
            return max(1, int(float(m.group(1))))
        except ValueError:
            pass
    low = name.lower()
    if "ultrapro" in low:
        return 3
    if "ultrashort" in low or "ultra short" in low or "ultra-short" in low:
        return 2
    return 1


def _category(name: str, asset_class: str | None = None) -> str:
    """Best-guess underlying category, or "" when nothing specific matches (an
    index-like name resolves to Equity; the empty case is left to the caller's
    direction-appropriate fallback). A `crypto` asset_class is authoritative."""
    if asset_class and asset_class.strip().lower() == "crypto":
        return "Crypto"
    for cat, rx in _DETECTORS:
        if rx.search(name):
            return cat
    return "Equity" if _INDEX_RE.search(name) else ""


def classify_short(name: str | None, asset_class: str | None = None) -> dict | None:
    """Detect an inverse/short ETF from its name. Returns
    ``{"multiplier": int, "category": str, "sector": "Short <category>"}`` or
    ``None`` when it's not a directional inverse product. Skips long/short &
    market-neutral funds and leveraged-LONG products; excludes short-maturity
    bond funds; ignores the "Leveraged and Inverse Series" umbrella so a LONG
    twin isn't flagged."""
    if not name:
        return None
    name = _clean(name)
    if _LONG_SHORT_RE.search(name):
        return None
    probe = _FAMILY_RE.sub(" ", name)     # ignore the umbrella "…and Inverse Series"
    probe = _DURATION_RE.sub(" ", probe)  # ignore bond short-maturity phrasing
    if not _INVERSE_RE.search(probe):
        return None
    # Inverse ETFs are predominantly index or SINGLE-STOCK, so an unmatched name
    # falls back to "Single Stock" (a real, distinct bucket).
    category = _category(name, asset_class) or "Single Stock"
    return {"multiplier": _multiplier(name), "category": category, "sector": f"Short {category}"}


def classify_sector(name: str | None, asset_class: str | None = None) -> dict:
    """Propose a real category sector for ANY fund-like instrument (long OR short).
    Short/inverse → ``Short <cat>`` + a leverage multiplier; long → ``<cat>``
    (an unrecognized-but-fund-like name falls back to ``Equity`` so no bare
    ``etf`` sector survives). Always returns a proposal (never None)."""
    short = classify_short(name, asset_class)
    if short:
        return {"sector": short["sector"], "category": short["category"],
                "multiplier": short["multiplier"], "is_short": True}
    cat = _category(_clean(name), asset_class) or "Equity"
    return {"sector": cat, "category": cat, "multiplier": None, "is_short": False}

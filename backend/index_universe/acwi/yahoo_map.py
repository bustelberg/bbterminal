"""iShares exchange + ticker -> the Yahoo symbol `asset_execution` is keyed by.

⚠⚠ THIS IS THE JOIN THE BUNDLED FILE CAN ACTUALLY SUPPORT. The iShares ACWI export carries no ISIN
column — `Ticker, Name, Sector, Asset Class, Market Value, Weight, ..., Location, Exchange,
Currency` — so ACWI membership has been resolved by NAME, through GuruFocus, which drops every
constituent GuruFocus does not sell us. Measured 2026-09-01: 189 constituents sit in
`asset_execution` priced and healthy while not marked as ACWI, concentrated in Canada (65),
Australia (37), the UK (25) and South Africa (18) — the regions outside the subscription. yfinance
prices all of them.

⚠⚠ A KEY LOOKUP, NOT A FUZZY MATCH, AND THE DIFFERENCE IS THE WHOLE POINT. `scripts/
measure_acwi_asset_gap.py` sized the gap with a name matcher and its own output shows why one must
never ship: of its first twelve matches, `BERKSHIRE HATHAWAY CLASS B` matched Berkshire **A**,
`NEWMONT` (United States) matched the Australian CDI line, and `MIZUHO FINANCIAL GROUP` (Japan)
matched MAGELLAN FINANCIAL GROUP (Australia) — a different company on a different continent, which
is the WisdomTree Coffee -> Luckin Coffee failure reproduced. A ticker plus an exchange is a
deterministic address; a name is a guess.

⚠ AMBIGUOUS EXCHANGES ARE RESOLVED BY `Location`, NEVER GUESSED. `Nasdaq Omx Nordic` is one iShares
label covering Stockholm, Helsinki and Copenhagen, which are three Yahoo suffixes; the row's country
is what separates them. An exchange this module cannot place returns None and the constituent is
left out rather than pointed at a plausible wrong listing.

⚠⚠ THE KEYS ARE THE FILE'S OWN SPELLINGS, AND TWELVE OF THEM USED TO BE SOMEBODY ELSE'S (fixed
2026-09-01). This map was written against the grid's exchange names rather than the export's, so a
dozen keys named a venue the file never writes while the file's own string for that same venue was
absent — `Euronext Brussels` against the file's `Nyse Euronext - Euronext Brussels`, `Borsa
Istanbul` against `Istanbul Stock Exchange`, `Warsaw Stock Exchange` against `Warsaw Stock
Exchange/Equities/Main Market`, `Bombay Stock Exchange` against `Bse Ltd`. A dead key is silent:
the venue resolves to None, the constituent is counted as "venue unknown", and the map LOOKS like
it covers the market. Measured on the 15-Apr-2026 file: 137 of 2,270 equity rows unplaced, of which
78 were instruments we already hold and price.

⚠⚠ A SUFFIX IS ADDED ONLY WHEN A HELD ROW WAS FOUND UNDER IT — never from what Yahoo's convention
is believed to be. Each one below was probed against `asset_execution` and joined: `.CO` 14, `.IS`
12, `.BR` 11, `.WA` 11, `.HE` 10, `.TWO` 6, `.IR` 5, `.AT` 5, `.LS` 3, Cboe BZX bare 1. Every one
joined on the PLAIN ticker plus the suffix, so no space- or dot-mangling is warranted here.

⚠ SO SEVEN VENUES ARE DELIBERATELY STILL UNMAPPED: Kuwait (6 rows), Santiago (10), Kosdaq (4),
Philippines (10), Prague (2), the Egyptian Exchange (2) and `Bse Ltd` (1). We hold NOT ONE of their
constituents, so a suffix for them could not be verified by a join and would be a guess pointing at
a plausible wrong listing — the failure this module exists to avoid. They cost nothing today and
resolve the moment one of their names is ingested and a spelling can be checked.

⚠ RUSSIA (`Standard-Classica-Forts`, 9 rows) IS REFUSED ON PURPOSE, not overlooked. Yahoo delisted
Russian equities; there is no series to point at under any suffix.
"""
from __future__ import annotations

#: iShares `Exchange` -> Yahoo suffix. `''` means a bare US symbol.
#:
#: ⚠ BUILT FROM THE FILE AND THE GRID, not from memory: the keys are the exchange strings this
#: export actually uses and the values are suffixes `asset_execution.yahoo_symbol` actually carries.
EXCHANGE_SUFFIX: dict[str, str] = {
    "NYSE": "", "NASDAQ": "", "Nyse Mkt Llc": "", "Cboe Bzx Exchange": "",
    "Nyse Arca": "", "Bats Exchange": "", "Cboe BZX": "",
    "Toronto Stock Exchange": ".TO", "Cboe Canada": ".TO",
    "London Stock Exchange": ".L",
    "Asx - All Markets": ".AX",
    "Johannesburg Stock Exchange": ".JO",
    "Tokyo Stock Exchange": ".T",
    "Hong Kong Exchanges And Clearing Ltd": ".HK",
    "Shanghai Stock Exchange": ".SS",
    "Shenzhen Stock Exchange": ".SZ",
    "Taiwan Stock Exchange": ".TW",
    "Korea Exchange (Stock Market)": ".KS",
    "National Stock Exchange Of India": ".NS",
    "Bombay Stock Exchange": ".BO", "Bse Ltd": ".BO",
    "Singapore Exchange": ".SI",
    "Bursa Malaysia": ".KL",
    "Indonesia Stock Exchange": ".JK",
    "Stock Exchange Of Thailand": ".BK",
    "Philippine Stock Exchange": ".PS",
    "Gretai Securities Market": ".TWO",
    "Xetra": ".DE",
    "Nyse Euronext - Euronext Paris": ".PA",
    "Euronext Amsterdam": ".AS",
    "Euronext Brussels": ".BR", "Nyse Euronext - Euronext Brussels": ".BR",
    "Euronext Lisbon": ".LS", "Nyse Euronext - Euronext Lisbon": ".LS",
    "Irish Stock Exchange - All Market": ".IR",
    "Borsa Italiana": ".MI",
    "Bolsa De Madrid": ".MC",
    "SIX Swiss Exchange": ".SW",
    "Wiener Boerse Ag": ".VI",
    "Warsaw Stock Exchange": ".WA", "Warsaw Stock Exchange/Equities/Main Market": ".WA",
    "Bolsa Mexicana De Valores": ".MX",
    "XBSP": ".SA",
    "Bolsa De Valores De Colombia": ".CL",
    "Saudi Stock Exchange": ".SR",
    "Abu Dhabi Securities Exchange": ".AE",
    "Dubai Financial Market": ".AE",
    "Tel Aviv Stock Exchange": ".TA",
    "Borsa Istanbul": ".IS", "Istanbul Stock Exchange": ".IS",
    "Qatar Exchange": ".QA",
    "New Zealand Exchange Ltd": ".NZ",
    "Oslo Bors Asa": ".OL",
    "Omx Nordic Exchange Copenhagen A/S": ".CO",
    "Nasdaq Omx Helsinki Ltd.": ".HE",
    "Athens Stock Exchange": ".AT", "Athens Exchange S.A. Cash Market": ".AT",
    "Budapest Stock Exchange": ".BD",
}

#: `Nasdaq Omx Nordic` is one label over three markets — see the module note.
#:
#: ⚠ THE 15-Apr-2026 FILE NAMES THE NORDIC MARKETS INDIVIDUALLY (`Omx Nordic Exchange Copenhagen
#: A/S`, `Nasdaq Omx Helsinki Ltd.`) and never uses the shared label, so those two are plain keys
#: above. This stays for the export that does use it — one label whose suffix depends on the row.
NORDIC_BY_COUNTRY: dict[str, str] = {
    "Sweden": ".ST", "Finland": ".HE", "Denmark": ".CO", "Iceland": ".IC",
}


def yahoo_symbol(ticker: str, exchange: str, location: str = "") -> str | None:
    """`('CSU', 'Toronto Stock Exchange')` -> `'CSU.TO'`, or None when the venue is unknown.

    ⚠ HONG KONG IS ZERO-PADDED TO FOUR DIGITS. iShares files `700`, Yahoo wants `0700.HK`; without
    the pad the symbol is simply absent from the grid and the constituent goes missing for a
    formatting reason that looks like a data gap.

    ⚠ A DOT IN A US TICKER BECOMES A HYPHEN (`BRK.B` -> `BRK-B`) — Yahoo's convention, and the one
    `asset_execution` stores.

    ⚠⚠ A TRAILING DOT IS THE LSE'S OWN PADDING AND MUST GO (`BP.` -> `BP.L`, not `BP..L`). iShares
    files London tickers dotted to a fixed width; appending the suffix to that produced a double
    dot, which matched nothing. It looked harmless because the country-gated interlisting fallback
    then found the right row anyway — so BP, BAE, National Grid and Rolls-Royce were resolving
    through the LOOSE rule when the exact one should have had them, and the table recorded a
    symbol that does not exist.

    ⚠⚠ A SPACE BECOMES A HYPHEN (`NOVO B` -> `NOVO-B.CO`, `NDA FI` -> `NDA-FI.HE`) — the Nordic
    share-class convention. Without it Novo Nordisk and Nordea, both held and priced, resolved to
    nothing at all.

    ⚠ BOTH RULES ARE EVIDENCE, NOT CONVENTION: each was applied only after probing the spelling
    against `asset_execution` and finding the row. See the module note.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return None
    ex = (exchange or "").strip()
    suffix = EXCHANGE_SUFFIX.get(ex)
    if suffix is None and ex == "Nasdaq Omx Nordic":
        suffix = NORDIC_BY_COUNTRY.get((location or "").strip())
    if suffix is None:
        return None
    if suffix == ".HK":
        digits = "".join(c for c in t if c.isdigit())
        return f"{digits.zfill(4)}.HK" if digits else None
    # ⚠⚠ BORSA ISTANBUL'S BOARD CODE IS NOT PART OF THE TICKER. iShares writes `ASELS.E`,
    # `THYAO.E`, `KCHOL.E` — the `.E` is the equity board, and Yahoo's symbol is `ASELS.IS`.
    # Measured: all 12 Turkish rows join after stripping it, and none join before.
    # ⚠ SCOPED TO `.IS` ON PURPOSE. Elsewhere a letter after a dot is a SHARE CLASS — `BBD.B` is
    # Bombardier B, a different security from `BBD.A` — so a general "drop after the dot" would
    # merge two classes into whichever we happen to hold.
    if suffix == ".IS" and t.endswith(".E"):
        t = t[:-2]
    # ⚠ TRAILING SEPARATORS FIRST, then spaces — `BP.` must not become `BP-`.
    t = t.rstrip(".").replace(" ", "-")
    if not t:
        return None
    # ⚠⚠ A SHARE CLASS IS A HYPHEN ON EVERY VENUE, NOT ONLY IN THE US. The US branch has always
    # done this (`BRK.B` -> `BRK-B`); the suffixed branch did not, so `BBD.B` became `BBD.B.TO`,
    # which exists nowhere. Probed against `asset_execution` over all 32 non-US rows carrying an
    # internal dot: **8 join as a hyphen, 0 join as a dot** — Teck, BT Group, Bombardier, CGI,
    # Rogers, CCL, Canadian Tire and Empire, every one of them a constituent we already price.
    # ⚠ The remaining 12 are Thai `.R` NVDR lines, which are a DIFFERENT INSTRUMENT from the
    # ordinary we hold rather than a spelling of it, so no transform reaches them here and
    # `asset_membership`'s country-gated fallback is the right place for them.
    return f"{t.replace('.', '-')}{suffix}"

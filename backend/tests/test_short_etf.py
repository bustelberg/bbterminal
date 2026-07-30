"""Unit tests for the inverse/short-ETF detector (pure name classifier).

Cases drawn from the real detected list so the category routing + false-positive
guards stay pinned.
"""
import pytest

from asset_pipeline.short_etf import (
    classify_sector, classify_short, is_fallback_sector, known_sector, normalize_sector,
)


def _c(name: str) -> tuple[str, int] | None:
    r = classify_short(name, "etf")
    return (r["sector"], r["multiplier"]) if r else None


@pytest.mark.parametrize("name,sector,mult", [
    # Broad equity indices → Short Equity
    ("ProShares Short S&P500", "Short Equity", 1),
    ("ProShares Short QQQ", "Short Equity", 1),
    ("ProShares UltraPro Short QQQ", "Short Equity", 3),       # SQQQ
    ("ProShares UltraShort S&P500", "Short Equity", 2),        # SDS
    ("Direxion Daily MSCI Emerging Markets Bear 3X Shares", "Short Equity", 3),
    ("NEXT FUNDS Nikkei 225 Inverse Index Exchange Traded Fund", "Short Equity", 1),
    ("Xtrackers Euro Stoxx 50 Short Daily Swap UCITS ETF 1C", "Short Equity", 1),
    ("CSOP Leveraged and Inverse Series - CSOP Hang Seng Index Daily (-2x) Inverse Product", "Short Equity", 2),
    # Sector equities
    ("Direxion Daily Real Estate Bear 3X Shares", "Short Real Estate", 3),   # DRV
    ("ProShares Short Real Estate", "Short Real Estate", 1),                 # REK
    ("ProShares UltraShort Energy ETF", "Short Energy", 2),                  # DUG
    ("Direxion Daily S&P Oil & Gas Exp. & Prod. Bear 2X Shares", "Short Energy", 2),  # DRIP
    ("ProShares UltraShort Materials", "Short Materials", 2),                # SMN
    ("WisdomTree EURO STOXX Banks 3x Daily Short", "Short Financials", 3),   # 3BAS
    ("ProShares UltraShort Nasdaq Biotechnology", "Short Healthcare", 2),    # BIS
    # Single stocks
    ("Direxion Daily AAPL Bear 1X Shares", "Short Single Stock", 1),         # AAPD
    ("Tradr 2X Short TSLA Daily ETF", "Short Single Stock", 2),              # TSLQ
    ("Defiance Daily Target 2X Short MSTR ETF", "Short Single Stock", 2),    # SMST
    # Non-equity asset classes
    ("ProShares UltraShort 20+ Year Treasury", "Short Bonds", 2),           # TBT
    ("ProShares Short 20+ Year Treasury", "Short Bonds", 1),                # TBF
    ("Amundi German Bund Daily (-2x) Inverse UCITS ETF", "Short Bonds", 2), # DSB
    ("ProShares Short High Yield", "Short Bonds", 1),                       # SJB
    ("WisdomTree Gold 3x Daily Short", "Short Commodity", 3),               # 3GOS
    ("WisdomTree WTI Crude Oil 3x Daily Short ETN", "Short Commodity", 3),  # 3OIS
    ("BetaPro Silver -2x Daily Bear ETF", "Short Commodity", 2),           # SLVD
    ("WisdomTree Short EUR Long USD 3x Daily", "Short FX", 3),              # SEU3
    ("WisdomTree Short GBP Long USD 3x Daily", "Short FX", 3),             # SGB3
    ("ProShares Short Bitcoin ETF", "Short Crypto", 1),                    # BITI
    ("Csop Bitcoin Futures Daily (-1X) Inverse Product", "Short Crypto", 1),  # 7376.HK
])
def test_category_routing(name, sector, mult):
    assert _c(name) == (sector, mult)


@pytest.mark.parametrize("name", [
    # long/short & market-neutral funds — not directional shorts
    "Convergence Long/Short Equity ETF",
    "First Trust Long/Short Equity ETF",
    "Militia Long/Short Equity ETF",
    # LONG twin of a "Leveraged and Inverse Series" — must NOT flag
    "CSOP Leveraged and Inverse Series - CSOP Hang Seng Index Daily (2x) Leveraged Product",
    # leveraged-LONG products
    "ProShares Ultra S&P500",
    "ProShares UltraPro QQQ",
    # short-maturity bond funds (duration, not inverse)
    "iShares Short Treasury Bond ETF",
    "iShares Ultra Short-Term Bond ETF",
    "VanEck Short Muni ETF",
    # plain long ETF
    "Vanguard S&P 500 ETF",
    "",
])
def test_not_short(name):
    assert classify_short(name, "etf") is None


def test_none_name():
    assert classify_short(None, "etf") is None


def _s(name: str) -> tuple[str, bool]:
    r = classify_sector(name, "etf")
    return (r["sector"], r["is_short"])


@pytest.mark.parametrize("name,sector", [
    # Long broad index → Equity
    ("Vanguard S&P 500 ETF", "Equity"),
    ("iShares Core MSCI World UCITS ETF", "Equity"),
    ("iShares MSCI Japan ETF", "Equity"),
    # Long sector ETFs → their sector (all 11 GICS)
    ("Financial Select Sector SPDR Fund", "Financials"),
    ("Energy Select Sector SPDR Fund", "Energy"),
    ("Vanguard Real Estate ETF", "Real Estate"),
    ("iShares Semiconductor ETF", "Technology"),
    ("SPDR S&P Biotech ETF", "Healthcare"),
    ("Industrial Select Sector SPDR Fund", "Industrials"),
    ("Utilities Select Sector SPDR Fund", "Utilities"),
    ("Consumer Discretionary Select Sector SPDR Fund", "Consumer Cyclical"),
    ("Consumer Staples Select Sector SPDR Fund", "Consumer Defensive"),
    ("Communication Services Select Sector SPDR Fund", "Communication Services"),
    ("iShares U.S. Aerospace & Defense ETF", "Industrials"),
    # Long non-equity → asset-class category
    ("SPDR Gold Shares", "Commodity"),
    ("United States Oil Fund", "Commodity"),
    ("iShares 20+ Year Treasury Bond ETF", "Bonds"),
    ("iShares Core U.S. Aggregate Bond ETF", "Bonds"),
    ("iShares Bitcoin Trust", "Crypto"),
    ("Invesco CurrencyShares Euro Trust", "FX"),
    # Unrecognized fund-like name → Equity fallback (no bare 'etf' sector)
    ("ARK Innovation ETF", "Equity"),
    ("First Trust Multi-Asset Diversified Income ETF", "Equity"),
])
def test_long_category(name, sector):
    assert _s(name) == (sector, False)


@pytest.mark.parametrize("name,asset_class", [
    ("21Shares Algorand ETP", "crypto"),      # ALGO-USD — via asset_class
    ("21Shares Avalanche ETP", "crypto"),
    ("21Shares Cardano ETP", "crypto"),
    ("21Shares Chainlink ETP", "crypto"),
    ("21Shares Polkadot ETP", "crypto"),
    ("21Shares Stellar ETP", "crypto"),
    ("21Shares Tezos ETP", "crypto"),
    ("21Shares Binance BNB ETP", "etf"),      # ABNB.SW — via name (etf class)
    ("21Shares Hyperliquid ETP", "etf"),      # HYPE.SW — via name
    ("Grayscale Digital Large Cap Fund", "etf"),
])
def test_crypto_etps(name, asset_class):
    assert classify_sector(name, asset_class)["sector"] == "Crypto"


@pytest.mark.parametrize("name,sector", [
    ("WisdomTree Agriculture", "Commodity"),
    ("WisdomTree Coffee", "Commodity"),
    ("WisdomTree Nickel", "Commodity"),
    ("WisdomTree Industrial Metals", "Commodity"),
    ("db Physical Rhodium ETC", "Commodity"),
    ("VanEck Gold Miners ETF", "Materials"),          # miners = equity, NOT the metal
    ("WisdomTree Gold 1x Daily Short ETP Securities", "Short Commodity"),
    ("WisdomTree Natural Gas 3x Daily Short", "Short Commodity"),
    ("WisdomTree Silver 3x Daily Leveraged", "Commodity"),  # long leveraged
    ("Invesco Bloomberg Enhanced Fallen Angels ETF", "Bonds"),
    ("Realstone Swiss Property Fund", "Real Estate"),
    ("La Foncière", "Real Estate"),
    ("21Shares Crypto Basket 10 Core ETP", "Crypto"),
])
def test_classify_sector_hard_cases(name, sector):
    assert classify_sector(name, "equity")["sector"] == sector


def test_short_still_prefixed_via_classify_sector():
    r = classify_sector("ProShares UltraPro Short QQQ", "etf")
    assert r == {"sector": "Short Equity", "category": "Equity", "multiplier": 3, "is_short": True}


@pytest.mark.parametrize("name,sector", [
    ("Brown & Brown Inc.", "Financials"),
    ("RWE AG", "Utilities"),
    ("LKQ Corp. R", "Consumer Cyclical"),
    ("XTB S.A.", "Financials"),
    ("Amedeo Air Four Plus Limited", "Industrials"),
    ("Fair Oaks Income Limited", "Bonds"),
    ("MedNation AG", "Healthcare"),
    ("PROCIMMO ANR", "Real Estate"),
    ("Solvalor 61", "Real Estate"),
    ("Wisdomtree Issuer Icav - Wisdomtree Space Economy Ucits Etf", "Industrials"),
    ("Roundhill ETF Trust - Roundhill Meme Stock ETF", "Equity"),
    ("Lazard Active ETF Trust - Lazard International Dynamic Equity ETF", "Equity"),
])
def test_known_sector(name, sector):
    assert known_sector(name) == sector


def test_known_sector_none():
    assert known_sector("Apple Inc.") is None
    assert known_sector(None) is None


def test_html_entities_in_name():
    # Names stored with literal HTML entities must still match.
    assert known_sector("Brown &amp; Brown Inc.") == "Financials"
    assert classify_sector("Direxion Daily S&amp;P Oil &amp; Gas Bear 2X", "etf")["sector"] == "Short Energy"


def test_normalize_sector():
    assert normalize_sector("Financial Services") == "Financials"
    assert normalize_sector("Basic Materials") == "Materials"
    assert normalize_sector("Technology") == "Technology"        # identity
    assert normalize_sector("Consumer Cyclical") == "Consumer Cyclical"
    assert normalize_sector(None) is None


def test_is_fallback_sector():
    assert is_fallback_sector("etf", "etf") is True
    assert is_fallback_sector("commodity", "commodity") is True
    assert is_fallback_sector(None, "etf") is True
    assert is_fallback_sector("Technology", "equity") is False   # real Yahoo sector preserved
    assert is_fallback_sector("Short Commodity", "etf") is False  # already tagged preserved

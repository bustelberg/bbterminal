"""Geography derivation: exchange/symbol -> country -> continent + MSCI region.

The listing maps are pure data, and a silent gap in them classifies a real
listing as "unknown" — so every exchange value present in `asset_execution`
today is pinned here as a regression case, the same way test_exchange_map pins
the iShares<->GuruFocus resolution.
"""
from __future__ import annotations

import pytest

from asset_pipeline import geo

# Every distinct `asset_execution.exchange` value in the DB as of 2026-07-10.
# A new venue landing here with no mapping should fail this test, not silently
# produce a NULL country.
LIVE_EXCHANGES = {
    "ASX": "Australia", "Amsterdam": "Netherlands", "Athens": "Greece",
    "BSE": "India", "Brussels": "Belgium", "Canadian Sec": "Canada",
    "Cboe AU": "Australia", "Cboe CA": "Canada", "Cboe Europe": "Netherlands",
    "Cboe UK": "United Kingdom", "Cboe US": "United States",
    "Copenhagen": "Denmark", "Dusseldorf": "Germany", "Frankfurt": "Germany",
    "HKSE": "Hong Kong", "Hamburg": "Germany", "Hanover": "Germany",
    "Helsinki": "Finland", "IOB": "United Kingdom", "Irish": "Ireland",
    "Jakarta": "Indonesia", "KSE": "South Korea", "LSE": "United Kingdom",
    "Lisbon": "Portugal", "MCE": "Spain", "Mexico": "Mexico", "Milan": "Italy",
    "Munich": "Germany", "NSE": "India", "NYSE": "United States",
    "NYSE American": "United States", "NYSEArca": "United States",
    "NZSE": "New Zealand", "NasdaqCM": "United States",
    "NasdaqGM": "United States", "NasdaqGS": "United States",
    "OTC Markets OTCID": "United States", "OTC Markets OTCPK": "United States",
    "OTC Markets OTCQB": "United States", "OTC Markets OTCQX": "United States",
    "Oslo": "Norway", "Paris": "France", "Prague": "Czech Republic",
    "SES": "Singapore", "Shanghai": "China", "Shenzhen": "China",
    "Stockholm": "Sweden", "Stuttgart": "Germany", "Swiss": "Switzerland",
    "São Paulo": "Brazil", "TSXV": "Canada", "Taipei Exchange": "Taiwan",
    "Taiwan": "Taiwan", "Tokyo": "Japan", "Toronto": "Canada",
    "Vienna": "Austria", "Warsaw": "Poland", "XETRA": "Germany",
}


class TestCountryFromExchange:
    @pytest.mark.parametrize(("exchange", "country"), sorted(LIVE_EXCHANGES.items()))
    def test_every_live_exchange_resolves(self, exchange: str, country: str) -> None:
        assert geo.country_from_exchange(exchange) == country

    def test_accents_are_folded(self) -> None:
        # The DB stores "São Paulo"; the map key is ASCII.
        assert geo.country_from_exchange("São Paulo") == "Brazil"
        assert geo.country_from_exchange("Sao Paulo") == "Brazil"

    def test_case_insensitive(self) -> None:
        assert geo.country_from_exchange("nyse") == "United States"
        assert geo.country_from_exchange("  XETRA ") == "Germany"

    def test_unknown_and_empty(self) -> None:
        assert geo.country_from_exchange("Ulaanbaatar") is None
        assert geo.country_from_exchange(None) is None
        assert geo.country_from_exchange("") is None


class TestCountryFromSymbol:
    @pytest.mark.parametrize(("symbol", "country"), [
        ("AAPL", "United States"),        # bare ticker = US listing
        ("BRK-B", "United States"),       # US share class, not a crypto pair
        ("PUM.DE", "Germany"),
        ("GSK.L", "United Kingdom"),
        ("7203.T", "Japan"),
        ("0700.HK", "Hong Kong"),
        ("RY.TO", "Canada"),
        ("NESN.SW", "Switzerland"),
        ("TSM.TW", "Taiwan"),
        ("005930.KS", "South Korea"),
    ])
    def test_equity_suffixes(self, symbol: str, country: str) -> None:
        assert geo.country_from_symbol(symbol, "equity") == country

    @pytest.mark.parametrize(("symbol", "venue_country"), [
        ("SMSN.IL", "United Kingdom"),    # Samsung GDR on the LSE's IOB
        ("RIGD.IL", "United Kingdom"),    # Reliance GDR
        ("ATCOBS.XC", "United Kingdom"),  # Atlas Copco (Swedish) on Cboe UK, in SEK
        ("EBPW.XD", "Netherlands"),       # Cboe Europe, in PLN
        ("ERA.XA", "Australia"),          # Cboe Australia
    ])
    def test_cross_listing_venues_give_the_venue_not_the_issuer(
        self, symbol: str, venue_country: str,
    ) -> None:
        # These MTF/GDR lines list foreign issuers, so listing country is the
        # VENUE. Recovering Sweden/South Korea is the domicile backfill's job.
        assert geo.country_from_symbol(symbol, "equity") == venue_country

    @pytest.mark.parametrize("symbol", ["BTC-USD", "ETH-USD", "GC=F", "EURUSD=X", "^GSPC"])
    def test_non_geographic_symbols_have_no_country(self, symbol: str) -> None:
        # These are suffix-less and would otherwise read as US listings.
        assert geo.country_from_symbol(symbol, "crypto") is None
        assert geo.country_from_symbol(symbol, None) is None

    def test_non_geo_asset_class_short_circuits(self) -> None:
        # Even a plausible-looking symbol gets no country when it's not tradeable
        # equity/etf geography.
        assert geo.country_from_symbol("GLD", "commodity") is None
        assert geo.country_from_symbol("GLD", "etf") == "United States"

    def test_unknown_suffix(self) -> None:
        assert geo.country_from_symbol("FOO.ZZZ", "equity") is None


class TestNormalizeCountry:
    @pytest.mark.parametrize(("raw", "canonical"), [
        ("United States", "United States"),
        ("USA", "United States"),
        ("united states of america", "United States"),
        ("UK", "United Kingdom"),
        ("Czechia", "Czech Republic"),          # Yahoo says "Czech Republic"
        ("Korea, Republic of", "South Korea"),
        ("Hong Kong SAR, China", "Hong Kong"),
        ("Taiwan, Province Of China", "Taiwan"),
    ])
    def test_aliases(self, raw: str, canonical: str) -> None:
        assert geo.normalize_country(raw) == canonical

    def test_unknown_country_is_preserved_not_dropped(self) -> None:
        # An unmapped-but-real country must stay visible so the backfill can
        # report it, rather than vanish into None.
        assert geo.normalize_country("Andorra") == "Andorra"

    def test_empty(self) -> None:
        assert geo.normalize_country(None) is None
        assert geo.normalize_country("   ") is None


class TestContinentAndRegionDiverge:
    """continent is geographic; msci_region is financial. They must not agree."""

    def test_israel_is_asia_but_msci_europe(self) -> None:
        assert geo.continent_of("Israel") == "Asia"
        assert geo.msci_region_of("Israel") == "Europe"

    def test_turkey_is_asia_and_emerging(self) -> None:
        assert geo.continent_of("Turkey") == "Asia"
        assert geo.msci_region_of("Turkey") == "Emerging Markets"

    def test_south_korea_and_taiwan_are_emerging(self) -> None:
        for c in ("South Korea", "Taiwan"):
            assert geo.continent_of(c) == "Asia"
            assert geo.msci_region_of(c) == "Emerging Markets"

    def test_japan_and_australia_are_developed_pacific(self) -> None:
        assert geo.msci_region_of("Japan") == "Pacific"
        assert geo.msci_region_of("Australia") == "Pacific"
        assert geo.continent_of("Australia") == "Oceania"

    def test_greece_is_europe_but_emerging(self) -> None:
        assert geo.continent_of("Greece") == "Europe"
        assert geo.msci_region_of("Greece") == "Emerging Markets"

    def test_mexico_is_north_america_but_emerging(self) -> None:
        assert geo.continent_of("Mexico") == "North America"
        assert geo.msci_region_of("Mexico") == "Emerging Markets"

    def test_country_with_no_msci_market(self) -> None:
        assert geo.continent_of("Ukraine") == "Europe"
        assert geo.msci_region_of("Ukraine") is None


class TestResolveGeo:
    def test_domicile_wins_over_listing(self) -> None:
        # Linde: NYSE-listed, UK-domiciled.
        g = geo.resolve_geo("United Kingdom", "United States")
        assert g == {"country": "United Kingdom", "continent": "Europe",
                     "msci_region": "Europe"}

    def test_adr_domiciles_abroad(self) -> None:
        # Alibaba ADR: US listing, Chinese issuer.
        g = geo.resolve_geo("China", "United States")
        assert g == {"country": "China", "continent": "Asia",
                     "msci_region": "Emerging Markets"}

    def test_tencent_lists_hk_domiciles_china(self) -> None:
        g = geo.resolve_geo("China", "Hong Kong")
        assert g["country"] == "China"
        assert g["msci_region"] == "Emerging Markets"   # not Pacific (HK)

    def test_listing_only_when_no_domicile(self) -> None:
        # An ETF has no assetProfile, so only the listing is known.
        g = geo.resolve_geo(None, "United States")
        assert g == {"country": "United States", "continent": "North America",
                     "msci_region": "North America"}

    def test_offshore_domicile_falls_back_to_listing_for_region(self) -> None:
        # Cyprus is a real HQ country (continent Europe) but not an MSCI market,
        # so the region falls back to the Athens listing -> Emerging Markets.
        g = geo.resolve_geo("Cyprus", "Greece")
        assert g["country"] == "Cyprus"
        assert g["continent"] == "Europe"
        assert g["msci_region"] == "Emerging Markets"

    def test_unmapped_domicile_still_takes_listing_continent(self) -> None:
        g = geo.resolve_geo("Andorra", "Spain")
        assert g["country"] == "Andorra"        # kept, visible to the backfill
        assert g["continent"] == "Europe"       # from Spain
        assert g["msci_region"] == "Europe"

    def test_nothing_known(self) -> None:
        assert geo.resolve_geo(None, None) == {
            "country": None, "continent": None, "msci_region": None}

    def test_aliases_resolve_through(self) -> None:
        g = geo.resolve_geo("USA", None)
        assert g["country"] == "United States"
        assert g["msci_region"] == "North America"


class TestMapIntegrity:
    def test_every_msci_country_has_a_continent(self) -> None:
        missing = sorted(set(geo.MSCI_REGION) - set(geo.CONTINENT))
        assert not missing, f"in MSCI_REGION but no continent: {missing}"

    def test_every_exchange_country_has_a_continent(self) -> None:
        missing = sorted(set(geo.EXCHANGE_COUNTRY.values()) - set(geo.CONTINENT))
        assert not missing, f"exchange maps to a country with no continent: {missing}"

    def test_every_suffix_country_has_a_continent(self) -> None:
        missing = sorted(set(geo.SUFFIX_COUNTRY.values()) - set(geo.CONTINENT))
        assert not missing, f"suffix maps to a country with no continent: {missing}"

    def test_no_country_in_two_continents(self) -> None:
        seen: dict[str, str] = {}
        for cont, countries in geo._CONTINENT_GROUPS.items():
            for c in countries:
                assert c not in seen, f"{c} in both {seen[c]} and {cont}"
                seen[c] = cont

    def test_aliases_point_at_canonical_names(self) -> None:
        # Every alias target must itself be a mapped country, else normalization
        # produces a name the continent map can't resolve.
        for target in geo._COUNTRY_ALIASES.values():
            assert target in geo.CONTINENT, f"alias target {target!r} has no continent"

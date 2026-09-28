from __future__ import annotations

import html
import json

import pytest

from routers import _etf_sector_allocation as sector_allocation
from routers._etf_sector_allocation import (
    MOMENTUM_ISIN,
    parse_ishares_sector_payload,
    parse_justetf_sector_payload,
    supported,
)


def _page(*, names=None, weights=None, as_of=20260925) -> str:
    payload = {
        "name": "sector",
        "fullName": "exposureBreakdowns.sector",
        "dataPointsByNameMap": {
            "asOf": {"value": as_of},
            "fund": {"value": weights or [60.0, 40.0]},
            "type": {"value": names or ["Information Technology", "Financials"]},
        },
    }
    embedded = f'"sector":{json.dumps(payload, separators=(",", ":"))}'
    return f'<div data-product="{html.escape(embedded, quote=True)}"></div>'


def test_registry_includes_verified_etfs_but_not_internal_or_mutual_funds():
    assert supported(MOMENTUM_ISIN)
    assert supported(MOMENTUM_ISIN.lower())
    assert supported("IE00B6R52259")  # iShares ACWI, via justETF
    assert supported("US46138E3392")  # Invesco SPMO, via Yahoo
    assert supported("IE000PS0J481")  # Global X BRIJ, verified ETF via Yahoo
    assert supported("IE000LCKJ888")  # WisdomTree WPAI, verified ETF via Yahoo
    assert not supported("CH1593776334")  # our own StarTopSelectie certificate
    assert not supported("IE000MEQP5U8")  # mutual fund, not an ETF
    assert not supported("XS2427355958")  # exchange-traded product, not an ETF


def test_physical_ishares_are_issuer_first_but_synthetic_exposure_stays_on_justetf():
    physical = {
        "IE00BP3QZ825", "IE00B4K48X80", "IE00B6R52259", "IE00BP3QZ601",
        "IE00BF4RFH31", "IE000R4ZNTN3", "IE00BZ0PKT83",
    }
    assert all(sector_allocation._PRODUCTS[isin]["provider"] == "ishares"
               for isin in physical)
    assert all(sector_allocation._PRODUCTS[isin]["fallback"] == "justetf"
               for isin in physical)
    assert sector_allocation._PRODUCTS["IE0001ZFMLN7"]["provider"] == "justetf"


def test_physical_ishares_falls_back_to_justetf_when_the_issuer_page_fails(monkeypatch):
    isin = "IE00B6R52259"
    sentinel = object()
    sector_allocation._CACHE.pop(isin, None)
    monkeypatch.setattr(
        sector_allocation, "_fetch_ishares",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("changed page")),
    )
    monkeypatch.setattr(sector_allocation, "_fetch_justetf", lambda value: sentinel)

    assert sector_allocation.fetch_sector_allocation(isin) is sentinel
    sector_allocation._CACHE.pop(isin, None)


def test_parses_the_server_rendered_ishares_sector_payload():
    result = parse_ishares_sector_payload(
        _page(
            names=["Technology", "Communication", "Cash and/or Derivatives"],
            weights=[60.0, 39.5, 0.5],
        ),
        isin=MOMENTUM_ISIN,
    )

    assert result.isin == MOMENTUM_ISIN
    assert result.as_of == "2026-09-25"
    assert result.source == "iShares"
    assert [(row.sector, row.weight_pct) for row in result.sectors] == [
        ("Information Technology", 60.0),
        ("Communication Services", 39.5),
        ("Cash", 0.5),
    ]
    assert [row.provider_sectors for row in result.sectors] == [
        ["Technology"],
        ["Communication"],
        ["Cash and/or Derivatives"],
    ]
    assert [[(source.sector, source.weight_pct) for source in row.provider_weights]
            for row in result.sectors] == [
        [("Technology", 60.0)],
        [("Communication", 39.5)],
        [("Cash and/or Derivatives", 0.5)],
    ]


def test_refuses_a_partial_or_changed_vendor_payload():
    with pytest.raises(ValueError, match="total"):
        parse_ishares_sector_payload(
            _page(weights=[10.0, 20.0]), isin=MOMENTUM_ISIN)

    with pytest.raises(ValueError, match="sector allocation payload"):
        parse_ishares_sector_payload("<html>changed</html>", isin=MOMENTUM_ISIN)


def test_parses_justetf_expanded_sector_table_and_reference_date():
    page = '<span data-testid="tl_etf-holdings_reference-date">As of 31/08/2026</span>'
    expanded = """
      <td data-testid="tl_etf-holdings_sectors_value_name">Technology</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">40.25%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Finance</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">20,00%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Consumer Cyclicals</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">20,00%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Consumer Services</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">19,75%</td>
    """

    result = parse_justetf_sector_payload(page, expanded, isin="IE00B6R52259")

    assert result.as_of == "2026-08-31"
    assert result.source == "justETF"
    assert [(row.sector, row.weight_pct) for row in result.sectors] == [
        ("Information Technology", 40.25),
        ("Financials", 20.0),
        ("Consumer Discretionary", 39.75),
    ]
    assert result.sectors[-1].provider_sectors == [
        "Consumer Cyclicals", "Consumer Services",
    ]
    assert [(source.sector, source.weight_pct)
            for source in result.sectors[-1].provider_weights] == [
        ("Consumer Cyclicals", 20.0),
        ("Consumer Services", 19.75),
    ]


def test_justetf_uses_complete_inline_table_when_there_is_no_show_more(monkeypatch):
    page = """
      <span data-testid="tl_etf-holdings_reference-date">As of 31/08/2026</span>
      <td data-testid="tl_etf-holdings_sectors_value_name">Utilities</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">97.09%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Energy</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">1.61%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Business Services</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">1.02%</td>
      <td data-testid="tl_etf-holdings_sectors_value_name">Industrials</td>
      <td data-testid="tl_etf-holdings_sectors_value_percentage">0.28%</td>
    """

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    calls = []

    def tracked_get(_session, url, *, source):
        calls.append((url, source))
        return type("Response", (), {"text": page})()

    monkeypatch.setattr(sector_allocation.requests, "Session", Session)
    monkeypatch.setattr(sector_allocation, "_tracked_get", tracked_get)

    result = sector_allocation._fetch_justetf("IE00BM67HQ30")

    assert len(calls) == 1
    assert [(row.sector, row.weight_pct) for row in result.sectors] == [
        ("Utilities", 97.09),
        ("Energy", 1.61),
        ("Industrials", 1.3),
    ]
    assert result.sectors[-1].provider_sectors == ["Business Services", "Industrials"]

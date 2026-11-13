"""Issuer overlap can fold share classes; listing resolution must retain them."""
import pytest

from index_universe.acwi.identity import issuer_key, share_class
from routers._airs_portfolio_analysis import _mark_acwi_sector_overlap


@pytest.mark.parametrize('held,benchmark,letter', [
    ('Berkshire Hathaway - B', 'BERKSHIRE HATHAWAY INC CLASS B', 'b'),
    ('Alphabet - C', 'ALPHABET INC CLASS C', 'c'),
    ('MSCI Inc. A', 'MSCI INC CLASS A', 'a'),
    ('Investor AB - B', 'INVESTOR AB (publ)', 'b'),
    ('Berkshire Hathaway – B', 'Berkshire Hathaway Inc.', 'b'),
    ('Berkshire Hathaway — B', 'Berkshire Hathaway Inc.', 'b'),
    ('Example Inc Cl.A', 'Example Inc Class A', 'a'),
    ('Example Inc CLA', 'Example Inc Class A', 'a'),
    ('Example Inc Shares C', 'Example Inc Class C', 'c'),
])
def test_explicit_share_class_variants_share_issuer_but_retain_class(held, benchmark, letter):
    assert issuer_key(held) == issuer_key(benchmark)
    assert share_class(held) == letter
    # No shared ISIN: this exercises the issuer fallback used by all three popups.
    portfolio = {'North America': [{'name': held, 'isin': 'PORTFOLIO_LISTING'}]}
    index = {'North America': [{'name': benchmark, 'isin': None}]}
    _mark_acwi_sector_overlap(portfolio, index, '2026-11-11')
    assert portfolio['North America'][0]['in_both'] is True
    assert index['North America'][0]['in_both'] is True
    assert portfolio['North America'][0]['issuer_key'] == index['North America'][0]['issuer_key']


@pytest.mark.parametrize('left,right', [
    ('Brookfield Corporation', 'Brookfield Asset Management'),
    ('Example B', 'Example'),
    ('Example A', 'Example'),
    ('Company C', 'Company'),
    ('Berkshire Hathaway Energy', 'Berkshire Hathaway Inc.'),
])
def test_word_overlap_or_bare_letters_do_not_establish_issuer_identity(left, right):
    assert issuer_key(left) != issuer_key(right)


def test_same_issuer_keeps_different_security_classes_distinct():
    assert issuer_key('Berkshire Hathaway - A') == issuer_key('Berkshire Hathaway - B')
    assert share_class('Berkshire Hathaway - A') == 'a'
    assert share_class('Berkshire Hathaway - B') == 'b'


"""The search box on /research-dashboard builds a PostgREST filter out of what someone typed.

`or=(isin.ilike.*x*,name.ilike.*x*,yahoo_symbol.ilike.*x*)` is a COMMA-SEPARATED list wrapped in
parentheses. A comma or a parenthesis inside the term therefore does not search for that character
— it ends one filter and begins another. That is the whole reason this function exists, and the
reason it is pure: being wrong here is a security question, not a slow query.
"""
from __future__ import annotations

import pytest

from routers.asset_pipeline import ilike_pattern


class TestTheSeparatorsCannotSurvive:
    """⚠ DROPPED, NOT ESCAPED. PostgREST has no escape for these inside `or=`."""

    @pytest.mark.parametrize("term", [
        "a,bars.gt.0",                 # a second filter smuggled in behind a comma
        "x)",                          # closing the or-list early
        "(x",
        "a,b),(c",
        "*",                           # PostgREST's own wildcard — would widen, not match
        "**apple**",
    ])
    def test_no_separator_reaches_the_filter(self, term):
        out = ilike_pattern(term)
        if out is None:
            return                                  # nothing left to search for is a fine answer
        # The only `*` allowed are the two this function puts there itself.
        assert out.startswith("*") and out.endswith("*")
        assert "," not in out
        assert "(" not in out and ")" not in out
        assert "*" not in out[1:-1]

    def test_a_term_that_is_only_separators_searches_for_nothing(self):
        # ⚠ None, NOT `"**"`. An empty pattern matches EVERY row, so a user typing `,,,` would be
        # served the first 25 instruments in the pipeline as though they were results.
        for term in [",", "()", "*", ",,,", "(,)"]:
            assert ilike_pattern(term) is None


class TestOrdinaryTermsSurviveIntact:
    @pytest.mark.parametrize(("term", "expect"), [
        ("apple", "*apple*"),
        ("  ASML  ", "*ASML*"),                     # trimmed, case preserved (ilike is insensitive)
        ("NL0010273215", "*NL0010273215*"),
        ("Berkshire Hathaway", "*Berkshire Hathaway*"),   # spaces are not separators
        ("AT&T", "*AT&T*"),                          # `&` is not a PostgREST or-separator
        ("Danaher-B", "*Danaher-B*"),
        ("BRK.B", "*BRK.B*"),                        # a dot is part of a symbol, not a filter here
    ])
    def test_kept(self, term, expect):
        assert ilike_pattern(term) == expect

    def test_empty_and_none_are_answered_not_raised(self):
        assert ilike_pattern("") is None
        assert ilike_pattern("   ") is None
        assert ilike_pattern(None) is None          # type: ignore[arg-type]

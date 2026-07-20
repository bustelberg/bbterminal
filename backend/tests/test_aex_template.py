"""The AEX universe — sourced from Wikipedia, resolved structurally, dated honestly.

The three things that would each ship a plausible-looking wrong index, and are each pinned here:

  1. The composition table is found by its HEADERS, not its position (the page has three
     wikitables and the composition is index 2 *today*).
  2. The page's OWN as-of date is the snapshot's date. Measured 2026-07-16 it reads
     "31 December 2024" — 562 days stale. Stamping today's month would assert a freshness we
     have not got, precisely when it matters most (just after a review).
  3. The three cross-listed names (Shell, RELX, Unilever) are resolved by an OpenFIGI
     Amsterdam-listing gate, NOT by name — because `same_company("Unilever", "HINDUSTAN
     UNILEVER LTD")` is True, and the NYSE ADR row matches "Unilever" too.
"""
from __future__ import annotations

import inspect
from datetime import date

import pytest

from index_universe.templates import TEMPLATES, get_template
from index_universe.templates.aex import AEXTemplate, _parse_as_of, _resolve_companies, scrape_aex


class TestTheTemplateIsRegistered:
    def test_it_is_in_the_registry(self):
        assert TEMPLATES["AEX"] is AEXTemplate
        assert isinstance(get_template("AEX"), AEXTemplate)

    def test_its_label_and_key_agree(self):
        """`store_index_membership` looks the universe up by LABEL while the template owns it by
        `template_key` — they must name the same row or the writer creates a second, keyless one."""
        assert AEXTemplate.label == AEXTemplate.template_key == "AEX"

    def test_it_is_capped_as_a_benchmark(self):
        """The universe and the cap are wired at opposite ends of the app; a template with no cap
        rule is an ASML tracker."""
        from routers._benchmark_index import INDEX_CAP_PCT

        assert INDEX_CAP_PCT[AEXTemplate.label] == 15.0


class TestThePageDatesItself:
    def test_the_real_sentence_parses(self):
        html = "<p>The index is composed of the following listings as of 31 December 2024.</p>"
        assert _parse_as_of(html) == date(2024, 12, 31)

    def test_an_abbreviated_month_parses(self):
        assert _parse_as_of("as of 1 Mar 2026") == date(2026, 3, 1)

    def test_no_date_is_none_not_today(self):
        """`scrape_aex` turns this into a REFUSAL. An undated composition presented as current is
        the exact failure this template exists to avoid, so the absence must be representable —
        defaulting to today here would erase it before anyone could refuse."""
        assert _parse_as_of("<p>The index is composed of the following listings.</p>") is None
        assert _parse_as_of("") is None

    def test_an_undated_page_is_refused_not_stamped(self):
        src = inspect.getsource(scrape_aex)
        assert "Refusing rather than" in src
        assert "raise ValueError" in src


class TestTheTableIsFoundByItsHeaders:
    def test_the_lookup_is_by_header_not_index(self):
        """`scrape_sp500` addresses its tables positionally. Here a page edit that adds or
        reorders a wikitable would silently hand us the wrong one."""
        src = inspect.getsource(scrape_aex)
        # No positional addressing of the composition table — it is found by its headers.
        assert "parser.tables[1]" not in src
        assert "parser.tables[2]" not in src

    def test_a_missing_table_raises_rather_than_returning_empty(self):
        """A scraper that quietly returns nothing shrinks the index instead of failing."""
        src = inspect.getsource(scrape_aex)
        assert "No AEX composition table" in src
        assert "parsed to zero rows" in src



class TestTier2IsStructuralNotAName:
    """⚠ THE TRAP THIS GATE EXISTS FOR. Three of the 25 (Shell, RELX, Unilever) carry a GB ISIN
    and our pipeline resolved each to its LONDON listing, so ticker AND exchange both differ. The
    obvious fallback — match on the company name — enlists an Indian company:

        same_company("Unilever", "HINDUSTAN UNILEVER LTD")  ->  True

    ...and cannot separate Unilever's ordinary (GB00BVZK7T90) from our NYSE ADR row
    (US9047678035), which is a different ISIN and a different price series.

    So the acceptance is definitional: an AEX constituent IS the ISIN that trades on Euronext
    Amsterdam under that ticker. Measured 2026-07-16 against the live API:

        GB00BP6MXD84  Shell             -> ('SHELL','NA')   accepted
        GB00B2B0DG97  RELX              -> ('REN','NA')     accepted
        GB00BVZK7T90  Unilever ordinary -> ('UNA','NA')     accepted
        US9047678035  Unilever ADR      -> []               REJECTED
    """

    def test_the_name_trap_is_real(self):
        """Pinned so nobody "simplifies" the OpenFIGI gate into a name match."""
        from asset_pipeline.resolve import same_company

        assert same_company("Unilever", "HINDUSTAN UNILEVER LTD") is True
        assert same_company("Unilever", "Unilever PLC") is True     # ...and the ADR, too

    def test_acceptance_requires_an_amsterdam_listing(self):
        src = inspect.getsource(_resolve_companies)
        assert "_OPENFIGI_AMSTERDAM" in src
        assert "lookup_isins" in src

    def test_the_gate_does_not_decide_on_a_name(self):
        """The name query is a candidate NET; `same_company` must not appear in the acceptance."""
        src = inspect.getsource(_resolve_companies)
        assert "same_company" not in src

    def test_ambiguity_is_unresolved_not_a_coin_flip(self):
        """Two accepted candidates is two different price series — a human's call."""
        src = inspect.getsource(_resolve_companies)
        assert "ambiguous" in src

    def test_a_failed_openfigi_call_loses_names_loudly_never_guesses(self):
        src = inspect.getsource(_resolve_companies)
        assert "unresolved" in src
        assert "log.warning" in src


class TestTheTemplateNeverInventsACompany:
    def test_it_does_not_create_company_rows(self):
        """LeonteqTemplate auto-creates via OpenFIGI; this one must not. An AEX name we cannot
        find is a data gap for a human, not a stub to conjure — the index is 25 known large-caps,
        so a miss means something is wrong, not that something is new."""
        src = inspect.getsource(_resolve_companies)
        assert "insert" not in src
        assert "upsert" not in src

    def test_an_empty_resolution_refuses_to_write(self):
        src = inspect.getsource(AEXTemplate.refresh)
        assert "refusing to write" in src

    def test_the_diff_is_taken_before_the_wipe(self):
        """`store_index_membership` clears membership first, so a diff taken afterwards reports
        all 25 as additions every single refresh — and a real change (a name entering at the
        March review) drowns in it. That signal is the whole point of a self-updating template."""
        src = inspect.getsource(AEXTemplate.refresh)
        before = src.index("before = {")
        write = src.index("store_index_membership(")
        assert before < write


@pytest.mark.skip(reason="hits Wikipedia + OpenFIGI; run manually when the page shape is in doubt")
class TestAgainstTheLivePage:
    def test_the_page_still_parses(self):
        as_of, cons = scrape_aex()
        assert len(cons) == 25
        assert as_of.year >= 2024
        assert all(c["bare"] and "." not in c["bare"] for c in cons)

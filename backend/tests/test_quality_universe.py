from index_universe.quality import load_quality_candidates
from index_universe.templates.quality import _manual_yahoo_overrides


def test_quality_source_union_excludes_cash_and_fx_rows():
    candidates, as_of = load_quality_candidates()
    by_ticker = {candidate.ticker: candidate for candidate in candidates}

    assert as_of.isoformat() == "2026-09-30"
    assert len(candidates) == 547
    assert "EUR" not in by_ticker
    assert by_ticker["NVDA"].sources == {"compounders", "ishares_quality"}
    assert by_ticker["CSU"].sources == {"compounders"}


def test_reviewed_yahoo_overrides_replace_vendor_identifiers():
    overrides = _manual_yahoo_overrides()

    assert overrides["2299955D"] == "CSU.TO"
    assert overrides["CSU"] == "CSU.TO"
    assert overrides["VMRK"] == "VMRK"

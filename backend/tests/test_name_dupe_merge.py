"""`ingest.dedupe.merge_name_dupes_keep_isin` — folds a NO-ISIN stub into a
same-name company that HAS an ISIN (the gap dedupe_by_isin + the
canonical_name merge both miss). Tested in `dry_run` so we assert the group
SELECTION + safety rules (exactly-one-ISIN) without the FK-rewire machinery.
Plus pure tests for the suffix-stripping `_name_dupe_key`.
"""
from __future__ import annotations

from ingest.dedupe import _name_dupe_key, merge_name_dupes_keep_isin
from tests._fake_supabase import FakeSupabase


def _co(cid, name, isin, ticker="X", exch="NYSE"):
    return {
        "company_id": cid,
        "company_name": name,
        "isin": isin,
        "gurufocus_ticker": ticker,
        "gurufocus_exchange": {"exchange_code": exch},
    }


class TestNameDupeKey:
    def test_strips_corporate_suffixes(self):
        assert _name_dupe_key("Celestica Inc") == "celestica"
        assert _name_dupe_key("Celestica") == "celestica"
        assert _name_dupe_key("FISERV INC") == "fiserv"
        assert _name_dupe_key("HDFC Bank Ltd") == "hdfc bank"

    def test_keeps_meaningful_words(self):
        assert _name_dupe_key("BYD Co Ltd") == "byd"
        assert _name_dupe_key("BYD Electronic") == "byd electronic"  # stays distinct


class TestMergeSelection:
    def _fake(self):
        return FakeSupabase(tables={"company": [
            # foldable: one ISIN row + one no-ISIN stub, same suffix-stripped name
            _co(1, "Celestica", None, "CLA", "TSX"),
            _co(2, "Celestica Inc", "CA15101Q2071", "CLS", "TSX"),
            _co(3, "HDFC BANK LTD", None, "HDFCBANK", "NSE"),
            _co(4, "HDFC Bank Ltd", "US40415F1012", "HDB", "NYSE"),
            # SAFETY: two ISIN rows (share classes) — must be SKIPPED
            _co(5, "Alphabet Inc", "US02079K3059", "GOOGL"),
            _co(6, "Alphabet Inc", "US02079K1079", "GOOG"),
            # SAFETY: no ISIN anywhere — no canonical to keep, SKIPPED
            _co(7, "Acme", None, "ACM1"),
            _co(8, "Acme Corp", None, "ACM2"),
            # distinct names (only a suffix differs from a real word) — not grouped
            _co(9, "BYD Electronic", "CNE100000F46", "0285", "HKSE"),
            # SAFETY: different suffix = different entity. "Siemens Ltd" (India,
            # no ISIN) must NOT fold into "Siemens AG" (German parent, ISIN).
            _co(10, "SIEMENS LTD", None, "SIEMENS", "NSE"),
            _co(11, "Siemens AG", "DE0007236101", "SIE", "XTER"),
        ]})

    def test_folds_only_single_isin_groups(self):
        rep = merge_name_dupes_keep_isin(self._fake(), dry_run=True)
        # Celestica + HDFC fold; Alphabet (2 ISINs), Acme (0 ISINs), Siemens
        # (Ltd vs AG — different suffix) all skipped.
        assert rep.groups_merged == 2
        assert rep.rows_deleted == 2
        joined = " ".join(rep.actions).lower()
        assert "celestica" in joined and "hdfc bank" in joined
        assert "alphabet" not in joined  # share classes never fused
        assert "acme" not in joined      # no canonical → skipped
        assert "siemens" not in joined   # India sub ≠ German parent

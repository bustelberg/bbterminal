""""27 report(s) failed" is a count, not a diagnosis.

That message said something was wrong, gave no handle on what, and left the operator to guess.
Thirteen Vermogensoverzicht failures because those books have not been valued yet and fourteen
Model failures because the session expired are the SAME number and completely different problems —
one needs waiting, the other needs a login. Grouping by cause is what makes the difference visible.
"""
from __future__ import annotations

from airs_vermogen import summarise_errors


def err(account, report, etype="RuntimeError", message="boom"):
    return {"account": account, "report": report, "error_type": etype, "message": message}


class TestItGroupsByCause:
    def test_identical_failures_collapse_to_one_line_with_a_count(self):
        out = summarise_errors([
            err("A", "Vermogensoverzicht", message="no valued Vermogensoverzicht in the last 7 days"),
            err("B", "Vermogensoverzicht", message="no valued Vermogensoverzicht in the last 7 days"),
            err("C", "Vermogensoverzicht", message="no valued Vermogensoverzicht in the last 7 days"),
        ])
        assert len(out) == 1
        assert out[0]["count"] == 3
        assert out[0]["report"] == "Vermogensoverzicht"
        assert "no valued" in out[0]["message"]

    def test_different_reports_stay_apart(self):
        out = summarise_errors([err("A", "Model"), err("B", "Mutaties")])
        assert {g["report"] for g in out} == {"Model", "Mutaties"}

    def test_different_exception_types_stay_apart(self):
        """A timeout and a parse failure are not the same problem even on the same report."""
        out = summarise_errors([
            err("A", "Model", etype="TimeoutError"),
            err("B", "Model", etype="ValueError"),
        ])
        assert len(out) == 2

    def test_commonest_cause_leads(self):
        out = summarise_errors([err("A", "Model"), err("B", "Mutaties"), err("C", "Mutaties")])
        assert out[0]["report"] == "Mutaties" and out[0]["count"] == 2

    def test_a_trailing_detail_does_not_scatter_one_cause(self):
        """⚠ THE KEY IS TRUNCATED FOR EXACTLY THIS. Two failures of the same kind usually differ in
        a trailing date or account code; keying on the whole string would produce a dozen groups of
        one — the un-summarised list this exists to replace."""
        base = "no valued Vermogensoverzicht in the last 7 days (Response too small for account "
        out = summarise_errors([
            err("A", "Vermogensoverzicht", message=base + "BUS_A on 2026-07-28)"),
            err("B", "Vermogensoverzicht", message=base + "BUS_B on 2026-07-27)"),
        ])
        assert len(out) == 1 and out[0]["count"] == 2


class TestItNamesSomeAccountsButNotAll:
    def test_a_few_examples_ride_along(self):
        """Enough to go and look at one; a full list would be the thing being summarised."""
        out = summarise_errors([err(f"BUS_{i}", "Model") for i in range(13)])
        assert out[0]["count"] == 13
        assert len(out[0]["accounts"]) == 4
        assert out[0]["accounts"][0] == "BUS_0"

    def test_it_survives_a_malformed_entry(self):
        # Bookkeeping must never be the thing that breaks a status read.
        out = summarise_errors([{}, err("A", "Model")])
        assert sum(g["count"] for g in out) == 2


def test_no_errors_is_an_empty_summary_not_a_placeholder():
    assert summarise_errors([]) == []

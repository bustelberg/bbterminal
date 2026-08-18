"""`fx_rate` is widened at BOTH ends now — it used to grow forward only.

⚠⚠ THE FAILURE THIS CLOSES PRODUCES A NUMBER, NOT A BLANK. `sync_fx_rates_to_db` read the stored
MAX and fetched from max+1, so a currency whose history simply STARTED too late was never repaired
by anything, in any environment, for ever. And the reader hides it: `load_fx_rates` does
`.reindex(daily).ffill().bfill()`, and the BACK-fill extends the earliest stored rate to whatever
`start_date` was asked for. So a backtest window opening before a currency's first stored day
converts that whole stretch at ONE wrong rate — no empty cell, no error, a return wrong by however
much the currency moved beforehand.

⚠ AND THE SHORT-CIRCUIT IS WHY IT WAS UNREACHABLE. The forwards leg returned `cached` from the
FUNCTION the moment the stored max reached `end_date` — which is every currency, most days — so
nothing ever got as far as looking at where the history began. The two legs are independent now,
which is the behaviour these tests exist to hold.

The related trap is one table over and already pinned by `test_fx_paging.py`: an unpaged FX read
truncates silently and drops a currency, which makes a fully-priced holding LEAVE its portfolio.
Same table, same class of consequence — a plausible figure instead of a visible failure.
"""
from __future__ import annotations

from datetime import date

import pytest

from momentum.data import fx as F
from tests._fake_supabase import FakeSupabase

CODE = "USD"


def _rows(dates: list[str]) -> list[dict]:
    return [{"currency_code": CODE, "rate_date": d, "rate": 1.1} for d in dates]


@pytest.fixture
def wired(monkeypatch):
    """A fake table plus a recorder of every `fetch_history(code, start)` the sync makes."""
    fake = FakeSupabase({"fx_rate": []})
    asked: list[tuple[str, str]] = []
    served: dict[str, list[dict]] = {}

    def _fetch(code: str, start: str | None = None):
        asked.append((code, start or ""))
        return served.get(start or "", [])

    monkeypatch.setattr("fx_rates.fetch_history", _fetch, raising=False)
    return fake, asked, served


class TestTheBackwardsLeg:
    def test_a_head_gap_is_filled(self, wired):
        """⚠ THE MEASURED SHAPE: the currencies actually in use (USD, CZK, GBP, JPY, CHF) started
        at 2024-03-07 while ISK/THB/IDR reached back to 2000 — so a 1998 window had almost no EUR
        line for the ones that mattered."""
        fake, asked, served = wired
        fake.tables["fx_rate"] = _rows(["2024-03-07", "2024-03-08"])
        served["2000-01-01"] = [{"date": "2000-01-03", "rate": 1.0},
                                {"date": "2000-01-04", "rate": 1.0}]

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2024, 3, 8))[CODE]

        assert (CODE, "2000-01-01") in asked, "never asked for the head"
        assert st["backfilled_rows"] == 2
        assert st["status"] == "synced"
        assert {r["rate_date"] for r in fake.tables["fx_rate"]} >= {"2000-01-03", "2000-01-04"}

    def test_it_runs_even_when_the_forward_end_is_already_covered(self, wired):
        """⚠⚠ THE ACTUAL BUG. Current-to-today is the normal state, and the old code returned
        `cached` from the function on exactly that condition — before anything looked at the head.
        """
        fake, asked, served = wired
        fake.tables["fx_rate"] = _rows(["2024-03-07", "2026-08-18"])
        served["2000-01-01"] = [{"date": "2000-01-03", "rate": 1.0}]

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2026, 8, 18))[CODE]

        assert st["backfilled_rows"] == 1
        assert (CODE, "2000-01-01") in asked

    def test_it_does_not_run_when_the_head_is_already_covered(self, wired):
        fake, asked, _ = wired
        fake.tables["fx_rate"] = _rows(["1999-12-31", "2026-08-18"])

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2026, 8, 18))[CODE]

        assert st["status"] == "cached"
        assert st["backfilled_rows"] == 0
        assert asked == [], "a covered currency must cost no request at either end"

    def test_an_empty_table_is_the_forward_leg_s_job(self, wired):
        """⚠ ONE REQUEST, NOT TWO. With nothing stored the forwards leg already fetches from
        `start_date`; running the backwards leg too would be the identical call twice."""
        fake, asked, served = wired
        served["2000-01-01"] = [{"date": "2000-01-03", "rate": 1.0}]

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2026, 8, 18))[CODE]

        assert asked == [(CODE, "2000-01-01")]
        assert st["backfilled_rows"] == 0
        assert st["rows"] == 1


class TestOneLegNeverCostsTheOther:
    def test_a_failed_backfill_still_gets_today_s_rate(self, wired, monkeypatch):
        """⚠ A LONG-STANDING HEAD GAP MUST NOT BECOME A FRESH TAIL GAP. The head repair is
        best-effort; the day's rate is not."""
        fake, asked, served = wired
        fake.tables["fx_rate"] = _rows(["2024-03-07"])
        served["2024-03-08"] = [{"date": "2024-03-08", "rate": 1.2}]

        def _fetch(code: str, start: str | None = None):
            asked.append((code, start or ""))
            if start == "2000-01-01":
                raise RuntimeError("ECB said no")
            return served.get(start or "", [])
        monkeypatch.setattr("fx_rates.fetch_history", _fetch, raising=False)

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2024, 3, 8))[CODE]

        assert "backfill failed" in st.get("note", "")
        assert st["rows"] == 1, "the forwards leg was lost with it"
        assert any(r["rate_date"] == "2024-03-08" for r in fake.tables["fx_rate"])

    def test_eur_is_still_skipped_at_both_ends(self, wired):
        fake, asked, _ = wired
        assert F.sync_fx_rates_to_db(fake, ["EUR"], date(2000, 1, 1), date(2026, 8, 18))["EUR"] == {
            "status": "skipped", "rows": 0}
        assert asked == []

    def test_it_reports_the_coverage_it_GOT_not_the_one_it_asked_for(self, wired):
        """⚠ A currency whose published history begins in 2005 does not gain a 2000 start by being
        asked for one. This table's entire failure mode is coverage read as wider than it is."""
        fake, _asked, served = wired
        fake.tables["fx_rate"] = _rows(["2024-03-07"])
        served["2000-01-01"] = [{"date": "2005-06-01", "rate": 1.0}]

        st = F.sync_fx_rates_to_db(fake, [CODE], date(2000, 1, 1), date(2024, 3, 7))[CODE]

        assert st["min_date"] == "2005-06-01"
        assert "2005-06-01" in st["note"]

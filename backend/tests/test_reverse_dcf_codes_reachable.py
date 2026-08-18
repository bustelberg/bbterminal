"""Every metric the Reverse DCF reads must be reachable through the endpoint that feeds it.

⚠⚠ THIS IS THE "ALLOWLIST STARVED THE FEATURE" BUG, AND NOTHING ON SCREEN COULD HAVE SHOWN IT.
`/api/earnings/by-isin/{isin}/metrics` does not return a company's metrics — it returns the ones on
`_DASHBOARD_METRIC_CODES`, an allowlist. Stock compensation, capital expenditure and cash-flow
depreciation were all absent from it, so the FCF normalisation added 2026-08-18 found nothing,
reported (correctly) that the correction could not be applied, and rendered an honest "—" for ASML
— a company that files all three, with 39,349 rows of them sitting in `metric_data`.

Every layer behaved exactly as written. The frontend asked for codes it never received, the
backend answered the question it was asked, and the panel told the truth about what it had. The
data stopped at the door, and the only symptom was a dash that looks identical to a company that
genuinely reports nothing.

⚠ THE ASSERTION IS "AT LEAST ONE SPELLING", NOT "ALL OF THEM". GuruFocus renamed its section keys
(`Income Statement` -> `income_statement`), so `egmInputs.ts` lists both spellings per metric and
`latestObs` takes whichever it finds. Only the Title Case forms are on the allowlist — the
snake_case fallbacks are unreachable today, which is a real but separate gap (see TODO.md on
`ingest/earnings/financials.py`). Requiring all of them would fail for a reason this test is not
about; requiring none would be the bug it exists to catch.

Pure — reads two source files, no DB and no network.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from routers.earnings import _DASHBOARD_METRIC_CODES

_EGM_INPUTS = Path(__file__).resolve().parents[2] / "frontend/app/components/portfolios/egmInputs.ts"

#: The code groups the Reverse DCF sources, by their `const` name in `egmInputs.ts`.
#: ⚠ NAMED EXPLICITLY rather than "every array in the file" — a test that discovers its own subject
#: passes when the thing it should be watching is deleted.
_GROUPS = ("FCF_CODES", "SBC_CODES", "CAPEX_CODES", "DEP_CODES", "SHARES_CODES", "WACC_CODES")


def _codes(group: str, src: str) -> list[str]:
    m = re.search(rf"const {group} = \[(.*?)\];", src, re.S)
    assert m, f"{group} is gone from egmInputs.ts — the Reverse DCF's inputs moved"
    return re.findall(r"'([^']+)'", m.group(1))


@pytest.fixture(scope="module")
def src() -> str:
    assert _EGM_INPUTS.is_file(), f"expected the frontend source at {_EGM_INPUTS}"
    return _EGM_INPUTS.read_text(encoding="utf-8")


@pytest.mark.parametrize("group", _GROUPS)
def test_each_reverse_dcf_input_has_a_reachable_code(group: str, src: str):
    codes = _codes(group, src)
    assert codes, f"{group} is empty"
    reachable = [c for c in codes if c in _DASHBOARD_METRIC_CODES]
    assert reachable, (
        f"{group} lists {codes} and NONE is on _DASHBOARD_METRIC_CODES, so the endpoint will never "
        f"send it and the panel will render a dash for every company. Add the code to the "
        f"allowlist in routers/earnings.py."
    )


def test_the_three_normalisation_legs_are_on_the_allowlist(src: str):
    """⚠ NAMED INDIVIDUALLY, because these are the three that were missing. The parametrised test
    above would also catch it, but only as "a group failed" — this says which feature breaks."""
    for group, label in (("SBC_CODES", "stock compensation"),
                         ("CAPEX_CODES", "capital expenditure"),
                         ("DEP_CODES", "cash-flow depreciation")):
        assert any(c in _DASHBOARD_METRIC_CODES for c in _codes(group, src)), \
            f"the Reverse DCF cannot see {label}"


def test_the_depreciation_leg_is_the_cash_flow_one(src: str):
    """⚠ CAPEX IS A CASH FIGURE, SO ITS MAINTENANCE PROXY MUST BE ONE TOO. GuruFocus files an
    income-statement depreciation as well, and it is not always equal — comparing a cash figure
    with an accrual one is a difference in BASIS reported as growth spend."""
    codes = _codes("DEP_CODES", src)
    assert codes, "DEP_CODES is empty"
    for c in codes:
        assert "ashflow" in c or "ash_flow" in c or "ashflow_statement" in c.lower(), c
        assert "Income Statement" not in c and "income_statement" not in c, c

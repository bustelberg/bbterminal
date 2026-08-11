"""The AIRS accounts view — what the books actually made, on AIRS's own numbers.

⚠⚠ THE ONE THING THIS MODULE EXISTS TO PREVENT: A VALUE RATIO PRESENTED AS A RETURN.

"Take the portfolio's worth on 31 December and its worth today, divide" is the obvious way to
build this, it is what was asked for, and it is wrong. It is a return ONLY if nothing was paid in
or out — and these are real accounts. AIRS publishes that ratio itself, as `rendement`, beside its
own flow-aware `cumulatief_rendement`. Measured 2026-07-16 across every account we hold:

    `rendement` == eindvermogen/beginvermogen - 1  ->  EXACT in 38 of 38
    the two disagree by more than 1pp              ->  31 of 38

    AITopSelectie OFF DYN      ratio  -5.85%   actual  +46.12%   gap +51.97pp
    BUS_BM_AAN_ww_EUR_2026_d          +0.40%           +14.29%       +13.90pp
    BUS_FTS_BEPOFF_DYN                +2.43%            -5.08%        -7.51pp

A 52-point error that reads as a perfectly ordinary number. `sum(current)/sum(start)` over the
holdings is the same wrong number in different arithmetic — which is how this view was nearly
built, and the cross-check against AIRS's own figure is what caught it.
"""
from __future__ import annotations

import inspect

from routers import _airs_accounts as acc


def code(obj) -> str:
    """The CODE, with the docstring and the `#` comments stripped out.

    ⚠ WHY THIS EXISTS. A guard that greps raw source fires on the prose EXPLAINING the guard —
    this module's own docstring says "NEVER `eindvermogen / beginvermogen`", so a search for that
    string finds the warning and fails. That happened four separate times in one session
    (`portfolioVariants.ts`, `_eur_price`, `_quality`, here), and each time the tempting fix is to
    soften the comment. A test that punishes documentation teaches people to delete it.

    Assert on OUTPUT where you can. Where a structural check is genuinely what you want, check the
    code and not the prose around it.
    """
    src = inspect.getsource(obj)
    parts = src.split('"""')
    if len(parts) > 2:                       # drop the leading docstring
        src = "".join(parts[2:])
    return "\n".join(line.split("#")[0] for line in src.splitlines())


class TestTheReturnIsAIRSsOwnNeverAValueRatio:
    """⚠ Read the module docstring before touching `ytd_pct`."""

    def test_ytd_is_cumulatief_rendement(self):
        src = code(acc.list_accounts)
        assert '"ytd_pct": r.get("cumulatief_rendement")' in src

    def test_nothing_divides_end_by_begin(self):
        """The ratio is READ from AIRS (`rendement`) so the gap can be shown — it is never
        COMPUTED here, because computing it is the act of believing it."""
        src = code(acc)
        for bad in ('end_value_eur / begin', 'eindvermogen / begin', 'end / begin',
                    'eindvermogen/begin', 'end_value_eur/begin'):
            assert bad not in src, f'{bad!r} — the ratio is READ from AIRS, never computed'

    def test_the_holdings_are_never_summed_into_a_portfolio_return(self):
        """`sum(current)/sum(start)` is the same wrong number wearing different arithmetic. The
        account's return comes from `airs_performance`; the holdings are never aggregated."""
        assert 'sum(' not in code(acc.account_holdings)

    def test_rendement_is_labelled_as_the_month_it_measures(self):
        """`rendement` is NOT a rival YTD, and calling it one was this module's own bug.

        It was served as `value_ratio_pct` — "the naive value ratio... the wrong one".
        Measured 2026-07-17: one ATT row is one MONTH, so AITopSelectie's -8.37% is simply
        July's return, and its `stortingen` are 0 in every month of 2026 — the flows the old
        story blamed do not exist. Both figures are AIRS's own and both are right, of
        different windows. The name must say which.
        """
        src = code(acc.list_accounts)
        assert '"latest_month_pct": r.get("rendement_latest_month")' in src
        assert 'value_ratio_pct' not in src, "names the window, not a verdict on the number"


class TestTheAccountsAreNotTheModels:
    """A model portfolio is a COMPOSITION — weights, no holdings — so AIRS has nothing to value and
    publishes no Vermogensoverzicht for one. Measured: 58 models with a composition, 39 accounts
    with AIRS values, overlap ZERO. They answer different questions and neither substitutes for the
    other; the gap between them is implementation drift, timing and fees."""

    def test_the_account_source_is_airs_not_our_prices(self):
        src = code(acc)
        assert "airs_performance" in src and "airs_holding" in src
        # No yfinance anywhere: that is the entire point of this view.
        assert "asset_price" not in src
        assert "_airs_portfolio_perf" not in src

    def test_the_price_result_and_income_are_kept_apart(self):
        """AIRS splits `koersresultaat` (price) from `opbrengsten` (dividends/coupons). Merging
        them would make the account's return look like a price return, which is one of the two
        reasons the positions never sum to it."""
        src = code(acc.list_accounts)
        assert '"price_result_eur": r.get("koersresultaat")' in src
        assert '"income_eur": r.get("opbrengsten")' in src


class TestAnUndefinedReturnStaysUndefined:
    """A position with no opening value — bought during the year, or a cash line (measured:
    `Effectenrekening`, start 0.0) — has an UNDEFINED return. Dividing by zero is infinite and
    calling it 0% is a claim. `parse_airs_excel` already refuses it; this must preserve the
    refusal rather than coalesce it to a number."""

    def test_the_pct_is_passed_through_not_defaulted(self):
        # Passed straight through. An earlier version also asserted `or 0` was absent and failed
        # on the SORT key (`-(r.get('current_value_eur') or 0)`), which is a perfectly good use of
        # it — the guard has to name the field, not ban a token.
        assert '"ytd_return_pct": r.get("ytd_return_pct")' in code(acc.account_holdings)
        assert '"ytd_return_pct": r.get("ytd_return_pct") or' not in code(acc.account_holdings)


class TestTheSnapshotIsTheFreshestOnly:
    """`airs_holding` accumulates a row per day. Mixing two snapshot dates into one position list
    would double-count every holding and silently double the account."""

    def test_holdings_are_filtered_to_one_as_of(self, monkeypatch):
        """Asserted by BEHAVIOUR, not by matching the source. It used to pin the exact line
        `as_of = max(...) for r in rows`, which stopped being true when the snapshot started
        being asked for by date instead of filtered out of the whole history — a change made
        because reading every snapshot under `.limit(2000)` is silently truncated to 1,000 in
        production, and `max()` over an arbitrary surviving subset names an OLD snapshot. The
        invariant ("one date, never mixed") is unchanged; only where it is enforced moved."""
        from tests._fake_supabase import FakeSupabase

        def _h(date_, name):
            return {"portefeuille": "P", "as_of_date": date_, "holding_name": name,
                    "quantity": 1, "currency": "EUR", "weight": 1, "start_value_eur": 1,
                    "current_value_eur": 1, "ytd_return_eur": 0, "ytd_return_pct": 0,
                    "ytd_return_local_pct": 0, "cost_basis_local": 1,
                    "current_price_local": 1, "airs_weight": 1, "fund_result_eur": 0,
                    "fx_result_eur": 0, "airs_result_pct": 0}

        fake = FakeSupabase({
            "airs_holding": [_h("2026-06-30", "STALE"), _h("2026-08-01", "FRESH")],
            "airs_mutatie": [], "airs_model_weight": [], "airs_performance": [],
        })
        monkeypatch.setattr(acc, "supabase", fake)
        # ⚠ AND `deps.supabase`: the mutatie/model-weight reads moved into the shared
        # `routers._airs_ref`, which resolves through `deps` at call time. Patching only the
        # router leaves those pointing at the real client.
        monkeypatch.setattr("deps.supabase", fake)

        got = acc.account_holdings("P")

        assert got["as_of"] == "2026-08-01"
        assert [r["holding_name"] for r in got["rows"]] == ["FRESH"]

    def test_the_year_is_assembled_from_every_month_not_the_freshest_row(self):
        """The freshest ATT row is JULY, not the year — taking it served a price result of
        -130,063 (July, negative) as the year's +420,225. `_year_perf` sums the months; the
        behaviour is pinned properly in test_airs_year_perf.py."""
        assert not hasattr(acc, "_latest_perf"), \
            "_latest_perf read one month as the year — do not reintroduce it"
        src = code(acc.list_accounts)
        assert "_year_perf()" in src

    def test_the_money_columns_are_summed_over_months(self):
        src = code(acc._year_perf)
        assert "_PER_PERIOD_SUMS" in src
        # A month re-measured by each daily run must not be counted twice.
        assert "per_month" in src

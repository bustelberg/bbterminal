"""Active share = ½ Σ|wᵖ − wᵇ|, and every test here is an IDENTITY rather than a fixture.

 THE FIGURE IS UNCHECKABLE BY EYE. "Active share 71%" against a 1,700-name index is not something
a reader can sanity-test, and neither is it something a screenshot review catches — so the things
worth pinning are the properties that make it the quantity it claims to be: it is 0 against itself,
100 against a disjoint book, it equals 100 − overlap exactly, and it is invariant to which SHARE
CLASS you happen to hold.

 THE SHARE-CLASS CASE IS THE ONE THAT BITES. `_asset_benchmark.members` keeps ONE row per company
(Yahoo reports the full company cap on every class), so a book holding GOOG against an index row
carrying GOOGL matches on nothing at all if the join is the ISIN — reporting a full overweight AND
a full underweight in Alphabet, roughly a 4% swing on a US book, invented by the identifier.
"""
from __future__ import annotations

import routers._active_share as A


def test_workbook_identity_bridges_amazon_and_sp_global_spellings(monkeypatch):
    from index_universe.acwi.holdings import load_acwi_holdings
    import index_universe.acwi.asset_membership as membership
    import routers._asset_benchmark as prices
    import routers._benchmark_index as pricing

    workbook, as_of = load_acwi_holdings()
    names = {h["Ticker"]: h["Name"] for h in workbook}
    file_rows = [{"analysis_id": aid, "name": names[ticker], "weight_pct": 50, "currency": "EUR"}
                 for aid, ticker in [(1, "AMZN"), (2, "SPGI")]]
    monkeypatch.setattr(membership, "resolve", lambda: (file_rows, {"as_of": as_of}))
    import index_universe.acwi.holdings as source
    monkeypatch.setattr(source, 'load_acwi_holdings', lambda: (
        [row for row in workbook if row['Ticker'] in ('AMZN', 'SPGI')], as_of))
    monkeypatch.setattr(A, "load_rows_via_copy", lambda *_args: [])
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "US0231351067": {"analysis_id": 1, "gf_company_name": "Amazon.com Inc"},
        "US78409V1044": {"analysis_id": 2, "gf_company_name": "S&P Global Inc."},
    })
    monkeypatch.setattr(prices, "window_marks", lambda *_args: {
        aid: {"start": ("2026-04-15", 100), "end": ("2026-10-15", 110)} for aid in (1, 2)
    })
    monkeypatch.setattr(pricing, "_fx_to_eur", lambda *_args: {})
    got = A.compute_active_share([
        _h("Amazon.com Inc", "US0231351067", 50),
        _h("S&P Global Inc", "US78409V1044", 50),
    ], "ACWI")
    assert got["n_in_benchmark"] == 2
    assert got["off_benchmark_pct"] == 0
    assert got["active_share_pct"] < 1e-9
    assert len(got["rows"]) == 2
    assert all(r["benchmark_pct"] > 0 for r in got["rows"] if r["held"])


def test_benchmark_work_is_shared_and_invalidates_on_data_change(monkeypatch):
    import routers._analysis_cache as cache
    stamp = ["risk-test-original"]
    calls = []
    monkeypatch.setattr(cache, "fingerprint", lambda: stamp[0])
    monkeypatch.setattr(A, "_load_benchmark_weights", lambda *_args: calls.append(1) or {"bench": {}})
    first = A._benchmark_weights("ACWI", None)
    first["bench"]["mutated"] = True
    assert A._benchmark_weights("ACWI", None) == {"bench": {}}
    assert len(calls) == 1
    stamp[0] = "risk-test-refreshed"
    A._benchmark_weights("ACWI", None)
    assert len(calls) == 2


def test_missing_price_does_not_turn_a_member_into_an_off_benchmark_position(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"amazon com": {"name": "AMAZON COM INC", "weight_pct": 0},
                  "apple": {"name": "APPLE INC", "weight_pct": 100}},
        "coverage": {}, "issuer_by_asset": {1: "amazon com"},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "US0231351067": {"analysis_id": 1, "gf_company_name": "Amazon.com Inc"},
    })
    got = A.compute_active_share([_h("Amazon", "US0231351067", 100)], "ACWI")
    assert got["n_in_benchmark"] == 1
    assert got["off_benchmark_pct"] == 0
    held = next(r for r in got["rows"] if r["held"])
    assert held["in_benchmark"] is True
    assert held["benchmark_pct"] == 0


def _bench(monkeypatch, names_caps: list[tuple[str, float]], covered=100.0):
    """Stand in for `_asset_benchmark.members` — (company_name, market_cap_eur)."""
    mem = [{"company_name": n, "isin": f"X{i:09d}", "market_cap_eur": c}
           for i, (n, c) in enumerate(names_caps)]
    import routers._asset_benchmark as B
    monkeypatch.setattr(B, "members",
                        lambda _label: (mem, {"covered_pct": covered, "universe_members": len(mem)}))


def _grid(monkeypatch, by_isin: dict[str, str]):
    """Stand in for the `asset_grid` read — ISIN → the company name it bridges to."""
    monkeypatch.setattr(A, "_grid_by_isin",
                        lambda isins: {i: {"isin": i, "gf_company_name": by_isin.get(i)}
                                       for i in isins if by_isin.get(i)})


def _h(name, isin, w, is_fund=False):
    return {"name": name, "isin": isin, "weight_pct": w, "is_fund": is_fund}


def test_risk_profile_active_share_uses_the_stock_and_bond_policy(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"nvidia": {"key": "nvidia", "name": "Nvidia", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "NVDA": {"isin": "NVDA", "gf_company_name": "Nvidia"},
    })
    got = A.compute_active_share([
        {**_h("Nvidia", "NVDA", 60.0), "bucket": "Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60.0, "Bonds": 40.0})

    assert got["benchmark"] == "60% ACWI + 40% AGGH.AS"
    assert got["active_share_pct"] == 40.0
    assert sum(row["portfolio_pct"] for row in got["rows"] if row["held"]) == 100.0
    assert any(row["name"] == "AGGH.AS" and row["benchmark_pct"] == 40.0 for row in got["rows"])
    assert next(row for row in got["rows"] if row["name"] == "Active bond")["bucket"] == "Bonds"
    assert next(row for row in got["rows"] if row["name"] == "Nvidia")["bucket"] == "Stocks"


def test_equity_only_policy_normalises_stocks_and_omits_bonds(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"nvidia": {"key": "nvidia", "name": "Nvidia", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "NVDA": {"isin": "NVDA", "gf_company_name": "Nvidia"},
    })
    got = A.compute_active_share([
        {**_h("Nvidia", "NVDA", 45.0), "bucket": "Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60, "Bonds": 40, "EquityOnly": 1})
    assert got["benchmark"] == "100% ACWI"
    assert sum(row["portfolio_pct"] for row in got["rows"]) == 100
    assert sum(row["benchmark_pct"] for row in got["rows"]) == 100
    assert abs(got["stocks_pct"] - 45 / 85 * 100) < 1e-9
    assert not any(row["bucket"] == "Bonds" for row in got["rows"])


def test_same_stock_basket_has_identical_active_share_across_profiles(monkeypatch):
    monkeypatch.setattr(A, '_benchmark_weights', lambda *_args: {
        'bench': {'apple': {'key': 'apple', 'name': 'Apple', 'weight_pct': 60},
                  'microsoft': {'key': 'microsoft', 'name': 'Microsoft', 'weight_pct': 40}},
        'coverage': {}, 'issuer_by_asset': {},
    })
    _grid(monkeypatch, {'APPLE': 'Apple', 'MSFT': 'Microsoft'})
    results = []
    for stocks, bonds in [(60, 40), (96, 4)]:
        result = A.compute_active_share([
            _h('Apple', 'APPLE', stocks * .8), _h('Microsoft', 'MSFT', stocks * .2),
            {**_h('Bond', 'BOND', bonds), 'bucket': 'Bonds'},
        ], 'ACWI', benchmark_weights={'Stocks': stocks, 'Bonds': bonds, 'EquityOnly': 1})
        assert abs(result['active_share_pct'] + result['overlap_pct'] - 100) < 1e-9
        assert abs(sum(row['portfolio_pct'] for row in result['rows']) - 100) < 1e-9
        assert abs(sum(row['benchmark_pct'] for row in result['rows']) - 100) < 1e-9
        results.append(result)
    assert abs(results[0]['active_share_pct'] - results[1]['active_share_pct']) < 1e-9
    assert abs(results[0]['overlap_pct'] - results[1]['overlap_pct']) < 1e-9
    for left, right in zip(results[0]['rows'], results[1]['rows']):
        assert left['name'] == right['name']
        assert abs(left['portfolio_pct'] - right['portfolio_pct']) < 1e-9


def test_risk_profile_reuses_the_acwi_asset_identity_bridge_for_direct_stocks(monkeypatch):
    """Policy mode must not turn an ACWI member into an unmatched name."""
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"hermes international": {
            "key": "hermes international", "name": "HERMES INTERNATIONAL", "weight_pct": 100.0,
        }},
        "coverage": {}, "issuer_by_asset": {7: "hermes international"},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "FR0000052292": {"analysis_id": 7, "gf_company_name": "Hermes Internationale"},
    })
    got = A.compute_active_share([
        {**_h("Hermes Internationale", "FR0000052292", 60.0), "bucket": "Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60.0, "Bonds": 40.0})
    hermes = next(row for row in got["rows"] if row["name"] == "Hermes Internationale")
    assert hermes["in_benchmark"] is True
    assert hermes["benchmark_pct"] == 60.0


def test_linde_eu_matches_the_workbook_issuer_even_when_listings_have_different_asset_ids(monkeypatch):
    monkeypatch.setattr(A, '_benchmark_weights', lambda *_args: {
        'bench': {'linde': {'key': 'linde', 'name': 'LINDE PLC', 'weight_pct': .25},
                  'other': {'key': 'other', 'name': 'Other', 'weight_pct': 99.75}},
        'coverage': {}, 'issuer_by_asset': {100: 'linde'},
    })
    monkeypatch.setattr(A, '_grid_by_isin', lambda _ids: {
        'IE000S9YS762': {'analysis_id': 200, 'name': 'Linde EU'},
    })
    monkeypatch.setattr(A, '_execution_by_isin', lambda _ids: {})
    got = A.compute_active_share([_h('Linde', 'IE000S9YS762', .99)], 'ACWI',
        benchmark_weights={'Stocks': 60, 'Bonds': 40, 'EquityOnly': 1}, held_only=True)
    linde = got['rows'][0]
    assert linde['in_benchmark']
    assert abs(linde['portfolio_pct'] - 100) < 1e-9
    assert abs(linde['benchmark_pct'] - .25) < 1e-9
    assert abs(linde['active_pct'] - 99.75) < 1e-9


def test_nonempty_stale_grid_identity_does_not_mask_a_current_execution_match(monkeypatch):
    monkeypatch.setattr(A, '_benchmark_weights', lambda *_args: {
        'bench': {'linde': {'key': 'linde', 'name': 'LINDE PLC', 'weight_pct': 100}},
        'coverage': {}, 'issuer_by_asset': {200: 'linde'},
    })
    monkeypatch.setattr(A, '_grid_by_isin', lambda _ids: {
        'IE000S9YS762': {'analysis_id': 100, 'name': 'Stale display name'},
    })
    monkeypatch.setattr(A, '_execution_by_isin', lambda _ids: {
        'IE000S9YS762': {'analysis_id': 200, 'status': 'ok'},
    })
    got = A.compute_active_share([_h('Linde', 'IE000S9YS762', 100)], 'ACWI')
    assert got['active_share_pct'] == 0
    assert got['rows'][0]['in_benchmark']


def test_execution_identity_keeps_an_acwi_member_matched_while_asset_grid_lags(monkeypatch):
    """The source-of-truth execution mapping must repair a stale grid identity."""
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"loreal": {"key": "loreal", "name": "LOREAL", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {41: "loreal"},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "FR0000120321": {"gf_company_name": "L'Oreal"},
    })
    monkeypatch.setattr(A, "_execution_by_isin", lambda _ids: {
        "FR0000120321": {"analysis_id": 41, "yahoo_symbol": "OR.PA", "status": "ok"},
    })
    got = A.compute_active_share([_h("L' Oreal", "FR0000120321", 100)], "ACWI")
    loreal = next(row for row in got["rows"] if row["held"])
    assert loreal["in_benchmark"] is True
    assert loreal["benchmark_pct"] == 100.0


def test_risk_profile_marks_equity_etfs_as_their_own_table_section(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"nvidia": {"key": "nvidia", "name": "Nvidia", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {})
    got = A.compute_active_share([
        {**_h("World ETF", "ETF", 60.0, is_fund=True), "bucket": "ETF Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60.0, "Bonds": 40.0})
    assert next(row for row in got["rows"] if row["name"] == "World ETF")["bucket"] == "Stock ETFs"


def test_risk_profile_recognises_an_etf_misclassified_as_plain_equity(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"nvidia": {"key": "nvidia", "name": "Nvidia", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {})
    got = A.compute_active_share([
        {**_h("iShares MSCI Wld Mom Fact ETF EUR", "ETF", 60.0), "bucket": "Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60.0, "Bonds": 40.0})
    assert next(row for row in got["rows"] if row["name"].startswith("iShares"))["bucket"] == "Stock ETFs"


def test_risk_profile_returns_only_the_verified_yahoo_symbol_for_a_held_etf(monkeypatch):
    monkeypatch.setattr(A, "_benchmark_weights", lambda *_args: {
        "bench": {"nvidia": {"key": "nvidia", "name": "Nvidia", "weight_pct": 100.0}},
        "coverage": {}, "issuer_by_asset": {},
    })
    monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {
        "ETF": {"isin": "ETF", "yahoo_symbol": "IWMO.AS"},
    })
    got = A.compute_active_share([
        {**_h("iShares MSCI Wld Mom Fact ETF EUR", "ETF", 60.0, is_fund=True), "bucket": "ETF Equity"},
        {**_h("Active bond", "BOND", 40.0), "bucket": "Bonds"},
    ], "ACWI", benchmark_weights={"Stocks": 60.0, "Bonds": 40.0})
    assert next(row for row in got["rows"] if row["name"].startswith("iShares"))["yahoo_symbol"] == "IWMO.AS"


class TestTheIdentities:
    def test_workbook_weight_rollforward_exposes_actual_calculation(self, monkeypatch):
        from types import SimpleNamespace
        import index_universe.acwi.asset_membership as membership
        import routers._asset_benchmark as prices
        import routers._benchmark_index as pricing

        monkeypatch.setattr(membership, "resolve", lambda: ([
            {"analysis_id": 1, "name": "Berkshire Hathaway Inc", "weight_pct": 60, "currency": "EUR"},
            {"analysis_id": 2, "name": "Apple Inc", "weight_pct": 40, "currency": "EUR"},
        ], {"as_of": "15-Apr-2026"}))
        class Grid:
            def select(self, _columns): return self
            def in_(self, _column, _ids): return self
            def execute(self): return SimpleNamespace(data=[])
        monkeypatch.setattr(A, "supabase", SimpleNamespace(table=lambda _name: Grid()))
        monkeypatch.setattr(A, "_grid_by_isin", lambda _ids: {})
        monkeypatch.setattr(prices, "window_marks", lambda *_args: {
            1: {"start": ("2026-04-15", 100), "end": ("2026-10-15", 110)},
            2: {"start": ("2026-04-15", 100), "end": ("2026-10-15", 90)},
        })
        monkeypatch.setattr(pricing, "_fx_to_eur", lambda *_args: {})
        result = A.compute_active_share([_h("Berkshire Hathaway Inc", "US1", 100)], "ACWI")
        row = next(r for r in result["rows"] if "Berkshire" in r["name"])
        calc = row["weight_calculation"]
        assert calc["as_of"] == "2026-04-15"
        assert calc["initial_weight_pct"] == 60
        assert abs(calc["return_eur_pct"] - 10) < 1e-9
        assert abs(calc["normalisation_total"] - 102) < 1e-9
        assert abs(row["benchmark_pct"] - 66 / 102 * 100) < 1e-9
        assert calc["prices"][0]["start_price_eur"] == 100
        # Exercise the HTTP response contract too: missing legacy cap metadata must not turn
        # a valid workbook calculation into FastAPI's 500 response-validation error.
        from routers.airs import ActiveShare
        assert ActiveShare.model_validate(result).benchmark_caps_unstamped == 0

    def test_a_book_that_IS_the_index_has_no_active_share(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 60.0), ("Microsoft Corp", 40.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "US2": "Microsoft Corp"})
        got = A.compute_active_share([_h("Apple", "US1", 60), _h("Microsoft", "US2", 40)], "SP500")
        assert got["active_share_pct"] < 1e-9
        assert abs(got["overlap_pct"] - 100.0) < 1e-9

    def test_a_book_sharing_nothing_is_fully_active(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"NL1": "ASML Holding NV"})
        got = A.compute_active_share([_h("ASML", "NL1", 100)], "SP500")
        assert abs(got["active_share_pct"] - 100.0) < 1e-9
        assert got["overlap_pct"] < 1e-9
        assert abs(got["off_benchmark_pct"] - 100.0) < 1e-9

    def test_held_only_payload_preserves_full_benchmark_arithmetic(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 60), ("Microsoft Corp", 40)])
        _grid(monkeypatch, {"US1": "Apple Inc"})
        holdings = [_h("Apple", "US1", 100)]
        full = A.compute_active_share(holdings, 'SP500')
        compact = A.compute_active_share(holdings, 'SP500', held_only=True)
        assert len(full['rows']) == 2
        assert len(compact['rows']) == 1
        assert all(row['held'] for row in compact['rows'])
        for key in ('active_share_pct', 'overlap_pct', 'off_benchmark_pct', 'benchmark_members'):
            assert compact[key] == full[key]

    def test_active_share_and_overlap_always_sum_to_100(self, monkeypatch):
        """ THE DEFINING IDENTITY, and the reason the ½ is there. Both vectors sum to 1, so every
        overweight has a matching underweight; without the half everything is counted twice and
        this sum comes out at 200 for a book with no overlap at all."""
        _bench(monkeypatch, [("Apple Inc", 50.0), ("Microsoft Corp", 30.0), ("Nvidia Corp", 20.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "US2": "Microsoft Corp", "NL1": "ASML Holding NV"})
        got = A.compute_active_share(
            [_h("Apple", "US1", 50), _h("Microsoft", "US2", 20), _h("ASML", "NL1", 30)], "SP500")
        assert abs(got["active_share_pct"] + got["overlap_pct"] - 100.0) < 1e-9

    def test_it_is_symmetric_in_the_sense_that_only_the_gap_matters(self, monkeypatch):
        """Half the index at double weight, half not held: AS is 50 either way you read it."""
        _bench(monkeypatch, [("A Inc", 25.0), ("B Inc", 25.0), ("C Inc", 25.0), ("D Inc", 25.0)])
        _grid(monkeypatch, {"1": "A Inc", "2": "B Inc"})
        got = A.compute_active_share([_h("A", "1", 50), _h("B", "2", 50)], "SP500")
        assert abs(got["active_share_pct"] - 50.0) < 1e-9


class TestTheIssuerNotTheLine:
    def test_a_different_share_class_still_matches(self, monkeypatch):
        """ THE REGRESSION THE WHOLE MODULE EXISTS FOR. The index row is Alphabet class A; the
        book holds class C. On an ISIN join these are two different companies and active share
        reads 100."""
        _bench(monkeypatch, [("Alphabet Inc", 100.0)])
        _grid(monkeypatch, {"US02079K1079": "Alphabet Inc Class C"})
        got = A.compute_active_share([_h("Alphabet C", "US02079K1079", 100)], "SP500")
        assert got["active_share_pct"] < 1e-9, got["rows"]

    def test_two_classes_of_one_issuer_are_summed_not_replaced(self, monkeypatch):
        """ A book holding BOTH classes holds ONE position in Alphabet. Taking either line alone
        would report half the position as an underweight that does not exist."""
        _bench(monkeypatch, [("Alphabet Inc", 100.0)])
        _grid(monkeypatch, {"US02079K1079": "Alphabet Inc Class C",
                            "US02079K3059": "Alphabet Inc Class A"})
        got = A.compute_active_share(
            [_h("Alphabet C", "US02079K1079", 60), _h("Alphabet A", "US02079K3059", 40)], "SP500")
        assert got["active_share_pct"] < 1e-9
        assert got["n_holdings"] == 1

    def test_corporate_suffixes_do_not_create_a_bet(self, monkeypatch):
        _bench(monkeypatch, [("ASML Holding NV", 100.0)])
        _grid(monkeypatch, {"NL1": "ASML Holding"})
        assert A.compute_active_share([_h("ASML", "NL1", 100)], "AEX")["active_share_pct"] < 1e-9

    def test_two_genuinely_different_companies_are_not_fused(self, monkeypatch):
        """ THE OTHER DIRECTION, and the reason this is not a fuzzy match. A false positive here
        does not mis-price a listing — it merges two companies into one row of a risk report, and
        the report then understates the bet. `Siemens Ltd` is not `Siemens AG`."""
        assert A._issuer_key("Siemens Ltd") != A._issuer_key("Siemens Energy AG")
        assert A._issuer_key("Shell PLC") != A._issuer_key("Shell Midstream Partners")


class TestTheDenominator:
    def test_stocks_keep_their_whole_book_weight_and_the_rest_is_explicit(self, monkeypatch):
        """ THE STATED ASSUMPTION: the individual stocks ARE 100% of the compared portfolio. A
        fund left in at its real weight would count as a bet against every index name at once."""
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"US1": "Apple Inc"})
        got = A.compute_active_share([
            _h("Apple", "US1", 25),
            _h("iShares Core MSCI World", "IE1", 50, is_fund=True),
            _h("Liquiditeiten", "", 25),
        ], "SP500")
        companies = [r for r in got["rows"] if r["held"] and not r["residual"]]
        residual = [r for r in got["rows"] if r["residual"]]
        assert sum(r["portfolio_pct"] for r in companies) == 100.0
        assert not residual
        assert got["active_share_pct"] == 0.0
        #  And the renormalisation is reported, never silent — 25 of 100 is the sleeve compared.
        assert abs(got["stocks_pct"] - 25.0) < 1e-9

    def test_a_book_with_no_individual_stocks_refuses(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {})
        got = A.compute_active_share([_h("iShares", "IE1", 100, is_fund=True)], "SP500")
        assert got["available"] is False
        assert "no individual stocks" in got["reason"]

    def test_a_non_fund_bond_certificate_is_not_an_equity_position(self, monkeypatch):
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "NL1": "Rabobank Certificaten"})
        got = A.compute_active_share([
            {**_h("Apple", "US1", 70), "bucket": "Equity"},
            {**_h("6.5% Rabobank Certificaten 14-perp.", "NL1", 30), "bucket": "Equity"},
        ], "SP500")
        assert got["active_share_pct"] == 0.0
        assert got["stocks_pct"] == 70.0

    def test_an_unmatchable_line_stays_active_rather_than_being_dropped(self, monkeypatch):
        """ DROPPING IT WOULD RENORMALISE THE REST UPWARD AND LOWER ACTIVE SHARE — the flattering
        direction. It is a real position the index has no row for, so it counts, and it is listed."""
        _bench(monkeypatch, [("Apple Inc", 100.0)])
        _grid(monkeypatch, {"US1": "Apple Inc"})    # 'XX9' bridges to nothing
        got = A.compute_active_share(
            [_h("Apple", "US1", 50), _h(None, "XX9", 50)], "SP500")
        assert abs(got["active_share_pct"] - 50.0) < 1e-9
        assert [u["isin"] for u in got["unresolved"]] == ["XX9"]


class TestWhatItRefusesToHide:
    def test_start_basis_uses_the_same_opening_index_weights_as_attribution(self, monkeypatch):
        import routers._asset_benchmark as B
        monkeypatch.setattr(B, "index_rows", lambda label, start: ([
            {"company_name": "Apple Inc", "isin": "US0", "weight_pct": 25.0,
             "start_date": "2026-01-02"},
            {"company_name": "Microsoft Corp", "isin": "US1", "weight_pct": 75.0,
             "start_date": "2026-01-02"},
        ], {"covered_pct": 99.0}))
        monkeypatch.setattr(B, "members", lambda _label: (_ for _ in ()).throw(
            AssertionError("current caps must not be read for a start snapshot")))
        _grid(monkeypatch, {"US1": "Microsoft Corp"})

        got = A.compute_active_share([_h("Microsoft", "US1", 100)], "ACWI", "2026-01-01")

        assert got["active_share_pct"] == 25.0
        assert got["benchmark_caps_from"] == "2026-01-02"
        assert got["benchmark_caps_to"] == "2026-01-02"

    def test_it_reports_how_much_of_the_index_it_could_price(self, monkeypatch):
        """ A MISSING CONSTITUENT INFLATES THE WEIGHT OF THE REST (renormalisation), so an
        unpriceable name we do not hold makes active share read slightly LOW. Never exact."""
        _bench(monkeypatch, [("Apple Inc", 100.0)], covered=78.5)
        _grid(monkeypatch, {"US1": "Apple Inc"})
        got = A.compute_active_share([_h("Apple", "US1", 100)], "SP500")
        assert got["benchmark_covered_pct"] == 78.5

    def test_off_benchmark_is_the_selection_half_of_the_bet(self, monkeypatch):
        """Active share mixes two different decisions — holding something the index does not have,
        and sizing something it does. Only the first is reported separately."""
        _bench(monkeypatch, [("Apple Inc", 50.0), ("Microsoft Corp", 50.0)])
        _grid(monkeypatch, {"US1": "Apple Inc", "NL1": "ASML Holding NV"})
        got = A.compute_active_share([_h("Apple", "US1", 70), _h("ASML", "NL1", 30)], "SP500")
        assert abs(got["off_benchmark_pct"] - 30.0) < 1e-9
        assert got["n_in_benchmark"] == 1

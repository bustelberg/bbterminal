"""A fundamental index sums the EUROS behind it; it does not average per-member growth rates.

⚠⚠ THE BUG THIS PINS READ +19.1%/yr WHERE THE ANSWER IS +7.56%/yr (ACWI FCF/share, 2015→2025).

`blend_series` chained a LEVEL line from `Σw_i·g_i / Σw_i` — each member's own growth, averaged by
MARKET CAP. Two independent defects, and they compound:

  * WRONG WEIGHT. Growth of a sum is `Σv_i(d)/Σv_i(a) − 1`, i.e. each member's growth weighted by
    ITS SHARE OF THE TOTAL BEING GROWN. Cap gives a company with a big valuation and small cash
    flow a big vote on cash-flow growth — on ACWI revenue, NVIDIA holds 4.77% of cap and supplies
    0.02% of revenue, a ~240x overweight on the quantity being measured.
  * UPWARD BIAS. A growth rate is floored at −100% and unbounded above, so averaging an asymmetric
    distribution is biased and the bias scales with dispersion.

⚠⚠ AND IT IS NOT ABOUT NEGATIVES, WHICH IS THE PART EVERY DISCUSSION OF IT GETS WRONG. Revenue is
never negative, so every zero-crossing rule is a no-op on it — and the two constructions still
disagree by more than 5pp/yr on ACWI (~9.95% averaged against +4.60% summed). The whole gap is the
weight.

⚠ SUMMING IS STILL THE CAP-WEIGHTED ANSWER. A cap-weighted index holds the SAME FRACTION of every
company (`n_i = shares_i/Σcap` — the price cancels), so its claim on a fundamental is
`(1/Σcap)·ΣF_i`, exactly proportional to the sum, and the scale cancels in any growth ratio. Cap
weighting enters through the SHARE COUNT inside `F_i`, never as a weight on a rate.

⚠ THE TESTS ARE BUILT SO THE RIGHT ANSWER IS NOT A MATTER OF OPINION: each panel's own euro total is
stated, and the growth of that total IS the index's growth. Only one construction reproduces it.

Pure — no DB, no network.
"""
from __future__ import annotations

from routers._fundamental_blend import blend_breakdown, blend_series, year_bucket

REVENUE = "annuals__Income Statement__Revenue"


def _levels(out: dict) -> dict[str, float]:
    return {p["period"]: p["value"] for p in out["points"]}


# Two members whose CAP ranking is the reverse of their EURO ranking — the only shape in which the
# two constructions can be told apart, and the shape ACWI actually has.
#
#   BIG_CAP  euros  10 ->  20   (+100%),  cap 900
#   SMALL    euros  90 ->  90   (   0%),  cap 100
#
# Euro total: 100 -> 110, so the index returned exactly +10%.
# Cap-weighted average of the rates: 0.9x(+100%) + 0.1x(0%) = +90%.
BIG_CAP = {"weight": 900.0,
           "points": {"2020-12-31": 10.0, "2021-12-31": 20.0},
           "fund_points": {"2020-12-31": 10.0, "2021-12-31": 20.0}}
SMALL = {"weight": 100.0,
         "points": {"2020-12-31": 90.0, "2021-12-31": 90.0},
         "fund_points": {"2020-12-31": 90.0, "2021-12-31": 90.0}}


class TestTheLineSumsEuros:
    def test_it_reproduces_the_move_in_the_euro_total(self):
        got = _levels(blend_series([BIG_CAP, SMALL], REVENUE, year_bucket))
        assert got["2020"] == 100.0
        # 110/100 — not 190, which is what weighting the RATES by cap gives.
        assert got["2021"] == 110.0

    def test_without_euros_it_keeps_the_growth_chain(self):
        # ⚠ THE FALLBACK MUST STAY, and it must be the SAME members minus `fund_points` — this is
        # the assertion that would have caught the aggregate silently never firing, which is how it
        # shipped the first time: three metrics, identical numbers on both paths, nothing to say
        # the new one had not run.
        bare = [{k: v for k, v in m.items() if k != "fund_points"} for m in (BIG_CAP, SMALL)]
        got = _levels(blend_series(bare, REVENUE, year_bucket))
        assert got["2021"] == 190.0            # the old construction, still reachable, still wrong
        assert got["2021"] != _levels(blend_series([BIG_CAP, SMALL], REVENUE, year_bucket))["2021"]

    def test_it_says_which_construction_it_used(self):
        agg = blend_series([BIG_CAP, SMALL], REVENUE, year_bucket)
        assert agg["aggregate"] is True
        assert agg["fund_members"] == 2 and agg["members"] == 2
        bare = [{k: v for k, v in m.items() if k != "fund_points"} for m in (BIG_CAP, SMALL)]
        assert "aggregate" not in blend_series(bare, REVENUE, year_bucket)

    def test_the_euros_are_bucketed_like_the_values(self):
        # ⚠ THE CALLER KEYS BY FILING DATE AND THE CHAIN WALKS BUCKETED PERIODS. Unbucketed, every
        # lookup misses, `fund` is empty, and the branch silently never fires — the exact failure
        # above. Same members, dates one day apart from the values': still aggregates.
        shifted = [{**m, "fund_points": {"2020-06-30": m["fund_points"]["2020-12-31"],
                                         "2021-06-30": m["fund_points"]["2021-12-31"]}}
                   for m in (BIG_CAP, SMALL)]
        assert _levels(blend_series(shifted, REVENUE, year_bucket))["2021"] == 110.0


class TestASumNeedsNoGuards:
    def test_a_round_trip_through_zero_nets_out(self):
        # ⚠⚠ THE PROPERTY THAT MAKES THIS CONSTRUCTION RIGHT FOR FCF. The growth path floors each
        # member at −100% one year and refuses the ratio the next, so a member that goes −200 and
        # back to +200 leaves a permanent mark on the index. A sum subtracts 200 and adds it back.
        a = {"weight": 100.0,
             "points": {"2020-12-31": 1.0, "2021-12-31": -2.0, "2022-12-31": 1.0},
             "fund_points": {"2020-12-31": 100.0, "2021-12-31": -200.0, "2022-12-31": 100.0}}
        b = {"weight": 100.0,
             "points": {"2020-12-31": 9.0, "2021-12-31": 9.0, "2022-12-31": 9.0},
             "fund_points": {"2020-12-31": 900.0, "2021-12-31": 900.0, "2022-12-31": 900.0}}
        got = _levels(blend_series([a, b], REVENUE, year_bucket))
        assert got["2020"] == 100.0
        assert round(got["2021"], 6) == 70.0    # 700/1000
        assert round(got["2022"], 6) == 100.0   # back to 1000/1000 — no permanent mark

    def test_each_step_is_intersected_so_a_joiner_is_not_growth(self):
        # A sum changes when its members change. A member appearing in 2021 must not make the
        # index grow by its whole size.
        old = {"weight": 100.0,
               "points": {"2020-12-31": 1.0, "2021-12-31": 1.1},
               "fund_points": {"2020-12-31": 100.0, "2021-12-31": 110.0}}
        joiner = {"weight": 100.0,
                  "points": {"2021-12-31": 5.0},
                  "fund_points": {"2021-12-31": 500.0}}
        got = _levels(blend_series([old, joiner], REVENUE, year_bucket))
        assert round(got["2021"], 6) == 110.0   # +10%, the only member that spans the step

    def test_the_series_stops_rather_than_going_negative(self):
        # ⚠ A LINE THAT STOPS IS VISIBLE; POINTS A LOG AXIS CANNOT DRAW ARE NOT. Unreachable for a
        # real index — aggregate free cash flow is deeply positive — so reaching it means the
        # totals are wrong, and stopping is how that becomes noticeable.
        a = {"weight": 100.0,
             "points": {"2020-12-31": 1.0, "2021-12-31": -3.0},
             "fund_points": {"2020-12-31": 100.0, "2021-12-31": -300.0}}
        got = _levels(blend_series([a], REVENUE, year_bucket))
        assert got == {"2020": 100.0}


class TestTheEurosAreCarriedLikeTheValues:
    def test_a_member_that_skips_a_period_keeps_its_euros(self):
        # ⚠ UNCARRIED, THE PER-STEP INTERSECTION SHRINKS TO WHOEVER FILED, and the aggregate
        # sawtooths on composition — the failure `carry_forward` exists to prevent, quietly
        # reintroduced one construction over. `b` does not file 2021; it must still be in the step.
        a = {"weight": 100.0,
             "points": {"2020-12-31": 1.0, "2021-12-31": 2.0, "2022-12-31": 2.0},
             "fund_points": {"2020-12-31": 100.0, "2021-12-31": 200.0, "2022-12-31": 200.0}}
        b = {"weight": 100.0,
             "points": {"2020-12-31": 9.0, "2022-12-31": 9.0},
             "fund_points": {"2020-12-31": 900.0, "2022-12-31": 900.0}}
        got = _levels(blend_series([a, b], REVENUE, year_bucket))
        # 2021: a is 200, b carried at 900 -> 1100/1000 = +10%. Without the carry it would be
        # 200/100 = +100%, an index that doubled because one member did not file.
        assert round(got["2021"], 6) == 110.0


class TestTheDecompositionIsAnIdentity:
    def test_the_contributions_sum_exactly_to_the_step(self):
        bd = blend_breakdown([BIG_CAP, SMALL], REVENUE, "2021")
        assert bd["aggregate"] is True
        total = sum(m["contribution_pp"] or 0 for m in bd["members"])
        assert round(total, 9) == round(bd["step_pct"], 9) == 10.0

    def test_share_times_growth_reaches_the_pp(self):
        bd = blend_breakdown([BIG_CAP, SMALL], REVENUE, "2021")
        for m in bd["members"]:
            if m["share_pct"] is None:
                continue
            assert round(m["share_pct"] * m["growth_pct"] / 100.0, 6) \
                == round(m["contribution_pp"], 6)

    def test_the_share_is_of_the_EUROS_not_the_cap(self):
        # ⚠ THE ENTIRE FINDING IN ONE ASSERTION. BIG_CAP carries 90% of the market cap and 10% of
        # the euros; its share of the move is the second. A cap share here would be the old bug
        # wearing the new construction's name.
        bd = blend_breakdown([BIG_CAP, SMALL], REVENUE, "2021")
        big = next(m for m in bd["members"] if m["growth_pct"] == 100.0)
        assert round(big["share_pct"], 6) == 10.0
        assert round(big["contribution_pp"], 6) == 10.0     # 10% x +100%

    def test_a_non_positive_base_still_contributes_exactly(self):
        # ⚠⚠ NOBODY IS DROPPED, WHICH THE GROWTH PATH CANNOT MANAGE. `share x growth` needs a
        # positive base and the difference form does not, so the factors go null while the pp
        # stays exact — and the column still sums.
        crosser = {"weight": 100.0,
                   "points": {"2020-12-31": -2.0, "2021-12-31": 3.0},
                   "fund_points": {"2020-12-31": -200.0, "2021-12-31": 300.0}}
        steady = {"weight": 100.0,
                  "points": {"2020-12-31": 12.0, "2021-12-31": 12.0},
                  "fund_points": {"2020-12-31": 1200.0, "2021-12-31": 1200.0}}
        bd = blend_breakdown([crosser, steady], REVENUE, "2021")
        row = next(m for m in bd["members"] if m["contribution_pp"] not in (0.0, None))
        assert row["growth_pct"] is None and row["share_pct"] is None
        assert round(row["contribution_pp"], 6) == 50.0     # +500 euros over a 1,000 base
        total = sum(m["contribution_pp"] or 0 for m in bd["members"])
        assert round(total, 9) == round(bd["step_pct"], 9) == 50.0

    def test_a_share_over_a_sum_containing_negatives_may_exceed_100_percent(self):
        # ⚠⚠ CORRECT, NOT A BUG TO CLAMP. `s_a` can contain negatives, so a profitable member's
        # share has a denominator smaller than its own numerator. Clamping would break the only
        # identity this panel exists for — `share × growth = pp`, which still holds here.
        crosser = {"weight": 100.0,
                   "points": {"2020-12-31": 12.0, "2021-12-31": 3.0},
                   "fund_points": {"2020-12-31": -200.0, "2021-12-31": 300.0}}
        steady = {"weight": 100.0,
                  "points": {"2020-12-31": 12.0, "2021-12-31": 12.0},
                  "fund_points": {"2020-12-31": 1200.0, "2021-12-31": 1200.0}}
        bd = blend_breakdown([crosser, steady], REVENUE, "2021")
        big = next(m for m in bd["members"] if m["share_pct"] is not None)
        assert round(big["share_pct"], 6) == 120.0
        assert round(big["share_pct"] * big["growth_pct"] / 100.0, 6) \
            == round(big["contribution_pp"], 6) == 0.0


class TestAMetricIsAggregatableOnlyIfItsForecastLegIs:
    """⚠⚠ THE SEAM BETWEEN TWO `blend_series` CALLS, WHICH NOTHING ELSE ASSERTS ACROSS.

    A forecast is a separate metric code, blended separately and rebased on the actual it
    continues, so aggregating the ACTUAL leg alone puts the two halves of one chart on two
    different scales. Measured on `eps_nri` (2026-08-25): the line ran to the euro-chain level and
    the forecast restarted near the per-share one, a vertical jump from LTM to 2026e. Every unit
    was individually correct and it was caught BY EYE.

    ⚠⚠ THE FIRST FIX WAS TO HOLD THE WHOLE METRIC BACK, and this class asserted that — that a
    metric WITH a consensus is never aggregated. It was superseded the same day: the euros for a
    year nobody has lived CAN be built (`estimate × latest filed shares`, see `_shares_at`), so
    both legs aggregate and `continue_from` joins them at the real euro step —
    `TestTheForecastLegJoinsTheLineItContinues` below is that behaviour. The old assertions went
    red on the shipped code and are gone; what survives is the RULE, which never changed: BOTH
    legs or NEITHER.
    """

    def test_a_metric_whose_consensus_can_be_priced_is_aggregated_with_it(self, monkeypatch):
        """⚠⚠ THE ONLY LIVE SUBJECT LEFT THE SET ON 2026-08-31, so this drives the rule against a
        DECLARED pairing rather than against today's configuration. `eps_nri` was the one metric
        that both aggregated and carried a consensus; it is now positives-only
        (`_POSITIVE_ONLY_METRICS`) and `_AGGREGATABLE_PER_SHARE` is empty, which would leave the
        rule asserted only by its negative case — green forever, proving nothing, and silently
        unprotected the day a per-share metric is added back."""
        from routers import earnings as e
        monkeypatch.setattr(e, "_AGGREGATABLE_PER_SHARE", frozenset({"eps_nri"}))
        monkeypatch.setattr(e, "_AGGREGATABLE_FORECAST", frozenset({"eps_nri_estimate"}))
        # ⚠ THE PRECONDITION, ASSERTED — without an actual overlap the rule below is vacuous.
        assert e._FORECAST_METRIC["eps_nri"] == "eps_nri_estimate"

        assert "eps_nri" in e.aggregatable_metrics([])
        assert e.aggregatable_metrics(["eps_nri"]) == ["eps_nri"]
        assert e.aggregatable_metrics(["revenue", "fcf_ps"]) == ["revenue", "fcf_ps"]

    def test_todays_configuration_sums_every_charted_level(self):
        """⚠ WHERE THE TWO SETS ACTUALLY STAND, pinned so the trade is a decision rather than a
        drift.

        ⚠⚠ `fcf_ps` WENT BACK ON THE AGGREGATE ON 2026-09-04, because the rate average was wrong by
        a factor of four rather than merely biased. ACWI 2015→2025, same members and window:
        +33.93%/yr as a cap-weighted average of per-member growth rates, against +7.52%/yr as the
        growth of the SUM of their free cash flow — with the median constituent at +8.90%/yr and
        the aggregate reproducing the +7.56% this construction measured on 2026-08-25. 18.6x over a
        decade is not a global index's cash flow.

        `eps_nri` followed the same day and reads the same way: +26.50%/yr as a rate average
        against +8.31% summed, with the median constituent at +8.82%.

        ⚠⚠ AND ITS FORECAST LEG CAME ALONG WITHOUT ANYONE LISTING IT — `_AGGREGATABLE_FORECAST` is
        derived from this set, which is the whole reason a consensus cannot end up on a different
        construction from the actual it continues. Asserted here because it is the property that
        makes the pair safe, not an implementation detail.

        ⚠ A METRIC IN BOTH SETS WOULD BE A SURVIVORSHIP-FILTERED SUM, which is the one combination
        nothing wants — the sum needs no filter and the filter only adds bias. That is why the two
        edits are one decision, and it is asserted below rather than left to memory."""
        from routers import earnings as e
        assert e._AGGREGATABLE_PER_SHARE == frozenset({"fcf_ps", "eps_nri"})
        assert e._AGGREGATABLE_FORECAST == frozenset({e._FORECAST_METRIC["eps_nri"]})
        assert set(e.aggregatable_metrics([])) == {"revenue", "fcf_ps", "eps_nri"}
        assert e.aggregatable_metrics(["eps_nri"]) == ["eps_nri"]
        assert e._POSITIVE_ONLY_METRICS == frozenset()
        assert not (e._POSITIVE_ONLY_METRICS
                    & (e._AGGREGATABLE_PER_SHARE | e._AGGREGATABLE_TOTAL))

    def test_a_metric_whose_consensus_cannot_be_priced_is_refused_whole(self, monkeypatch):
        # ⚠ THE RULE ITSELF, WITH THE PRICEABLE SET EMPTIED — this is what `aggregatable_metrics`
        # is FOR, and with today's data every consensus happens to be priceable, so the branch
        # would otherwise never be exercised at all.
        from routers import earnings as e
        monkeypatch.setattr(e, "_AGGREGATABLE_FORECAST", frozenset())
        assert "eps_nri" not in e.aggregatable_metrics([])
        assert e.aggregatable_metrics(["eps_nri"]) == []
        # …and the metrics with no forecast leg are untouched.
        assert set(e.aggregatable_metrics([])) == (
            (e._AGGREGATABLE_PER_SHARE | e._AGGREGATABLE_TOTAL) - set(e._FORECAST_METRIC))

    def test_it_is_decided_once_and_not_per_request(self):
        # ⚠ A NARROWED REQUEST AND A FULL ONE MUST AGREE. Keying the rule off "is the forecast code
        # in this payload" would let the same metric draw two different lines depending on which
        # chart asked for it.
        from routers.earnings import aggregatable_metrics
        full = set(aggregatable_metrics([]))
        assert ("eps_nri" in aggregatable_metrics(["eps_nri"])) is ("eps_nri" in full)
        assert ("eps_nri" in aggregatable_metrics(["eps_nri", "revenue"])) is ("eps_nri" in full)

    def test_an_unknown_metric_is_simply_not_aggregatable(self):
        from routers.earnings import aggregatable_metrics
        # `fundamental_totals` filters to the aggregatable sets anyway; this only has to not raise.
        assert aggregatable_metrics(["made_up"]) == ["made_up"]


class TestTheLtmPointNeedsItsOwnEuros:
    """⚠⚠ LTM IS NOT A FILED PERIOD — this app assembles it — so the euros must be built for it
    explicitly (`fundamental_totals` multiplies `_ltm_by_company`'s value by the as-of share count).

    Without them the outcome depends on THE DATE THE CODE RUNS, which is the worst property a
    defect can have: `period_end("LTM")` is TODAY, so within `_MAX_CARRY_DAYS` of the last filing
    `carry_forward` holds the previous year's euros into LTM and the line goes FLAT into its newest
    point (silent, reads as "nothing changed"); past that bound the point is dropped instead. One
    fix covers both.
    """

    #: Totals 1,000 → 1,100 over the fiscal years, with a trailing twelve months of 1,210.
    MEMBERS = [
        {"weight": 100.0,
         "points": {"2023-12-31": 1.0, "2024-12-31": 1.1, "LTM": 1.21},
         "fund_points": {"2023-12-31": 100.0, "2024-12-31": 110.0, "LTM": 121.0}},
        {"weight": 100.0,
         "points": {"2023-12-31": 9.0, "2024-12-31": 9.9, "LTM": 10.89},
         "fund_points": {"2023-12-31": 900.0, "2024-12-31": 990.0, "LTM": 1089.0}},
    ]

    def test_the_ltm_point_is_the_trailing_total_not_the_last_fiscal_year(self):
        got = _levels(blend_series(self.MEMBERS, REVENUE, year_bucket))
        assert got == {"2023": 100.0, "2024": 110.0, "LTM": 121.0}

    def test_the_key_survives_bucketing(self):
        # ⚠ `year_bucket("LTM")` is `"LTM"[:4]` = `"LTM"`, so the caller may key the euros on the
        # literal period. If that ever changed, the point would silently vanish — the same class of
        # miss as keying the euros by filing date on a chain that walks buckets.
        assert year_bucket("LTM") == "LTM"


class TestTheForecastLegJoinsTheLineItContinues:
    """⚠⚠ THE SEAM, MEASURED. A forecast is its own `blend_series` call; on the aggregate path it
    must start where the actual leg stopped, and the join is the real euro step across the
    boundary — `Σest(first forecast) ÷ Σactual(last actual)`.

    ⚠ THIS IS MORE EXACT THAN THE GROWTH PATH'S CONTINUATION, not merely equivalent: that one
    restarts the forecast at the weighted mean of each member's value rebased on its OWN actual
    base, which only approximates where the line stopped. Measured on the fixture below: 127.78
    against a true 110.0, i.e. a fabricated +16pp jump at the seam.
    """

    ACTUAL = [
        {"weight": 100.0, "points": {"2023-12-31": 1.0, "2024-12-31": 1.2},
         "fund_points": {"2023-12-31": 100.0, "2024-12-31": 120.0}},
        {"weight": 100.0, "points": {"2023-12-31": 9.0, "2024-12-31": 8.8},
         "fund_points": {"2023-12-31": 900.0, "2024-12-31": 880.0}},
    ]
    #: Totals 1,000 → 1,000 on actuals (the line runs flat at 100), consensus 1,100 → join at 110.
    FORECAST = [
        {"weight": 100.0, "points": {"2025-12-31": 1.5},
         "fund_points": {"2025-12-31": 150.0},
         "fund_base_points": {"2023-12-31": 100.0, "2024-12-31": 120.0},
         "base_points": {"2023-12-31": 1.0, "2024-12-31": 1.2}},
        {"weight": 100.0, "points": {"2025-12-31": 9.5},
         "fund_points": {"2025-12-31": 950.0},
         "fund_base_points": {"2023-12-31": 900.0, "2024-12-31": 880.0},
         "base_points": {"2023-12-31": 9.0, "2024-12-31": 8.8}},
    ]

    def _join(self):
        a = blend_series(self.ACTUAL, REVENUE, year_bucket)
        last = a["points"][-1]
        return a, blend_series(self.FORECAST, REVENUE, year_bucket,
                               continue_from={"level": last["value"],
                                              "period": last["period"]})

    def test_it_starts_at_the_real_step_from_the_last_actual_period(self):
        a, f = self._join()
        assert _levels(a) == {"2023": 100.0, "2024": 100.0}
        assert _levels(f) == {"2025": 110.0}          # 100 x 1100/1000

    def test_without_the_join_it_restarts_at_100(self):
        # ⚠ THE FALLBACK IS 100, NOT A GUESS. A standalone series has nothing to continue.
        assert _levels(blend_series(self.FORECAST, REVENUE, year_bucket)) == {"2025": 100.0}

    def test_the_old_growth_chain_leg_lands_nowhere_near_the_line(self):
        # ⚠ THE BUG THIS REPLACES, kept as a number rather than a description: the actual leg on
        # the aggregate and the forecast leg on the growth chain drew a vertical jump at LTM.
        bare = [{k: v for k, v in m.items()
                 if k not in ("fund_points", "fund_base_points")} for m in self.FORECAST]
        assert round(_levels(blend_series(bare, REVENUE, year_bucket))["2025"], 4) == 127.7778

    def test_a_member_with_a_consensus_but_no_actual_is_not_in_the_join(self):
        # A sum changes when its members change, and the seam is exactly where nobody would look
        # for composition showing up as growth.
        a = blend_series(self.ACTUAL, REVENUE, year_bucket)
        last = a["points"][-1]
        newcomer = {"weight": 100.0, "points": {"2025-12-31": 5.0},
                    "fund_points": {"2025-12-31": 500.0}}
        f = blend_series([*self.FORECAST, newcomer], REVENUE, year_bucket,
                         continue_from={"level": last["value"], "period": last["period"]})
        assert _levels(f) == {"2025": 110.0}

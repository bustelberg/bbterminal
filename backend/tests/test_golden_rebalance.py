"""Golden master for the legacy momentum rebalance.

This is the safety net for unifying the two signal engines. It replays a REAL
scheduled strategy's rebalance — strategy 34, "MomentumTopSelectie Offensief",
1,479-company Leonteq universe, 24 holdings — through `run_current_portfolio`
against frozen inputs, and asserts the output is byte-for-byte what the engine
produces today.

It exercises the whole calculation: universe filtering, the 30-day staleness
guard, the strict `<` signal cutoff, all seven weighted signals, per-category
0-100 min-max normalization, sector aggregation, top-N-sector / top-N-per-sector
selection, equal weighting, and entry/exit pricing in both local and EUR.

THE TWO FIXTURES
    `snapshot_829` — anchored to a rebalance that actually shipped (as_of Mon
        2026-07-06). Its holdings are diffed against `current_picks_snapshot`.
        Steady-state shape: the engine decides Monday's rebalance from Friday's
        close, so there is NO bar on the cutoff.
    `trading_day` — anchored to a past Monday (as_of 2026-06-01) that already has
        1,474 closes. A bar sits exactly ON the cutoff, so `searchsorted(side=
        "left")` and `side="right")` disagree and the strict `<` guard becomes
        observable. This is the only fixture that can catch a lookahead
        regression end-to-end.

WHY A FIXTURE AND NOT THE DB
    The engine cannot be replayed off the live database. GuruFocus publishes
    some closes late, and `ingest/prices.py` writes them with their true
    (earlier) `target_date`. So a bar can appear *inside* a window that a past
    rebalance already computed over. Concretely: Bayer's 2026-07-03 close was
    written on 2026-07-06, two days after snapshot 829 shipped — which moves
    Bayer's score from 27.40 to 27.56 and swaps its intra-sector rank with
    Sartorius. Same 24 names, different order. The price table is append-only in
    `recorded_at`, NOT in `target_date`. A golden master therefore has to freeze
    its inputs, which is exactly what `scripts/capture_golden_rebalance.py` does.

WHAT THIS CATCHES THAT THE UNIT TESTS DON'T
    Verified by mutation testing (2026-07-10), each mutation applied to a clean
    tree and reverted:

      1. strict `<` cutoff -> `<=` (lookahead)   test_signals + `trading_day`
      2. MAX_STALENESS_DAYS 30 -> 10             caught by NOTHING (see blind spots)
      3. 12-1 lookback 12mo -> 11mo, panel only  caught by both
      4. 12-1 lookback 12mo -> 11mo, BOTH impls  caught by both
      5. sector aggregation mean() -> median()   ALL 863 other tests pass; only this fails

    (5) is the point. The synthetic suite puts 2 companies in a sector, where
    mean == median by construction, so no unit test can see it. Real data with
    1,479 companies across 11 sectors can. Everything about how the pieces
    COMPOSE — score normalization bounds, sector aggregation, rank tie-breaks,
    the staleness guard's effect on the min-max range — is only observable at
    this scale.

BLIND SPOTS (do not assume this test covers them)
    * The 30-day staleness guard is not exercised: at neither as_of does a
      company in the Leonteq universe sit in the 10-30 day stale band.
    * ETF overlay and cash sleeve are not covered — strategy 34 has neither.
      Those transforms are pinned by `test_portfolio_math.py`.

WHEN THIS FAILS
    You changed the calculation. That is the point. If the change is intended,
    re-baseline deliberately:

        cd backend && PYTHONPATH=. uv run python scripts/capture_golden_rebalance.py \
            --strategy-id 34 --snapshot-id 829 \
            --out tests/fixtures/golden_rebalance_34.npz

        cd backend && PYTHONPATH=. uv run python scripts/capture_golden_rebalance.py \
            --strategy-id 34 --today 2026-06-01 --price-through 2026-06-01 \
            --out tests/fixtures/golden_rebalance_34_trading_day.npz

    and say so in the commit message. Regenerating a fixture to make a red test
    green, without understanding the diff, throws away the only thing protecting
    the live strategy.
"""
from __future__ import annotations

import io
import json
from dataclasses import asdict, is_dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURES = {
    "snapshot_829": _FIXTURE_DIR / "golden_rebalance_34.npz",
    "trading_day": _FIXTURE_DIR / "golden_rebalance_34_trading_day.npz",
}
# Only this one was captured from a rebalance that reached a user.
SHIPPED_FIXTURE = "snapshot_829"
# Only this one has a bar on the signal cutoff (see module docstring).
TRADING_DAY_FIXTURE = "trading_day"

pytestmark = pytest.mark.skipif(
    not all(p.exists() for p in FIXTURES.values()),
    reason=f"golden fixtures missing from {_FIXTURE_DIR} "
           "(regenerate with scripts/capture_golden_rebalance.py)",
)

# The engine returns full float precision; `current_picks_snapshot.holdings`
# persists `weight` rounded to 4dp. Only the shipped-comparison needs this.
_SHIPPED_WEIGHT_DP = 4


def _text(z, key: str) -> str:
    return bytes(z[f"{key}__utf8"]).decode("utf-8")


def _frame(z, key: str) -> pd.DataFrame | None:
    raw = z[f"{key}__parquet"].tobytes()
    if not raw:
        return None
    df = pd.read_parquet(io.BytesIO(raw))
    # The capture narrows keys for size; restore the dtypes the engine loaders
    # hand it, so the replay sees exactly what production sees.
    if "company_id" in df.columns:
        df["company_id"] = df["company_id"].astype("int64")
    if "target_date" in df.columns:
        df["target_date"] = pd.to_datetime(df["target_date"])
    return df


def _monthly_eligible(raw: dict) -> dict[str, dict[int, str | None]]:
    """`{"YYYY-MM": {company_id: sector}}` — JSON stringifies the inner int keys,
    and the engine looks them up by `int`. Left as strings, every company misses
    its sector, the universe empties, and the backtest silently returns nothing.
    """
    return {month: {int(cid): sector for cid, sector in members.items()}
            for month, members in raw.items()}


def _load_fixture(name: str) -> dict:
    z = np.load(FIXTURES[name], allow_pickle=False)
    return {
        "name": name,
        "meta": json.loads(_text(z, "meta_json")),
        "config": json.loads(_text(z, "config_json")),
        "today": date.fromisoformat(_text(z, "today")),
        "company_currency": {int(k): v for k, v in json.loads(_text(z, "company_currency_json")).items()},
        "monthly_eligible": _monthly_eligible(json.loads(_text(z, "monthly_eligible_json"))),
        "expected": json.loads(_text(z, "expected_holdings_json")),
        "shipped": json.loads(_text(z, "shipped_holdings_json")),
        "universe_df": _frame(z, "universe_df"),
        "prices_df": _frame(z, "prices_df"),
        "prices_local_df": _frame(z, "prices_local_df"),
        "volumes_df": _frame(z, "volumes_df"),
    }


def _replay(golden: dict) -> list[dict]:
    """Run the real engine over the frozen inputs. No DB, no network."""
    from momentum.backtest import run_current_portfolio
    from momentum.backtest.types import BacktestConfig

    cfg = dict(golden["config"])
    for k in ("start_date", "end_date"):
        if isinstance(cfg.get(k), str):
            cfg[k] = date.fromisoformat(cfg[k])

    result = run_current_portfolio(
        BacktestConfig(**cfg),
        golden["prices_df"],
        golden["universe_df"],
        volumes_df=golden["volumes_df"],
        prices_local_df=golden["prices_local_df"],
        company_currency=golden["company_currency"],
        monthly_eligible=golden["monthly_eligible"],
        today=golden["today"],
    )
    assert result.as_of_date == golden["meta"]["as_of_date"], (
        f"replay drifted to as_of={result.as_of_date}, fixture recorded "
        f"{golden['meta']['as_of_date']} — the rebalance anchor changed"
    )
    return [asdict(h) if is_dataclass(h) else h for h in result.holdings]


# Loading + replaying costs ~4s per fixture; do each once for the whole module.
_CACHE: dict[str, tuple[dict, list[dict]]] = {}


def _get(name: str) -> tuple[dict, list[dict]]:
    if name not in _CACHE:
        g = _load_fixture(name)
        _CACHE[name] = (g, _replay(g))
    return _CACHE[name]


@pytest.fixture(scope="module", params=sorted(FIXTURES))
def case(request) -> tuple[dict, list[dict]]:
    return _get(request.param)


@pytest.fixture(scope="module")
def shipped_case() -> tuple[dict, list[dict]]:
    return _get(SHIPPED_FIXTURE)


@pytest.fixture(scope="module")
def trading_day_case() -> tuple[dict, list[dict]]:
    return _get(TRADING_DAY_FIXTURE)


def _by_cid(holdings: list[dict]) -> dict[int, dict]:
    return {int(h["company_id"]): h for h in holdings}


class TestGoldenRebalance:
    """Every assertion is exact. Nothing here has a tolerance. Runs per fixture."""

    def test_holding_count(self, case):
        golden, replayed = case
        assert len(replayed) == len(golden["expected"])
        assert len(replayed) == golden["config"]["top_n_sectors"] * golden["config"]["top_n_per_sector"]

    def test_company_set(self, case):
        golden, replayed = case
        assert _by_cid(replayed).keys() == _by_cid(golden["expected"]).keys()

    @pytest.mark.parametrize("field", ["weight", "score", "entry_price_local", "entry_price_eur"])
    def test_numeric_fields_exact(self, case, field):
        golden, replayed = case
        exp, got = _by_cid(golden["expected"]), _by_cid(replayed)
        for cid in sorted(exp):
            e, g = exp[cid][field], got[cid][field]
            if e is None or g is None:
                assert e == g, f"cid={cid} {field}: {e!r} vs {g!r}"
            else:
                # `==`, not approx: a refactor must not move a single bit.
                assert float(g) == float(e), f"cid={cid} {field}: expected {e}, got {g}"

    @pytest.mark.parametrize("field", ["sector", "sector_rank", "company_rank", "currency", "entry_date"])
    def test_categorical_fields_exact(self, case, field):
        golden, replayed = case
        exp, got = _by_cid(golden["expected"]), _by_cid(replayed)
        for cid in sorted(exp):
            assert got[cid][field] == exp[cid][field], f"cid={cid} {field}"

    def test_ranks_are_a_permutation_within_each_sector(self, case):
        """Guards the selection invariant itself, not just the recorded values."""
        _, replayed = case
        per_sector: dict[str, list[int]] = {}
        for h in replayed:
            per_sector.setdefault(h["sector"], []).append(int(h["company_rank"]))
        for sector, ranks in per_sector.items():
            assert sorted(ranks) == list(range(1, len(ranks) + 1)), f"{sector}: {sorted(ranks)}"

    def test_weights_sum_to_one(self, case):
        _, replayed = case
        assert sum(h["weight"] for h in replayed) == pytest.approx(1.0, abs=1e-9)

    def test_scores_descend_with_company_rank(self, case):
        _, replayed = case
        for sector in {h["sector"] for h in replayed}:
            rows = sorted((h for h in replayed if h["sector"] == sector), key=lambda h: h["company_rank"])
            scores = [h["score"] for h in rows]
            assert scores == sorted(scores, reverse=True), f"{sector}: {scores}"


class TestStrictCutoff:
    """Only the trading-day fixture can observe the `<` vs `<=` distinction."""

    def test_a_bar_exists_on_the_signal_cutoff(self, trading_day_case):
        """If this ever goes to zero the fixture has silently stopped covering
        the lookahead guard, and mutation (1) would pass unnoticed."""
        golden, _ = trading_day_case
        as_of = pd.Timestamp(golden["meta"]["as_of_date"])
        on_cutoff = int((golden["prices_df"]["target_date"] == as_of).sum())
        assert on_cutoff > 1000, f"only {on_cutoff} bars on the cutoff {as_of.date()}"

    def test_entry_precedes_the_cutoff_bar(self, trading_day_case):
        """Entry is the PRIOR trading day's close — the last bar strictly before
        `as_of` (`current_portfolio.py:302`), which is also the newest bar the
        signals may see. So with a real bar sitting ON the cutoff, an inclusive
        `<=` guard would let the signals read a close that no holding was entered
        at. Every entry must stay strictly before it.
        """
        golden, replayed = trading_day_case
        as_of = golden["meta"]["as_of_date"]
        assert all(h["entry_date"] < as_of for h in replayed), (
            f"a holding entered on/after the cutoff {as_of}: "
            f"{sorted({h['entry_date'] for h in replayed})}"
        )

    def test_the_cutoff_bar_is_the_exit_mark(self, trading_day_case):
        """The cutoff bar exists and is what we mark against — it just must not
        feed the signals. If exits stopped landing on it, the fixture would no
        longer be exercising the boundary at all."""
        golden, replayed = trading_day_case
        as_of = golden["meta"]["as_of_date"]
        assert golden["meta"]["latest_price_date"] == as_of
        assert any(h["exit_date"] == as_of for h in replayed)


class TestShippedSnapshot:
    """What actually reached the user, versus what the engine computes now.

    The company SET must still match — that is the load-bearing claim, and it is
    what a refactor could plausibly break. Intra-sector ordering is allowed to
    drift, because late-arriving price bars legitimately change scores (see the
    module docstring). This test documents that boundary rather than hiding it.
    """

    def test_shipped_company_set_still_reproduces(self, shipped_case):
        golden, replayed = shipped_case
        assert _by_cid(replayed).keys() == _by_cid(golden["shipped"]).keys()

    def test_shipped_weights_match_at_persisted_precision(self, shipped_case):
        golden, replayed = shipped_case
        got = _by_cid(replayed)
        for cid, h in _by_cid(golden["shipped"]).items():
            assert round(got[cid]["weight"], _SHIPPED_WEIGHT_DP) == h["weight"]

    def test_known_drift_is_confined_to_bayer_and_its_rank_partner(self, shipped_case):
        """Pin the ONE explained discrepancy so a new, unexplained one fails."""
        golden, replayed = shipped_case
        got, shipped = _by_cid(replayed), _by_cid(golden["shipped"])
        drifted = {
            cid for cid in shipped
            if got[cid]["score"] != shipped[cid]["score"]
            or got[cid]["company_rank"] != shipped[cid]["company_rank"]
        }
        # 3463 = Bayer AG (late 2026-07-03 bar), 5025 = the name it swapped with.
        assert drifted == {3463, 5025}, (
            f"unexplained drift vs the shipped snapshot: {sorted(drifted - {3463, 5025})}"
        )

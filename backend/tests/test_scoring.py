"""Unit tests for scoring helpers."""
from __future__ import annotations

import numpy as np
import pandas as pd

from momentum.scoring import random_select


def _universe_df(n_per_sector: dict[str, int]) -> pd.DataFrame:
    rows = []
    cid = 1
    for sector, n in n_per_sector.items():
        for _ in range(n):
            rows.append({
                "company_id": cid,
                "sector": sector,
                "company_name": f"Co{cid}",
                "gurufocus_ticker": f"T{cid}",
            })
            cid += 1
    return pd.DataFrame(rows)


class TestRandomSelect:
    def test_picks_correct_counts(self):
        df = _universe_df({"Tech": 10, "Energy": 10, "Health": 10, "Finance": 10})
        out = random_select(
            df,
            top_n_sectors=2,
            top_n_per_sector=3,
            rng=np.random.default_rng(42),
        )
        assert out["sector"].nunique() == 2
        assert len(out) == 6  # 2 sectors × 3 per sector

    def test_seed_is_deterministic(self):
        df = _universe_df({"Tech": 10, "Energy": 10, "Health": 10, "Finance": 10})
        out1 = random_select(df, top_n_sectors=2, top_n_per_sector=3, rng=np.random.default_rng(7))
        out2 = random_select(df, top_n_sectors=2, top_n_per_sector=3, rng=np.random.default_rng(7))
        assert out1["company_id"].tolist() == out2["company_id"].tolist()

    def test_different_seeds_differ(self):
        df = _universe_df({"Tech": 10, "Energy": 10, "Health": 10, "Finance": 10})
        out1 = random_select(df, top_n_sectors=2, top_n_per_sector=3, rng=np.random.default_rng(1))
        out2 = random_select(df, top_n_sectors=2, top_n_per_sector=3, rng=np.random.default_rng(2))
        # Picks should not be identical with two different seeds.
        assert out1["company_id"].tolist() != out2["company_id"].tolist()

    def test_handles_undersized_sector(self):
        # Sector with only 2 companies, asking for 5 → returns 2 from that sector.
        df = _universe_df({"Tech": 2, "Energy": 10})
        out = random_select(df, top_n_sectors=2, top_n_per_sector=5, rng=np.random.default_rng(0))
        assert (out["sector"] == "Tech").sum() == 2
        assert (out["sector"] == "Energy").sum() == 5

    def test_handles_fewer_sectors_than_requested(self):
        df = _universe_df({"Tech": 10})
        out = random_select(df, top_n_sectors=4, top_n_per_sector=3, rng=np.random.default_rng(0))
        assert out["sector"].nunique() == 1
        assert len(out) == 3

    def test_empty_input_returns_empty(self):
        out = random_select(
            pd.DataFrame(),
            top_n_sectors=2,
            top_n_per_sector=3,
            rng=np.random.default_rng(0),
        )
        assert out.empty

    def test_score_columns_are_nan(self):
        # Random mode should not pretend to have meaningful scores.
        df = _universe_df({"Tech": 5, "Energy": 5})
        out = random_select(df, top_n_sectors=2, top_n_per_sector=3, rng=np.random.default_rng(0))
        assert "momentum_score" in out.columns
        assert out["momentum_score"].isna().all()

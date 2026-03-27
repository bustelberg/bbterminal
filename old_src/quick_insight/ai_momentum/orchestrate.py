# src\quick_insight\ai_momentum\orchestrate.py

from __future__ import annotations

import pandas as pd

from quick_insight.ai_momentum.signals.price import get_universe_price_stats
from quick_insight.ai_momentum.signals.smart_money import get_universe_smart_money_stats
from quick_insight.ai_momentum.scoring.scorer import compute_weighted_score
from quick_insight.ai_momentum.utils import aggregate_to_sector

# ---------------------------------------------------------------------------
# Dummy universe (replace later with get_universe())
# ---------------------------------------------------------------------------

_DUMMY_UNIVERSE = pd.DataFrame([
    {
        "sector": "IT Services & Software",
        "company_name": "Apple",
        "primary_ticker": "AAPL",
        "primary_exchange": "NASDAQ",
    },
    {
        "sector": "IT Services & Software",
        "company_name": "Microsoft",
        "primary_ticker": "MSFT",
        "primary_exchange": "NASDAQ",
    },
    {
        "sector": "Semiconductors",
        "company_name": "NVIDIA",
        "primary_ticker": "NVDA",
        "primary_exchange": "NASDAQ",
    },
])

_SMART_MONEY_KEEP_COLS = [
    # "net_guru_flow_1q_pct",
    # "net_guru_flow_4q_pct",
    "guru_buy_ratio_1q",
    "guru_buy_ratio_4q",
]

_JOIN_KEYS = ["sector", "company_name", "primary_ticker", "primary_exchange"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:

    # Step 1 — fetch price signals
    price_df = get_universe_price_stats(_DUMMY_UNIVERSE)

    print("\nRaw price momentum signals per company\n")
    print(price_df.to_string(index=False))

    # Step 2 — fetch smart money signals
    sm_raw_df = get_universe_smart_money_stats(_DUMMY_UNIVERSE)
    sm_df = sm_raw_df[_JOIN_KEYS + _SMART_MONEY_KEEP_COLS]

    print("\nRaw smart money signals per company\n")
    print(sm_df.to_string(index=False))

    # Step 3 — join price + smart money into unified company-level df
    company_df = price_df.merge(sm_df, on=_JOIN_KEYS, how="left")

    # Step 4 — define equal weights for price signals
    price_signal_weights = {
        "above_200ma": 1,
        "above_50ma": 1,
        "ma_50_above_200": 1,
        "mom_1m": 1,
        "mom_3m": 1,
        "mom_6m": 1,
        "mom_12_1": 1,
        "positive_months_6m": 1,
        "52w_high_pct": 1,
        "rsi_14": 1,
    }

    smart_money_signal_weights = {
        # "net_guru_flow_1q_pct": 1,
        # "net_guru_flow_4q_pct": 1,
        "guru_buy_ratio_1q":    1,
        "guru_buy_ratio_4q":    1,
    }

    # Step 5 — compute per-pillar scores
    company_df = compute_weighted_score(
        company_df,
        weights=price_signal_weights,
        score_col="price_momentum_score",
    )
    company_df = compute_weighted_score(
        company_df,
        weights=smart_money_signal_weights,
        score_col="smart_money_score",
    )

    print("\nCompany-level scores\n")
    print(
        company_df[
            _JOIN_KEYS + ["price_momentum_score", "smart_money_score"]
        ].sort_values("price_momentum_score", ascending=False)
        .to_string(index=False)
    )

    # Step 6 — aggregate to sector
    sector_df = aggregate_to_sector(
        company_df,
        score_cols=["price_momentum_score", "smart_money_score"],
    )

    # Step 7 — compute final sector score
    sector_weights = {
        "price_momentum_score": 1,
        "smart_money_score":    1,
    }
    final_df = compute_weighted_score(
        sector_df,
        weights=sector_weights,
        score_col="final_sector_score",
    )

    print("\nFinal sector scores\n")
    print(
        final_df[["sector", "price_momentum_score", "smart_money_score", "final_sector_score"]]
        .sort_values("final_sector_score", ascending=False)
        .to_string(index=False)
    )


if __name__ == "__main__":

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)
    pd.set_option("display.float_format", "{:.2f}".format)

    main()
"""
quick_insight/config/indicators.py
────────────────────────────────────────────────────────────────────────────
Static indicator configuration for GuruFocus ingestion.

Kept separate from Settings (which is env-driven) because this list is
a code-level decision, not a deployment-level one. Change it here and
every orchestrator / endpoint that imports it picks up the change.
"""
from __future__ import annotations
# ── Allowlist ────────────────────────────────────────────────────────────────
# Only these indicator keys are fetched from the GuruFocus API.
# Commented-out keys are preserved for easy re-activation.

INDICATOR_ALLOWLIST: list[str] = [
    # --- Price / valuation backbone ---
    "price",
    "volume",
    "mktcap",
    # "enterprise_value",

    # --- Income statement / profitability ---
    "revenue",
    "net_income",
    "gross_margin",
    "net_margin",
    # "operating_income",
    # "ebitda",

    # --- Per-share metrics ---
    "eps_basic",
    "earning_per_share_diluted",
    "free_cash_flow_per_share",

    # --- Cashflow reconstruction ---
    "free_cash_flow",
    "cash_flow_from_operations",
    "capital_expenditure",

    # --- Balance sheet reconstruction ---
    # "total_debt",
    # "cash_and_cash_equivalents",
    # "total_stockholders_equity",

    # --- Returns / quality ---
    "roe",
    "roic",
    "interest_coverage",

    # --- Estimates / forward metrics ---
    "revenue_estimate",
    "forward_pe_ratio",
    "peg_ratio",

    # --- Owner earnings / GF specific ---
    # "owner_earnings",
    # "price_to_owner_earnings",
    "gf_value",
    "price_to_gf_value",

    # --- Yield style metrics ---
    "fcf_yield",

    # =========================================================
    # RANKING A — Price Trend
    # (200MA computed from raw price history, not indicators)
    # volatility used for position sizing flags
    # =========================================================
    # "volatility_1m",        # 1-month volatility — position sizing + extended flag
    # "volatility",           # 1-year volatility — normalisation baseline

    # =========================================================
    # RANKING B — Smart Money
    # insider buy/sell counts + volume for cluster detection
    # guru buy/sell for premium guru signal
    # institutional flow from 13F filings
    # short interest for covering/building signal
    # =========================================================
    "insider_buy",                  # count of insider buys
    "insider_sell",                 # count of insider sells
    "insider_buy_volume",           # share volume of insider buys
    "insider_sells_volume",         # share volume of insider sells
    "guru_buy",                     # number of premium guru buys
    "guru_sell",                    # number of premium guru sells
    "guru_buy_volume",              # shares of premium guru buys
    "guru_sell_volume",             # shares of premium guru sells
    # "plus_guru_buy",                # PremiumPlus guru buys (higher conviction)
    # "plus_guru_sell",               # PremiumPlus guru sells
    # "institutional_guru_buy_pct",   # % of 13F filers that are net buyers
    # "institutional_guru_sell_pct",  # % of 13F filers that are net sellers
    # "premium_guru_buy_pct",         # % of premium gurus buying
    # "premium_guru_sell_pct",        # % of premium gurus selling
    # "ShortInterest",                # short interest — falling = shorts covering = tailwind

    # =========================================================
    # RANKING C — Expectations Momentum
    # all forward/estimate metrics — we snapshot monthly and
    # compare to 30/60/90 days ago to get direction of change
    # absolute level is less important than the trend
    # =========================================================
    "per_share_eps_estimate",           # EPS with estimate — direction of revision
    "eps_nri_estimate",                 # EPS ex-NRI with estimate — cleaner earnings
    "ebit_estimate",                    # EBIT with estimate
    "ebitda_estimate",                  # EBITDA with estimate
    "pe_ntm",                           # NTM PE — rolls forward cleanly, no fiscal year jumps
    "ps_ntm",                           # NTM PS ratio
    "enterprise_value_to_ebitda_ntm",   # NTM EV/EBITDA — preferred over trailing
    "earning_growth_5y_est",            # 3-5Y forward EPS growth estimate
    "ocf_yield",                        # OCF yield — complements FCF yield
    "revenue_estimate",         # revenue with estimate — direction of revision
    # "rate_of_return_value",     # Yacktman forward rate of return

    # =========================================================
    # RANKING D — Media Narrative
    # no indicators needed — driven by news headlines API + LLM
    # =========================================================

    # =========================================================
    # RISK FLAGS — not used in ranking scores
    # used only to trigger ⚠️ flags in the human review report
    # =========================================================
    # "zscore",       # Altman Z-Score — financial distress warning
    # "fscore",       # Piotroski F-Score — fundamental deterioration
    # "mscore",       # Beneish M-Score — earnings manipulation risk
    # # "rank_momentum",        # GF momentum rank — sanity check vs own calculation
    # "rank_profitability",   # GF profitability rank — quality drift detection
    # "plus_guru_buy_pct",        # % of PremiumPlus gurus buying
    # "plus_guru_sell_pct",       # % of PremiumPlus gurus selling
]
# ── Keys that should only ever be fetched without a period type ──────────────
# These are market/real-time series where quarterly/annual variants don't exist.
MARKET_ONLY_KEYS: frozenset[str] = frozenset({
    "price",
    "volume",
    "volatility_1m",
    "volatility",
    "mktcap",
    "enterprise_value",
    "shares_outstanding",
    "insti_owner",
    "ins_owner",
    "gf_score",
    "rank_balancesheet",
    "rank_profitability",
    "rank_gf_value",
    "rank_growth",
    "rank_momentum",
    "rank_predictability",
    "ShortInterest",
    "short_interest",
})

# ── Default period types to request (in priority order) ─────────────────────
# For non-market keys: try quarterly first, then fall back to no-type.
DEFAULT_PERIOD_TYPES: tuple[str, ...] = ("q",)
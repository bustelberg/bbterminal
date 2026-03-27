# src\quick_insight\ai_momentum\signals\smart_money.py

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd

from quick_insight.config.config import settings
from quick_insight.db import fetch_df
from quick_insight.ingest.gurufocus.stock_indicator.orchestrate import (
    orchestrate_indicator,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_REQUIRED_UNIVERSE_COLS = {
    "company_name",
    "primary_ticker",
    "primary_exchange",
}

_GURU_BUY_VOLUME_KEY = "guru_buy_volume"
_GURU_SELL_VOLUME_KEY = "guru_sell_volume"
_INSIDER_BUY_KEY = "insider_buy"


def _metric_code_for_indicator_key(indicator_key: str) -> str:
    return f"indicator_{indicator_key}"


def _ensure_indicator_in_db(
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
) -> None:
    """
    Ensure a single GuruFocus indicator is present in the DB for this company.

    Uses cache=True so this prefers already downloaded indicator data and only
    loads that single indicator through the normal stock-indicator pipeline.
    """
    orchestrate_indicator(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=indicator_key,
        use_cache=True,
    )


def ensure_universe_smart_money_metrics_in_db(
    universe_df: pd.DataFrame,
) -> None:
    """
    Serially ensure the required smart-money indicators are present in DB for
    every company in the universe.

    This avoids DuckDB catalog write conflicts when universe-level smart-money
    stats are computed in parallel threads.
    """
    missing = _REQUIRED_UNIVERSE_COLS - set(universe_df.columns)
    if missing:
        raise ValueError(
            "universe_df is missing required columns: "
            + ", ".join(sorted(missing))
        )

    seen: set[tuple[str, str]] = set()

    for _, row in universe_df.iterrows():
        key = (row["primary_ticker"], row["primary_exchange"])
        if key in seen:
            continue
        seen.add(key)

        _ensure_indicator_in_db(
            primary_ticker=row["primary_ticker"],
            primary_exchange=row["primary_exchange"],
            indicator_key=_GURU_BUY_VOLUME_KEY,
        )
        _ensure_indicator_in_db(
            primary_ticker=row["primary_ticker"],
            primary_exchange=row["primary_exchange"],
            indicator_key=_GURU_SELL_VOLUME_KEY,
        )
        _ensure_indicator_in_db(
            primary_ticker=row["primary_ticker"],
            primary_exchange=row["primary_exchange"],
            indicator_key=_INSIDER_BUY_KEY,
        )


def _query_indicator_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_indicator_in_db: bool = True,
) -> pd.Series:
    """
    Pull an indicator timeseries from DuckDB for a single company.

    Returns a date-indexed float Series named after the indicator key,
    sorted ascending.
    """
    db_path = db_path or settings.db_path

    if ensure_indicator_in_db:
        _ensure_indicator_in_db(
            primary_ticker=primary_ticker,
            primary_exchange=primary_exchange,
            indicator_key=indicator_key,
        )

    sql = """
        SELECT
            s.target_date   AS date,
            fn.metric_value AS value
        FROM facts_number fn
        JOIN company  c  ON c.company_id  = fn.company_id
        JOIN metric   m  ON m.metric_id   = fn.metric_id
        JOIN snapshot s  ON s.snapshot_id = fn.snapshot_id
        WHERE c.primary_ticker   = ?
          AND c.primary_exchange = ?
          AND m.metric_code      = ?
          {date_filters}
        ORDER BY s.target_date
    """

    params: list = [
        primary_ticker,
        primary_exchange,
        _metric_code_for_indicator_key(indicator_key),
    ]
    date_filters: list[str] = []

    if start is not None:
        date_filters.append("AND s.target_date >= ?")
        params.append(str(pd.Timestamp(start).date()))

    if end is not None:
        date_filters.append("AND s.target_date <= ?")
        params.append(str(pd.Timestamp(end).date()))

    sql = sql.format(date_filters="\n          ".join(date_filters))

    df = fetch_df(db_path, sql, params)

    if df.empty:
        return pd.Series(dtype="float64", name=indicator_key)

    df["date"] = pd.to_datetime(df["date"])

    return pd.Series(
        df["value"].values,
        index=pd.DatetimeIndex(df["date"]),
        name=indicator_key,
        dtype="float64",
    ).sort_index()


def get_indicator_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    indicator_key: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_indicator_in_db: bool = True,
) -> pd.Series:
    """
    Public accessor — returns a date-indexed indicator Series for a company.
    """
    return _query_indicator_timeseries(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=indicator_key,
        start=start,
        end=end,
        db_path=db_path,
        ensure_indicator_in_db=ensure_indicator_in_db,
    )


# ---------------------------------------------------------------------------
# Signal computation helpers
# ---------------------------------------------------------------------------

def _sum_last_n_months(
    series: pd.Series,
    n_months: int,
) -> float | None:
    """
    Sum all observations whose date falls within the trailing N-month window.

    This is appropriate for event-like / flow-like metrics such as guru buy and
    sell volume, where we want cumulative activity over the window.
    """
    if series.empty:
        return None

    cutoff = series.index[-1] - pd.DateOffset(months=n_months)
    window = series[series.index > cutoff]

    if window.empty:
        return None

    return round(float(window.sum()), 2)


def _net_flow_pct(
    buy_series: pd.Series,
    sell_series: pd.Series,
    n_months: int,
) -> float | None:
    """
    Compute net guru flow % over a trailing window:

        (buy - sell) / (buy + sell)

    Returns None when there is no flow in the window.
    """
    buy_n = _sum_last_n_months(buy_series, n_months)
    sell_n = _sum_last_n_months(sell_series, n_months)

    buy_value = 0.0 if buy_n is None else float(buy_n)
    sell_value = 0.0 if sell_n is None else float(sell_n)
    total_flow = buy_value + sell_value

    if total_flow <= 0:
        return None

    return round((buy_value - sell_value) / total_flow, 4)


# ---------------------------------------------------------------------------
# Single-company smart-money signal computation
# ---------------------------------------------------------------------------

def get_smart_money_stats(
    primary_ticker: str,
    primary_exchange: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_smart_money_in_db: bool = True,
) -> dict:
    """
    Query guru flow history from DuckDB and compute raw smart-money signals.

    Returns
    -------
    dict with keys:
        guru_buy_volume_12m, guru_sell_volume_12m,
        guru_net_volume_12m,
        guru_net_flow_pct_24m, guru_net_flow_pct_12m,
        guru_net_flow_pct_6m, guru_net_flow_pct_3m,
        insider_buy_12m

    Interpretation
    --------------
    - guru_buy_volume_12m: total guru buy volume over the last 12 months
    - guru_sell_volume_12m: total guru sell volume over the last 12 months
    - guru_net_volume_12m: buy volume minus sell volume over the last 12 months
    - guru_net_flow_pct_*:
        (buy - sell) / (buy + sell), bounded between -1 and 1 when flow exists
    - insider_buy_12m:
        total insider buy count over the last 12 months

    Positive net flow % means guru buy volume exceeded guru sell volume over
    the window, suggesting net acquisition.
    Negative net flow % means guru sell volume exceeded guru buy volume,
    suggesting net distribution.
    """
    effective_start = start
    if effective_start is None:
        effective_start = (pd.Timestamp.today() - pd.DateOffset(months=30)).date()

    guru_buy_volume = get_indicator_timeseries(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=_GURU_BUY_VOLUME_KEY,
        start=effective_start,
        end=end,
        db_path=db_path,
        ensure_indicator_in_db=ensure_smart_money_in_db,
    )

    guru_sell_volume = get_indicator_timeseries(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=_GURU_SELL_VOLUME_KEY,
        start=effective_start,
        end=end,
        db_path=db_path,
        ensure_indicator_in_db=ensure_smart_money_in_db,
    )

    insider_buy = get_indicator_timeseries(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key=_INSIDER_BUY_KEY,
        start=effective_start,
        end=end,
        db_path=db_path,
        ensure_indicator_in_db=ensure_smart_money_in_db,
    )

    if guru_buy_volume.empty and guru_sell_volume.empty and insider_buy.empty:
        raise ValueError(
            f"No smart-money data in DB for {primary_exchange}:{primary_ticker} "
            f"in range [{effective_start}, {end}]."
        )

    buy_12m = _sum_last_n_months(guru_buy_volume, 12)
    sell_12m = _sum_last_n_months(guru_sell_volume, 12)
    insider_buy_12m = _sum_last_n_months(insider_buy, 12)

    buy_12m_value = 0.0 if buy_12m is None else float(buy_12m)
    sell_12m_value = 0.0 if sell_12m is None else float(sell_12m)
    insider_buy_12m_value = 0.0 if insider_buy_12m is None else float(insider_buy_12m)

    return {
        # Total guru buy volume over the last 12 months.
        # Higher means gurus cumulatively bought more shares over the window.
        "guru_buy_volume_12m": round(buy_12m_value, 2),

        # Total guru sell volume over the last 12 months.
        # Higher means gurus cumulatively sold more shares over the window.
        "guru_sell_volume_12m": round(sell_12m_value, 2),

        # Net guru volume over the last 12 months.
        # Positive means gurus were net acquirers of the stock;
        # negative means gurus were net sellers / distributors.
        "guru_net_volume_12m": round(buy_12m_value - sell_12m_value, 2),

        # Net guru flow as a share of total guru flow over the last 24 months.
        # +1.0 means all observed flow was buy-side, -1.0 means all observed
        # flow was sell-side, and 0.0 means buy/sell flow was balanced.
        "guru_net_flow_pct_24m": _net_flow_pct(guru_buy_volume, guru_sell_volume, 24),

        # Net guru flow as a share of total guru flow over the last 12 months.
        "guru_net_flow_pct_12m": _net_flow_pct(guru_buy_volume, guru_sell_volume, 12),

        # Net guru flow as a share of total guru flow over the last 6 months.
        "guru_net_flow_pct_6m": _net_flow_pct(guru_buy_volume, guru_sell_volume, 6),

        # Net guru flow as a share of total guru flow over the last 3 months.
        "guru_net_flow_pct_3m": _net_flow_pct(guru_buy_volume, guru_sell_volume, 3),

        # Total insider buy count over the last 12 months.
        # Rare but potentially meaningful when non-zero.
        "insider_buy_12m": round(insider_buy_12m_value, 2),
    }


# ---------------------------------------------------------------------------
# Universe-level function
# ---------------------------------------------------------------------------

def get_universe_smart_money_stats(
    universe_df: pd.DataFrame,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    max_workers: int = 8,
    raise_on_error: bool = False,
    ensure_smart_money_in_db: bool = True,
) -> pd.DataFrame:
    """
    Extend the passed universe dataframe with raw per-company smart-money
    signals.

    Default date logic
    ------------------
    - end   : first day of the current month
    - start : 2 years prior to that computed end date

    Required input columns
    ----------------------
    company_name, primary_ticker, primary_exchange
    """
    missing = _REQUIRED_UNIVERSE_COLS - set(universe_df.columns)
    if missing:
        raise ValueError(
            "universe_df is missing required columns: "
            + ", ".join(sorted(missing))
        )

    computed_end = pd.Timestamp.today().normalize().replace(day=1)

    end = computed_end.date() if end is None else pd.Timestamp(end).date()
    start = (
        (pd.Timestamp(end) - pd.DateOffset(years=3)).date()
        if start is None
        else pd.Timestamp(start).date()
    )

    base_df = universe_df.copy().reset_index(drop=True)

    if ensure_smart_money_in_db:
        ensure_universe_smart_money_metrics_in_db(base_df)
        ensure_smart_money_in_db = False

    signal_columns = [
        "guru_buy_volume_12m",
        "guru_sell_volume_12m",
        "guru_net_volume_12m",
        "guru_net_flow_pct_24m",
        "guru_net_flow_pct_12m",
        "guru_net_flow_pct_6m",
        "guru_net_flow_pct_3m",
        "insider_buy_12m",
        "smart_money_signal_error",
    ]

    _empty_signals: dict = {
        "guru_buy_volume_12m": pd.NA,
        "guru_sell_volume_12m": pd.NA,
        "guru_net_volume_12m": pd.NA,
        "guru_net_flow_pct_24m": pd.NA,
        "guru_net_flow_pct_12m": pd.NA,
        "guru_net_flow_pct_6m": pd.NA,
        "guru_net_flow_pct_3m": pd.NA,
        "insider_buy_12m": pd.NA,
    }

    def _fetch_one(idx: int, row: pd.Series) -> tuple[int, dict]:
        try:
            stats = get_smart_money_stats(
                primary_ticker=row["primary_ticker"],
                primary_exchange=row["primary_exchange"],
                start=start,
                end=end,
                db_path=db_path,
                ensure_smart_money_in_db=ensure_smart_money_in_db,
            )
            stats["smart_money_signal_error"] = None
            return idx, stats
        except Exception as exc:
            if raise_on_error:
                raise
            return idx, {**_empty_signals, "smart_money_signal_error": str(exc)}

    results: dict[int, dict] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_one, idx, row): idx
            for idx, row in base_df.iterrows()
        }
        for future in as_completed(futures):
            idx, result = future.result()
            results[idx] = result

    signal_df = pd.DataFrame.from_dict(results, orient="index").reindex(base_df.index)

    for col in signal_columns:
        if col not in signal_df.columns:
            signal_df[col] = pd.NA

    return pd.concat([base_df, signal_df[signal_columns]], axis=1)


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)
    pd.set_option("display.float_format", "{:.4f}".format)

    dummy_universe = pd.DataFrame([
        {
            "company_name": "Apple",
            "primary_ticker": "AAPL",
            "primary_exchange": "NASDAQ",
        },
        {
            "company_name": "Microsoft",
            "primary_ticker": "MSFT",
            "primary_exchange": "NASDAQ",
        },
        {
            "company_name": "NVIDIA",
            "primary_ticker": "NVDA",
            "primary_exchange": "NASDAQ",
        },
        {
            "company_name": "Hermes",
            "primary_ticker": "RMS",
            "primary_exchange": "XPAR",
        },
        {
            "company_name": "ASML",
            "primary_ticker": "ASML",
            "primary_exchange": "XAMS",
        },
    ])

    smart_money_df = get_universe_smart_money_stats(
        dummy_universe,
        max_workers=4,
        ensure_smart_money_in_db=True,
    )
    print(smart_money_df.to_string(index=False))
# src\quick_insight\ai_momentum\signals\price.py

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

_PRICE_METRIC_CODE = "indicator_price"


def _ensure_price_metric_in_db(
    primary_ticker: str,
    primary_exchange: str,
) -> None:
    """
    Ensure the GuruFocus price indicator is present in the DB for this company.

    Uses cache=True so this prefers already downloaded indicator data and only
    loads that single indicator through the normal stock-indicator pipeline.
    """
    orchestrate_indicator(
        primary_ticker=primary_ticker,
        primary_exchange=primary_exchange,
        indicator_key="price",
        use_cache=True,
    )


def ensure_universe_price_metrics_in_db(
    universe_df: pd.DataFrame,
) -> None:
    """
    Serially ensure the GuruFocus price indicator is present in DB for every
    company in the universe.

    This avoids DuckDB catalog write conflicts when universe-level price stats
    are computed in parallel threads.
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

        _ensure_price_metric_in_db(
            primary_ticker=row["primary_ticker"],
            primary_exchange=row["primary_exchange"],
        )


def _query_price_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_price_in_db: bool = True,
) -> pd.Series:
    """
    Pull price timeseries from DuckDB for a single company.

    Returns a date-indexed float Series named 'price', sorted ascending.
    """
    db_path = db_path or settings.db_path

    if ensure_price_in_db:
        _ensure_price_metric_in_db(
            primary_ticker=primary_ticker,
            primary_exchange=primary_exchange,
        )

    sql = """
        SELECT
            s.target_date   AS date,
            fn.metric_value AS price
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

    params: list = [primary_ticker, primary_exchange, _PRICE_METRIC_CODE]
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
        return pd.Series(dtype="float64", name="price")

    df["date"] = pd.to_datetime(df["date"])

    return pd.Series(
        df["price"].values,
        index=pd.DatetimeIndex(df["date"]),
        name="price",
        dtype="float64",
    ).sort_index()


def get_price_timeseries(
    primary_ticker: str,
    primary_exchange: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_price_in_db: bool = True,
) -> pd.Series:
    """
    Public accessor — returns a date-indexed price Series for a company.
    """
    return _query_price_timeseries(
        primary_ticker,
        primary_exchange,
        start=start,
        end=end,
        db_path=db_path,
        ensure_price_in_db=ensure_price_in_db,
    )


# ---------------------------------------------------------------------------
# Signal computation helpers
# ---------------------------------------------------------------------------

def _price_n_months_ago(series: pd.Series, n_months: int) -> float | None:
    if series.empty:
        return None
    cutoff = series.index[-1] - pd.DateOffset(months=n_months)
    past = series[series.index <= cutoff]
    return float(past.iloc[-1]) if not past.empty else None


def _mom_return(series: pd.Series, n_months: int) -> float | None:
    if series.empty:
        return None
    past_price = _price_n_months_ago(series, n_months)
    if past_price is None:
        return None
    return round((float(series.iloc[-1]) / past_price - 1) * 100, 2)


def _distance_from_ma_pct(series: pd.Series, window: int) -> float | None:
    if series.empty:
        return None

    ma = float(series.tail(window).mean()) if len(series) >= window else float(series.mean())
    if ma == 0:
        return None

    return round((float(series.iloc[-1]) / ma - 1) * 100, 2)


def _sma_slope_pct(
    series: pd.Series,
    *,
    window: int = 50,
    lookback_days: int = 20,
) -> float | None:
    """
    Slope proxy for the moving average:
    percent change in the rolling SMA vs the SMA `lookback_days` observations ago.
    """
    if series.empty:
        return None

    sma = series.rolling(
        window=window,
        min_periods=max(5, min(window, 20)),
    ).mean().dropna()

    if len(sma) <= lookback_days:
        return None

    current = float(sma.iloc[-1])
    past = float(sma.iloc[-(lookback_days + 1)])

    if past == 0:
        return None

    return round((current / past - 1) * 100, 2)


def _drawdown_from_recent_high_pct(
    series: pd.Series,
    lookback_days: int = 252,
) -> float | None:
    if series.empty:
        return None

    window = series.tail(lookback_days)
    if window.empty:
        return None

    recent_high = float(window.max())
    if recent_high == 0:
        return None

    return round((float(series.iloc[-1]) / recent_high - 1) * 100, 2)


def _annualized_volatility_pct(
    series: pd.Series,
    lookback_days: int = 126,
) -> float | None:
    if len(series) < 3:
        return None

    daily_returns = series.pct_change().dropna().tail(lookback_days)
    if len(daily_returns) < 2:
        return None

    vol = float(daily_returns.std())
    if pd.isna(vol):
        return None

    return round(vol * (252 ** 0.5) * 100, 2)


def _volatility_adjusted_return(
    series: pd.Series,
    *,
    n_months: int = 6,
    vol_lookback_days: int = 126,
) -> float | None:
    """
    Simple vol-adjusted return:
    n-month return (%) divided by annualized volatility (%).
    """
    ret = _mom_return(series, n_months)
    vol = _annualized_volatility_pct(series, lookback_days=vol_lookback_days)

    if ret is None or vol in (None, 0):
        return None

    return round(ret / vol, 4)


# ---------------------------------------------------------------------------
# Single-company price signal computation
# ---------------------------------------------------------------------------

def get_price_stats(
    primary_ticker: str,
    primary_exchange: str,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    ensure_price_in_db: bool = True,
) -> dict:
    """
    Query price history from DuckDB and compute raw price momentum signals.

    Returns
    -------
    dict with keys:
        mom_6m, mom_12_1, mom_12m, mom_3m, mom_1m,
        above_200ma, distance_from_200dma_pct, ma_50_above_200,
        ma_50_slope_20d_pct, drawdown_from_recent_high_pct,
        volatility_adjusted_return_6m
    """
    effective_start = start
    if effective_start is None:
        effective_start = (pd.Timestamp.today() - pd.DateOffset(months=18)).date()

    series = get_price_timeseries(
        primary_ticker,
        primary_exchange,
        start=effective_start,
        end=end,
        db_path=db_path,
        ensure_price_in_db=ensure_price_in_db,
    )

    if series.empty:
        raise ValueError(
            f"No price data in DB for {primary_exchange}:{primary_ticker} "
            f"in range [{effective_start}, {end}]."
        )

    price_now = float(series.iloc[-1])

    ma_200 = float(series.tail(200).mean()) if len(series) >= 200 else float(series.mean())
    ma_50 = float(series.tail(50).mean()) if len(series) >= 50 else float(series.mean())

    skip_last_month_cutoff = series.index[-1] - pd.DateOffset(months=1)
    series_skip_last_month = series[series.index <= skip_last_month_cutoff]
    price_12m_ago = _price_n_months_ago(series, 12)

    mom_12_1 = None
    if price_12m_ago is not None and not series_skip_last_month.empty:
        mom_12_1 = round(
            (float(series_skip_last_month.iloc[-1]) / price_12m_ago - 1) * 100,
            2,
        )

    return {
        # Total price return over the last 6 months.
        # Good medium-term trend measure and often one of the strongest momentum signals.
        "mom_6m": _mom_return(series, 6),

        # 12-month return excluding the most recent month.
        # Classic "12-1" momentum signal that reduces short-term reversal noise.
        "mom_12_1": mom_12_1,

        # Total price return over the last 12 months including the most recent month.
        # Broad long-term momentum measure.
        "mom_12m": _mom_return(series, 12),

        # Total price return over the last 3 months.
        # Useful shorter-term confirmation of trend acceleration or weakening.
        "mom_3m": _mom_return(series, 3),

        # Total price return over the last 1 month.
        # Very short-term momentum; informative but noisier than 3m/6m signals.
        "mom_1m": _mom_return(series, 1),

        # Binary trend flag: 1 if current price is above its 200-day moving average, else 0.
        # Simple long-term trend regime indicator.
        "above_200ma": 1 if price_now > ma_200 else 0,

        # Percent distance between current price and the 200-day moving average.
        # Shows how extended the stock is versus its long-term trend baseline.
        "distance_from_200dma_pct": _distance_from_ma_pct(series, 200),

        # Binary trend-structure flag: 1 if 50-day MA is above 200-day MA, else 0.
        # Often used to confirm a healthier medium-vs-long-term trend setup.
        "ma_50_above_200": 1 if ma_50 > ma_200 else 0,

        # Percent change in the 50-day moving average versus 20 trading days ago.
        # Measures whether the medium-term trend is strengthening or flattening.
        "ma_50_slope_20d_pct": _sma_slope_pct(series, window=50, lookback_days=20),

        # Current percent drawdown from the highest price in the recent lookback window.
        # Tells you how far the stock sits below its recent high.
        "drawdown_from_recent_high_pct": _drawdown_from_recent_high_pct(
            series,
            lookback_days=252,
        ),

        # 6-month return divided by annualized volatility.
        # Risk-adjusted momentum measure: rewards strong returns achieved with less volatility.
        "volatility_adjusted_return_6m": _volatility_adjusted_return(
            series,
            n_months=6,
            vol_lookback_days=126,
        ),
    }


# ---------------------------------------------------------------------------
# Universe-level function
# ---------------------------------------------------------------------------

def get_universe_price_stats(
    universe_df: pd.DataFrame,
    *,
    start: date | str | None = None,
    end: date | str | None = None,
    db_path: Path | str | None = None,
    max_workers: int = 8,
    raise_on_error: bool = False,
    ensure_price_in_db: bool = True,
) -> pd.DataFrame:
    """
    Extend the passed universe dataframe with raw per-company price signals.

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
        (pd.Timestamp(end) - pd.DateOffset(years=2)).date()
        if start is None
        else pd.Timestamp(start).date()
    )

    base_df = universe_df.copy().reset_index(drop=True)

    if ensure_price_in_db:
        ensure_universe_price_metrics_in_db(base_df)
        ensure_price_in_db = False

    signal_columns = [
        "mom_6m",
        "mom_12_1",
        "mom_12m",
        "mom_3m",
        "mom_1m",
        "above_200ma",
        "distance_from_200dma_pct",
        "ma_50_above_200",
        "ma_50_slope_20d_pct",
        "drawdown_from_recent_high_pct",
        "volatility_adjusted_return_6m",
        "price_signal_error",
    ]

    _empty_signals: dict = {
        "mom_6m": pd.NA,
        "mom_12_1": pd.NA,
        "mom_12m": pd.NA,
        "mom_3m": pd.NA,
        "mom_1m": pd.NA,
        "above_200ma": pd.NA,
        "distance_from_200dma_pct": pd.NA,
        "ma_50_above_200": pd.NA,
        "ma_50_slope_20d_pct": pd.NA,
        "drawdown_from_recent_high_pct": pd.NA,
        "volatility_adjusted_return_6m": pd.NA,
    }

    def _fetch_one(idx: int, row: pd.Series) -> tuple[int, dict]:
        try:
            stats = get_price_stats(
                primary_ticker=row["primary_ticker"],
                primary_exchange=row["primary_exchange"],
                start=start,
                end=end,
                db_path=db_path,
                ensure_price_in_db=ensure_price_in_db,
            )
            stats["price_signal_error"] = None
            return idx, stats
        except Exception as exc:
            if raise_on_error:
                raise
            return idx, {**_empty_signals, "price_signal_error": str(exc)}

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
    pd.set_option("display.float_format", "{:.2f}".format)

    dummy_universe = pd.DataFrame([
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

    price_df = get_universe_price_stats(
        dummy_universe,
        max_workers=4,
        ensure_price_in_db=True,
    )
    print(price_df.to_string(index=False))
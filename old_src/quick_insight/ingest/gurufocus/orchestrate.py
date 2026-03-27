# src/quick_insight/ingest/gurufocus/orchestrate.py
from __future__ import annotations

import time
from collections import defaultdict
from typing import Callable

from quick_insight.ingest.gurufocus.analyst_estimates.orchestrate import (
    orchestrate_analyst_estimates,
)
from quick_insight.ingest.gurufocus.financials.orchestrate import (
    orchestrate_financials,
)
from quick_insight.ingest.gurufocus.get_companies_from_db import (
    fetch_all_tickers_and_exchanges,
)
from quick_insight.ingest.gurufocus.stock_indicator.orchestrate import (
    orchestrate_indicator_allowlist,
)

Company = tuple[str, str]  # (ticker, exchange)


def _format_duration(seconds: float) -> str:
    total = int(round(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def _print_progress(i: int, total: int, t0: float, *, note: str = "") -> None:
    elapsed = time.perf_counter() - t0
    avg = elapsed / max(i, 1)
    eta_remaining = max(0.0, avg * total - elapsed)
    pct = (i / total) * 100 if total else 100.0

    msg = (
        f"[{i}/{total} | {pct:5.1f}%] "
        f"Elapsed: {_format_duration(elapsed)} | "
        f"ETA: {_format_duration(eta_remaining)}"
    )
    if note:
        msg += f" | {note}"
    print(msg)


def _run_step(
    step_name: str,
    func: Callable[[], object],
) -> tuple[bool, str, object]:
    try:
        result = func()

        if step_name == "analyst_estimates" and result is None:
            return (
                True,
                f"{step_name} skipped company (blocked exchange)",
                None,
            )

        return True, f"{step_name} ok", result

    except Exception as exc:
        return False, f"{step_name} failed: {exc}", None


def _clean_companies(
    companies: list[Company],
) -> list[Company]:
    """
    Remove rows with UNKNOWN exchange, but print them so they are visible.
    """
    cleaned: list[Company] = []

    for ticker, exchange in companies:
        exchange_upper = str(exchange).upper()
        ticker_str = str(ticker)

        if exchange_upper == "UNKNOWN":
            print(f"Skipping UNKNOWN exchange row: {ticker_str}:{exchange_upper}")
            continue

        cleaned.append((ticker_str, exchange_upper))

    return cleaned


def _build_companies_iter(
    companies: list[Company],
    *,
    only_first_round: bool = False,
) -> list[Company]:
    """
    Reorder companies so each round picks one company per exchange.

    Example:
        NASDAQ: AAPL, MSFT
        NYSE:   V, MA, SPGI
        XAMS:   ASML

    Output:
        round 1 -> AAPL, V, ASML
        round 2 -> MSFT, MA
        round 3 -> SPGI

    If only_first_round=True, only the first round is returned.
    """
    by_exchange: dict[str, list[str]] = defaultdict(list)

    for ticker, exchange in companies:
        by_exchange[exchange].append(ticker)

    ordered_exchanges = sorted(by_exchange.keys())
    max_rounds = 1 if only_first_round else max(
        (len(tickers) for tickers in by_exchange.values()),
        default=0,
    )

    result: list[Company] = []

    for round_idx in range(max_rounds):
        for exchange in ordered_exchanges:
            tickers = by_exchange[exchange]
            if round_idx < len(tickers):
                result.append((tickers[round_idx], exchange))

    return result


def main(
    *,
    companies_override: list[tuple[str, str]] | None = None,
    stop_on_error: bool = False,
    only_first_round: bool = False,
) -> None:
    t0 = time.perf_counter()

    if companies_override is not None:
        raw_companies = [
            (str(ticker), str(exchange).upper())
            for ticker, exchange in companies_override
        ]
    else:
        df = fetch_all_tickers_and_exchanges()
        raw_companies = [
            (str(row.primary_ticker), str(row.primary_exchange).upper())
            for row in df.itertuples()
        ]

    cleaned_companies = _clean_companies(raw_companies)
    companies_iter = _build_companies_iter(
        cleaned_companies,
        only_first_round=only_first_round,
    )

    total = len(companies_iter)

    ok_companies = 0
    failed_companies = 0
    skipped_companies = 0
    blocked_exchanges: set[str] = set()

    step_success_totals = {
        "analyst_estimates": 0,
        "financials": 0,
        "indicators": 0,
    }
    step_failure_totals = {
        "analyst_estimates": 0,
        "financials": 0,
        "indicators": 0,
    }

    for i, (ticker, exchange) in enumerate(companies_iter, start=1):
        label = f"{exchange}:{ticker}"

        if exchange in blocked_exchanges:
            skipped_companies += 1
            _print_progress(
                i,
                total,
                t0,
                note=f"SKIPPED {label} (exchange already blocked)",
            )
            continue

        print(f"\n=== [{i}/{total}] Starting {label} ===")

        company_failed = False

        success, message, result = _run_step(
            "analyst_estimates",
            lambda t=ticker, e=exchange: orchestrate_analyst_estimates(
                primary_ticker=t,
                primary_exchange=e,
            ),
        )
        print(f"  - {message}")

        if success:
            step_success_totals["analyst_estimates"] += 1
        else:
            step_failure_totals["analyst_estimates"] += 1
            company_failed = True

            if stop_on_error:
                _print_progress(
                    i,
                    total,
                    t0,
                    note=f"FAILED {label} at step=analyst_estimates",
                )
                return

        if result is None and success:
            blocked_exchanges.add(exchange)
            skipped_companies += 1
            _print_progress(
                i,
                total,
                t0,
                note=f"SKIPPED {label} (blocked exchange)",
            )
            continue

        remaining_steps: list[tuple[str, Callable[[], object]]] = [
            (
                "financials",
                lambda t=ticker, e=exchange: orchestrate_financials(
                    primary_ticker=t,
                    primary_exchange=e,
                ),
            ),
            (
                "indicators",
                lambda t=ticker, e=exchange: orchestrate_indicator_allowlist(
                    primary_ticker=t,
                    primary_exchange=e,
                ),
            ),
        ]

        for step_name, func in remaining_steps:
            success, message, _ = _run_step(step_name, func)
            print(f"  - {message}")

            if success:
                step_success_totals[step_name] += 1
            else:
                step_failure_totals[step_name] += 1
                company_failed = True

                if stop_on_error:
                    _print_progress(
                        i,
                        total,
                        t0,
                        note=f"FAILED {label} at step={step_name}",
                    )
                    return

        if company_failed:
            failed_companies += 1
            _print_progress(i, total, t0, note=f"FAILED {label}")
        else:
            ok_companies += 1
            _print_progress(i, total, t0, note=f"OK {label}")

    elapsed = time.perf_counter() - t0

    print("\n=== GuruFocus ingest summary ===")
    print(f"Companies attempted: {total}")
    print(f"OK companies:        {ok_companies}")
    print(f"Failed companies:    {failed_companies}")
    print(f"Skipped companies:   {skipped_companies}")
    print()
    print("Step successes:")
    print(f"  analyst_estimates: {step_success_totals['analyst_estimates']}")
    print(f"  financials:        {step_success_totals['financials']}")
    print(f"  indicators:        {step_success_totals['indicators']}")
    print()
    print("Step failures:")
    print(f"  analyst_estimates: {step_failure_totals['analyst_estimates']}")
    print(f"  financials:        {step_failure_totals['financials']}")
    print(f"  indicators:        {step_failure_totals['indicators']}")
    print()
    print(f"Elapsed:             {_format_duration(elapsed)}")


if __name__ == "__main__":
    # main(
    #     stop_on_error=False,
    #     only_first_round=True,
    #     companies_override=None,
    # )
    manual_companies: list[tuple[str, str]] = [
        # ("00388", "HKSE"),
        # ("00700", "HKSE"),
        # ("01093", "HKSE"),
        # ("AAPL", "NASDAQ"),
        # ("MSFT", "NASDAQ"),
        ("ASML", "XAMS"),
    ]

    main(
        stop_on_error=False,
        only_first_round=False,
        companies_override=manual_companies,
    )
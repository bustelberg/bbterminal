"""The SINGLE definition of a held basket's return.

Every "portfolio return" in the app must come from here — the engine computes
it once, stores it on the snapshot (`period_return_pct`) + on each holding
(`forward_return_pct`), and every surface (the /schedule header MTD, the
current-portfolio card, the run-history rows) DISPLAYS those stored values
rather than recomputing. That's what keeps them from ever disagreeing.

The two functions form the fine-grained → aggregate chain:

  holding_eur_return_pct(h)   one holding's EUR return = exit_eur/entry_eur − 1
  portfolio_eur_return_pct(hs) weighted EUR return = Σ wᵢ · holding_returnᵢ

A snapshot is internally consistent iff, for every holding,
`forward_return_pct == holding_eur_return_pct(h)` and
`period_return_pct == portfolio_eur_return_pct(holdings)`. Those invariants are
pinned in `tests/test_portfolio_math.py` so the single-source-of-truth can't
silently regress.
"""
from __future__ import annotations


def holding_eur_return_pct(h: dict) -> float | None:
    """A holding's EUR return since entry, in percent: `exit_eur/entry_eur − 1`.
    The atomic truth — both the company (metric_data) and ETF (benchmark_price)
    re-pricers must store `entry_price_eur`/`exit_price_eur` so this is well
    defined. None when the EUR marks aren't both present (an un-repriced ETF)."""
    entry = h.get("entry_price_eur")
    exit_ = h.get("exit_price_eur")
    if entry and exit_ and entry > 0:
        return (exit_ / entry - 1.0) * 100.0
    return None


def portfolio_eur_return_pct(holdings: list[dict]) -> float | None:
    """A basket's weighted EUR return — THE value stored as a snapshot's
    `period_return_pct`. Uses each holding's stored `forward_return_pct` (which
    IS its EUR return once the re-pricer has run) weighted by `weight`, so the
    aggregate is the exact weighted mean of the per-row returns the card shows.
    Holdings with no return (un-priced) are skipped; None when none have one."""
    rsum = 0.0
    wsum = 0.0
    for h in holdings:
        fr = h.get("forward_return_pct")
        if fr is None:
            continue
        w = float(h.get("weight") or 0.0)
        rsum += float(fr) * w
        wsum += w
    return (rsum / wsum) if wsum > 0 else None

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


# Cash sleeve: a synthetic holding with a flat 0% return (`company_id = 0`, the
# unused sentinel — real companies are positive, ETF overlays negative). Marked
# `is_cash` so re-pricers skip it and `apply_cash_allocation` can strip + re-add
# it idempotently. Its 0% return, weighted into `portfolio_eur_return_pct`, is
# exactly the cash drag on the basket.
CASH_COMPANY_ID = 0


def make_cash_holding(weight: float) -> dict:
    """One cash holding at `weight` (0..1). Flat 1.0 EUR marks → 0% return."""
    return {
        "company_id": CASH_COMPANY_ID,
        "is_cash": True,
        "ticker": "CASH",
        "company_name": "Cash",
        "sector": "Cash",
        "weight": float(weight),
        "forward_return_pct": 0.0,
        "entry_price_local": 1.0,
        "exit_price_local": 1.0,
        "entry_price_eur": 1.0,
        "exit_price_eur": 1.0,
        "currency": "EUR",
        "side": "long",
    }


def apply_cash_allocation(holdings: list[dict], cash_pct: float | None) -> list[dict]:
    """Return `holdings` with a `cash_pct` (0..1) cash sleeve applied: strip any
    existing cash, RENORMALIZE the remaining holdings to sum-to-1, scale them by
    `(1 - cash_pct)`, and append one flat 0%-return cash holding at `cash_pct` —
    so the weights still sum to ~1 and the reported return picks up the cash drag.

    The renormalize step is what makes this IDEMPOTENT: the stored holdings may
    already be scaled by a PRIOR cash % (the re-pricer reads a cash-baked
    rebalance snapshot), and re-scaling those directly would compound the shrink.
    Normalizing the stripped base back to sum-1 first recovers the true weights.
    `cash_pct` None/≤0 → the base re-normalized to sum-1 (fully invested), no
    cash holding."""
    base = [h for h in holdings if not h.get("is_cash")]
    pct = min(max(float(cash_pct or 0.0), 0.0), 1.0)
    wsum = sum(float(h.get("weight") or 0.0) for h in base)
    norm = (1.0 / wsum) if wsum > 0 else 1.0
    scale = (1.0 - pct) * norm
    out: list[dict] = []
    for h in base:
        nh = dict(h)
        nh["weight"] = float(h.get("weight") or 0.0) * scale
        out.append(nh)
    if pct > 0.0:
        out.append(make_cash_holding(pct))
    return out


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

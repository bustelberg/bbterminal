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


def split_book(holdings: list[dict]) -> tuple[list[dict], list[dict], float]:
    """Take a stored book apart into `(stocks, etfs, cash_pct)`.

    The engine-wide id convention IS the discriminator: a real company has a
    POSITIVE `company_id`, an ETF overlay sleeve a NEGATIVE one (`-benchmark_id`),
    and cash the sentinel 0 (`is_cash`). Reading the sleeves back out of the
    holdings — rather than off the config — is what lets a hand edit rebuild the
    book from what is actually held."""
    stocks = [h for h in holdings if not h.get("is_cash") and int(h.get("company_id") or 0) > 0]
    etfs = [h for h in holdings if not h.get("is_cash") and int(h.get("company_id") or 0) < 0]
    cash = sum(float(h.get("weight") or 0.0) for h in holdings if h.get("is_cash"))
    return stocks, etfs, cash


def apply_sleeves(
    stock_holdings: list[dict],
    etf_holdings: list[dict],
    cash_pct: float | None,
) -> list[dict]:
    """THE definition of how a scheduled strategy's book is weighted.

    Three sleeves, assembled in one place so a hand edit, a rebalance and the
    daily re-pricer cannot produce three different books:

        cash    `cash_pct` of the whole portfolio (0..1)
        ETFs    each `weight` is INVESTED-relative — a share of the non-cash
                book, the same convention `config.etf_overlay[].weight_pct/100`
                and the diversifier's normalization already use. Final weight is
                `weight × (1 − cash)`.
        stocks  whatever is left: `(1 − Σ etf) × (1 − cash)`, spread over the
                stock sleeve RENORMALIZED to sum-1.

    ⚠ THE RENORMALIZE IS THE WHOLE POINT, AND IT IS WHAT MAKES THIS IDEMPOTENT.
    The stored stock weights are already scaled by whatever cash + ETF sleeves
    were applied last time (0.7 × 0.9 = 0.63 of the book). Scaling THOSE by a new
    sleeve compounds the shrink — set 10% cash three times and the stocks quietly
    drain away. Normalizing the stock sleeve back to sum-1 first recovers the
    underlying strategy's own start weights, so every edit is computed from the
    strategy's selection rather than from the last edit's output.

    Pure — pricing the ETF sleeves is the caller's job (see
    `routers/_schedule_snapshots.apply_sleeves_to_snapshot`)."""
    cash = min(max(float(cash_pct or 0.0), 0.0), 1.0)
    invested = 1.0 - cash
    etf_total = sum(float(h.get("weight") or 0.0) for h in etf_holdings)
    # A book cannot be more than fully allocated. Clamping instead of raising
    # keeps a bad stored config from bricking a rebalance; the endpoint that
    # accepts hand input validates up front and refuses with a message.
    etf_total = min(max(etf_total, 0.0), 1.0)
    stock_sleeve = max(0.0, 1.0 - etf_total)

    wsum = sum(float(h.get("weight") or 0.0) for h in stock_holdings)
    norm = (1.0 / wsum) if wsum > 0 else 0.0
    out: list[dict] = []
    for h in stock_holdings:
        nh = dict(h)
        nh["weight"] = float(h.get("weight") or 0.0) * norm * stock_sleeve * invested
        out.append(nh)
    for h in etf_holdings:
        nh = dict(h)
        nh["weight"] = float(h.get("weight") or 0.0) * invested
        out.append(nh)
    if cash > 0.0:
        out.append(make_cash_holding(cash))
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

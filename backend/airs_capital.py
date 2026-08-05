"""ONE ledger of every position a book touched this year — held AND sold — on one denominator.

THE PROBLEM THIS SOLVES
    The positions table shows what a book HOLDS, weighted by each position's share of the held
    book. Those weights sum to 100% and the returns beside them sum to nothing in particular: a
    name sold in March is absent, so the list cannot add up to the year. Measured, that is not a
    rounding hole — 22.5% of BUS_Offensief_Dyn's year happened in positions it no longer holds.

⚠⚠ A 1-JANUARY WEIGHT IS THE WRONG FIX, AND AITopSelectie IS THE PROOF. Its equity positions were
    worth EUR 40,319 on 1 January against a EUR 1,000,000 opening capital, because the book opened
    the year in CASH and deployed it on 5 January. A start-weighted table would report it as 96%
    cash — true, and useless for "what drove the year", since the money was invested for 51 of 52
    weeks.

THE WEIGHT THAT WORKS: AVERAGE INVESTED CAPITAL (Modified Dietz)

        avg capital = value at 1 Jan  +  SUM( flow x (days still invested / days in period) )

    A buy on 5 January counts for ~97% of the year; a sale in February stops counting in February.
    It is defined for a SOLD position as readily as a held one, which is the whole point — it is
    the only weight both kinds of row can carry. Validated 2026-08-05 against each book's own
    opening capital:

        BUS_Offensief_Dyn      SUM avg capital 1,195,470   beginvermogen 1,197,811   ratio 0.998
        AITopSelectie OFF DYN  SUM avg capital 1,022,695   beginvermogen 1,000,000   ratio 1.023

    ⚠ THE RATIO IS REPORTED, NOT ASSUMED TO BE 1. Modified Dietz ignores the price path within a
    position (a flow is weighted by time, not by what the position did between flows), and the
    de-restatement below is its own approximation. `capital_coverage_ratio` is how a reader sees
    that rather than discovering it.

⚠ `Beginwaarde` IS RESTATED TO THE CURRENT QUANTITY, SO IT IS NOT THE 1-JANUARY VALUE. AIRS
    restates it so a purchase does not read as a gain: own 100 shares on 1 Jan, buy 50 in March,
    and Beginwaarde is 150 x the 1-Jan price. Measured, that is why AITopSelectie's rows claim
    EUR 1,006,881 of opening value against a book that opened at EUR 1,000,000. The 1-Jan price is
    recoverable — `Beginwaarde / quantity` — so the true opening value is

        opening = Beginwaarde x (quantity - bought + sold) / quantity

⚠ A SOLD-OUT POSITION'S OPENING VALUE IS `proceeds - Res. YtD`, WHICH IS `Kostprijs +
    Res. voorg. jr.` — cost plus everything earned in EARLIER years, i.e. last year's closing
    value. ⚠ BUT ONLY FOR SHARES HELD ON 1 JANUARY. For a parcel bought in February that same
    expression is simply its purchase cost, and counting it invents opening capital that did not
    exist: doing so naively moved BUS_Offensief_Dyn's gap from EUR 55,427 to EUR 377,776. The
    shares held at the open are `sold - bought`, so the value is scaled by that share — an
    APPROXIMATION (AIRS's own parcel matching is not published), and the aggregate ratio above is
    what keeps it honest.

⚠ CONTRIBUTION, NOT WEIGHT, IS WHAT ADDS UP. Every position's contribution is its EUR result over
    the book's own `beginvermogen`, and those sum to the book's year exactly. The weight is
    DESCRIPTIVE — how much of the year's capital a position occupied — and is normalised over the
    positions, so `contribution ~= weight x return` holds only approximately. The identity the
    table asserts is the contribution one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class Position:
    """One instrument's whole year: what it opened at, what it occupied, what it produced."""

    name: str
    held: bool = False
    # Value at the period's open, de-restated (held) or scaled from the sales (sold out).
    opening_eur: float = 0.0
    # Modified Dietz average capital. ⚠ Can come out slightly negative on odd data (a sale
    # recorded before its buy); clamped to 0 for the weight so a nonsense negative share cannot
    # appear, and the raw value is kept so it is inspectable.
    avg_capital_eur: float = 0.0
    weight_pct: float | None = None

    # ── What it produced, split by where the figure comes from.
    # Still-held P&L: AIRS's own current − restated Beginwaarde. Restatement is what makes this
    # the true P&L rather than the purchase showing up as a gain.
    held_result_eur: float = 0.0
    # Realised on sales, AIRS's own `Res. YtD` — never proceeds − cost (see `airs_transacties`).
    realised_result_eur: float = 0.0
    # Net dividend (gross + tax; the tax is already negative as AIRS books it).
    income_eur: float = 0.0

    bought_eur: float = 0.0
    sold_eur: float = 0.0
    sales: int = 0
    first_sale: str | None = None
    last_sale: str | None = None
    prior_year_eur: float = 0.0

    @property
    def result_eur(self) -> float:
        return round(self.held_result_eur + self.realised_result_eur + self.income_eur, 2)

    @property
    def closed_out(self) -> bool:
        """⚠ DECIDED BY ABSENCE FROM THE HOLDINGS, NOT BY HAVING SOLD. A sale is a realisation —
        most sold names are trims and are still held."""
        return not self.held and self.sales > 0


@dataclass
class Ledger:
    positions: list[Position] = field(default_factory=list)
    basis_eur: float | None = None          # the book's own `beginvermogen`
    total_result_eur: float = 0.0
    avg_capital_eur: float = 0.0
    # ⚠ SUM avg capital / beginvermogen. Reported, never assumed to be 1 — see the module note.
    capital_coverage_ratio: float | None = None
    days: int = 0


def _flow_weight(datum: str | None, start: date, end: date, days: int) -> float:
    """The fraction of the period a flow on `datum` was invested.

    ⚠ CLAMPED TO [0, 1]. A transaction dated outside the window (AIRS occasionally books a trade
    to a settlement date past the report's end) would otherwise contribute a weight above 1 or
    below 0, i.e. more capital than was ever invested, or negative capital.
    """
    if not datum or days <= 0:
        return 1.0
    try:
        d = date.fromisoformat(datum)
    except ValueError:
        return 1.0
    return min(1.0, max(0.0, (end - d).days / days))


def build_ledger(volk_rows: list[dict], trades: list, income_by_name: dict[str, float],
                 beginvermogen: float | None, period_start: date, period_end: date) -> Ledger:
    """Every position the book touched, with its average capital and its contribution.

    `volk_rows` are `airs_holding` rows (holding_name, quantity, start_value_eur,
    current_value_eur); `trades` are `airs_transacties.Trade`; `income_by_name` is net EUR
    dividend per AIRS `Fonds` — joined by NAME, EXACTLY, because none of these sheets carries an
    ISIN and both sides are AIRS strings truncated at the same width.
    """
    days = (period_end - period_start).days
    led = Ledger(basis_eur=beginvermogen, days=days)

    bought_qty: dict[str, float] = {}
    sold_qty: dict[str, float] = {}
    by_name: dict[str, Position] = {}

    def pos(name: str) -> Position:
        return by_name.setdefault(name, Position(name=name))

    # ── Pass 1: the trades, so quantities are known before the holdings are de-restated.
    for t in trades:
        p = pos(t.fonds)
        w = _flow_weight(t.datum, period_start, period_end, days)
        if t.kind == "buy":
            bought_qty[t.fonds] = bought_qty.get(t.fonds, 0.0) + t.quantity
            p.bought_eur = round(p.bought_eur + t.eur, 2)
            p.avg_capital_eur += t.eur * w
        else:
            sold_qty[t.fonds] = sold_qty.get(t.fonds, 0.0) + t.quantity
            p.sold_eur = round(p.sold_eur + t.eur, 2)
            p.sales += 1
            p.realised_result_eur = round(p.realised_result_eur + t.realised_ytd_eur, 2)
            p.prior_year_eur = round(p.prior_year_eur + t.prior_year_eur, 2)
            p.avg_capital_eur -= t.eur * w
            if t.datum:
                p.first_sale = t.datum if p.first_sale is None else min(p.first_sale, t.datum)
                p.last_sale = t.datum if p.last_sale is None else max(p.last_sale, t.datum)

    # ── Pass 2: the held positions, de-restated to what was actually owned at the open.
    for r in volk_rows:
        name = r.get("holding_name")
        if not name:
            continue
        p = pos(name)
        p.held = True
        qty = _f(r.get("quantity")) or 0.0
        start = _f(r.get("start_value_eur")) or 0.0
        cur = _f(r.get("current_value_eur"))
        if start and cur is not None:
            p.held_result_eur = round(cur - start, 2)
        if qty > 0 and start:
            # ⚠ THE DE-RESTATEMENT. Beginwaarde is qty_now x the 1-Jan price, so the 1-Jan price is
            # Beginwaarde/qty_now and the true opening value is that price x the qty actually held
            # on 1 January. Clamped at 0: a position whose buys exceed its current quantity plus
            # sales cannot have held a negative number of shares.
            qty_open = qty - bought_qty.get(name, 0.0) + sold_qty.get(name, 0.0)
            p.opening_eur = round(start * max(qty_open, 0.0) / qty, 2)
        p.avg_capital_eur += p.opening_eur

    # ── Pass 3: positions that are gone — their opening value comes from the sales.
    for name, p in by_name.items():
        if p.held or not p.sales:
            continue
        sq, bq = sold_qty.get(name, 0.0), bought_qty.get(name, 0.0)
        # `proceeds − Res. YtD` is the parcel's value at LAST YEAR'S CLOSE. ⚠ Only the shares held
        # on 1 January count; anything bought this year contributes a purchase cost, not opening
        # capital. AIRS does not publish its parcel matching, so the split is proportional to
        # quantity — an approximation, and `capital_coverage_ratio` is where it shows.
        share = (max(sq - bq, 0.0) / sq) if sq > 0 else 0.0
        p.opening_eur = round((p.sold_eur - p.realised_result_eur) * share, 2)
        p.avg_capital_eur += p.opening_eur

    # ── Income, attached to the name that earned it — including names no longer held. That is why
    # it is joined here rather than only over the holdings: a sold position's dividend is real and
    # belongs on its own row, not in a leftover bucket.
    for name, eur in (income_by_name or {}).items():
        pos(name).income_eur = round(eur, 2)

    led.positions = sorted(by_name.values(), key=lambda p: -abs(p.result_eur))
    led.total_result_eur = round(sum(p.result_eur for p in led.positions), 2)
    for p in led.positions:
        p.avg_capital_eur = round(p.avg_capital_eur, 2)
    # ⚠ The denominator is the POSITIVE average capital. A position with a negative one is a data
    # oddity (a sale weighted more heavily than the buy that supplied it); letting it shrink the
    # denominator would inflate every other row's weight.
    led.avg_capital_eur = round(sum(max(p.avg_capital_eur, 0.0) for p in led.positions), 2)
    if led.avg_capital_eur > 0:
        for p in led.positions:
            p.weight_pct = max(p.avg_capital_eur, 0.0) / led.avg_capital_eur * 100
    if beginvermogen:
        led.capital_coverage_ratio = led.avg_capital_eur / beginvermogen
    return led


def contribution_pct(p: Position, basis_eur: float | None) -> float | None:
    """This position's share of the book's YEAR, in points on the book's opening capital.

    ⚠ THE ONE COLUMN THAT ADDS UP. Weights are descriptive; contributions sum to the book's own
    return. None where there is no basis — a book with no `beginvermogen` has no denominator, and
    a 0 there would read as "this position did nothing".
    """
    return None if not basis_eur else p.result_eur / basis_eur * 100


def money_weighted_return_pct(p: Position) -> float | None:
    """The position's own return on the capital it actually occupied.

    ⚠ NOT COMPARABLE TO THE HOLDINGS TABLE'S `Return`, which is the instrument's price+income
    return over the window. This one divides by AVERAGE capital, so a name bought late in the year
    shows a larger percentage on the same euros — it answers "how hard did this money work", not
    "what did the instrument do". None on a position with no capital to divide by.
    """
    return None if p.avg_capital_eur <= 0 else p.result_eur / p.avg_capital_eur * 100


def _f(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None

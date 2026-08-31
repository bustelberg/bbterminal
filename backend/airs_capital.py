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
    # ── AIRS's OWN SPLIT of the held result into price and currency, both in EUR.
    #
    # ⚠⚠ THEY DECOMPOSE `held_result_eur` AND NOTHING ELSE, EXACTLY. `Fondsresultaat` +
    # `Valutaresultaat` = `current_value_eur - start_value_eur` on the Vermogensoverzicht, which is
    # the same subtraction `held_result_eur` is (measured 2026-08-31: the identity holds on 494 of
    # 518 holdings of the newest fleet snapshot, and the 24 exceptions are the `Effectenrekening`
    # cash rows, where AIRS reports both legs as 0 and the delta is deposits and withdrawals).
    #
    # ⚠ SO THE REALISED AND INCOME LEGS HAVE NO SPLIT AND NEVER WILL FROM HERE. The transacties
    # sheet has no currency column, and a dividend is booked in EUR. Anything reading these must
    # report the remainder rather than fold it into either leg — see `unsplit_result_eur`.
    #
    # ⚠ NONE, NOT 0.0. AIRS only began publishing the two columns on 2026-07-18 (the last snapshot
    # without them is 2026-07-16), so an older book has no split at all — and a 0 there would say
    # "the currency did nothing", which is a claim about a position rather than about our data.
    fund_result_eur: float | None = None
    fx_result_eur: float | None = None

    bought_eur: float = 0.0
    sold_eur: float = 0.0
    sales: int = 0
    first_sale: str | None = None
    last_sale: str | None = None
    prior_year_eur: float = 0.0
    # ⚠ Its share count moved for a reason we do not interpret, so no quantity arithmetic on it is
    # trustworthy. Its EUR result stays valid — a corporate action of this kind carries no money.
    capital_unknown: bool = False

    @property
    def result_eur(self) -> float:
        return round(self.held_result_eur + self.realised_result_eur + self.income_eur, 2)

    @property
    def unsplit_result_eur(self) -> float | None:
        """The part of `result_eur` AIRS's price/currency split does NOT cover — in practice the
        realised leg and the dividends.

        ⚠ IT IS DERIVED BY SUBTRACTION, not by adding the two legs it names, so it also absorbs any
        gap between AIRS's own split and our held result. A table showing `koers + valuta` beside a
        larger `Result` invites the reader to conclude one of them is wrong; showing what is left
        over answers it, and the three columns then sum to the figure beside them.

        ⚠ None WHEN THERE IS NO SPLIT AT ALL — a remainder is only meaningful against something.
        """
        if self.fund_result_eur is None and self.fx_result_eur is None:
            return None
        return round(self.result_eur - (self.fund_result_eur or 0.0)
                     - (self.fx_result_eur or 0.0), 2)

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


# ⚠ REAL SPLIT RATIOS ONLY, exactly as `_benchmark_index._split_adjust` does it. "Any small
# rational n/d" is dense enough to sit within a few percent of anything and would "correct" a
# genuine event into nothing; a whitelist cannot.
SPLIT_RATIOS = (2, 3, 4, 5, 6, 8, 10, 15, 20)
_RATIO_TOLERANCE = 0.01
# How far a pre-event trade's price may sit from the 1-Jan price once the ratio is divided out.
# ⚠ THIS IS THE TEST THAT SEPARATES A SPLIT FROM A TRANSFER, and it is not a fudge factor. On a
# split, both prices are the same economic price in different unit bases, so the quotient is
# 1 + whatever the stock did in between. On a TRANSFER-IN the prices share one basis while the
# quantity ratio is 10, so the quotient is ~0.1 — the stock would have had to fall 90%.
_MOVE_LO, _MOVE_HI = 0.5, 2.0


def detect_split(qty_now: float, deposited_qty: float, opening_price: float,
                 pre_event_prices: list[float]) -> float | None:
    """The ratio a `Tt = D` row represents, or None if it cannot be shown to be a split.

    ⚠ TWO INDEPENDENT COLUMNS MUST AGREE, WHICH IS WHY THIS IS A MEASUREMENT AND NOT A GUESS.
    `D` is *Deponering* — a DEPOSIT of securities (AIRS's own page says so). A split produces one;
    so does a transfer in from another custodian, and those need opposite handling: a split
    rescales every earlier quantity, a transfer leaves them alone. So:

      1. the QUANTITY ratio `qty_now / (qty_now − deposited)` must be a whitelisted split ratio;
      2. every pre-event trade's PRICE ratio, divided by that same quantity ratio, must land in a
         plausible price move.

    Measured 2026-08-05 on KLA-Tencor, in two different books with different share counts:

        BUS_Offensief   310 / (310 − 279) = 10.0000   implied move 1.074
        AITopSelectie   410 / (410 − 369) = 10.0000   implied moves 1.000, 1.185

    The 1.000 is the decisive one: a 5 January purchase at EXACTLY 10.0000× the 1 January price.
    A stock does not move 0.00% in four days and independently happen to be 10× — that is one
    price written in two unit bases, and the quantity column reached 10.0000 on its own.

    ⚠ RETURNS None RATHER THAN A BEST GUESS. Both gates must pass; a deposit that is not
    demonstrably a split leaves the position refused, which is what it was before this existed.
    """
    before = qty_now - deposited_qty
    if before <= 0 or qty_now <= 0 or opening_price <= 0 or not pre_event_prices:
        return None
    ratio = qty_now / before
    whole = min(SPLIT_RATIOS, key=lambda w: abs(w - ratio))
    if abs(whole - ratio) / whole > _RATIO_TOLERANCE:
        return None
    if not all(_MOVE_LO <= (px / opening_price) / ratio <= _MOVE_HI
               for px in pre_event_prices if px > 0):
        return None
    return ratio


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
                 beginvermogen: float | None, period_start: date, period_end: date,
                 unknown_names: set[str] | None = None,
                 splits: dict[str, float] | None = None) -> Ledger:
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
    # ⚠⚠ NAMES WHOSE SHARE COUNT MOVED FOR A REASON WE DO NOT INTERPRET — a corporate action, in
    # practice a split. Their quantity arithmetic CANNOT be trusted, because the trade quantities
    # and the holdings quantity are then on different bases and subtracting one from the other is
    # meaningless. Measured on KLA-Tencor, 2026: the book holds 310 shares POST-split, bought 14
    # PRE-split on 3 February (EUR 1,183/share against a Beginwaarde of EUR 110/share — 10.7x
    # apart), and a `D` row added 279 on 12 June. `qty_now - bought` gave 296 where the truth is
    # 170, so the opening value came out EUR 32,605 instead of EUR 18,725 and the money-weighted
    # return read +39.81% instead of +56.67%. Plausible, and wrong by seventeen points.
    #
    # ⚠ THE FIX IS TO REFUSE, NOT TO INFER. The ratio IS recoverable from the `D` row (310/31 = 10)
    # — and inferring it would mean deciding that `D` means "split", which is precisely what this
    # codebase has declined to do until somebody measures one (see `airs_transacties`). A figure
    # withheld with a reason costs one cell; a figure quietly rescaled by a guessed ratio is the
    # kind nobody re-checks.
    unknown_action: set[str] = set(unknown_names or ())
    # ⚠ A PROVEN ratio per name — see `detect_split`. Only names in here are rescaled; a deposit
    # that could not be shown to be a split stays in `unknown_action` and stays refused.
    splits = dict(splits or {})

    def pos(name: str) -> Position:
        return by_name.setdefault(name, Position(name=name))

    # ── Pass 1: the trades, so quantities are known before the holdings are de-restated.
    for t in trades:
        p = pos(t.fonds)
        # ⚠ `trades()` emits only buys and sells, so an uninterpreted type never reaches here —
        # which is exactly why it has to be flagged from the sheet instead. See `unknown_names`.
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
        # ⚠ CARRIED, NOT COMPUTED. This is AIRS's own arithmetic on its own figures; deriving a
        # currency leg from a price series and an FX series would be a second answer to a question
        # the source already answers, and the two would differ on the day a holding traded.
        if r.get("fund_result_eur") is not None or r.get("fx_result_eur") is not None:
            p.fund_result_eur = _f(r.get("fund_result_eur"))
            p.fx_result_eur = _f(r.get("fx_result_eur"))
        # ⚠ RESCALE, THEN DE-RESTATE. A proven split means every quantity traded BEFORE it is in
        # the old basis; multiplying those by the ratio puts the whole position on today's basis,
        # after which the ordinary de-restatement below is valid again. The EUR flows are untouched
        # — money is unit-invariant, and only the share counts were ever ambiguous.
        split = splits.get(name)
        if split:
            bought_qty[name] = bought_qty.get(name, 0.0) * split
            sold_qty[name] = sold_qty.get(name, 0.0) * split
        if name in unknown_action and not split:
            # ⚠ REFUSED, NOT ESTIMATED. Its share count moved for a reason we do not interpret, so
            # `qty_now − bought` subtracts quantities on two different bases. `None` propagates to
            # the UI as a blank cell with a reason; the euro columns beside it are unaffected,
            # because a corporate action of this kind carries no money.
            p.capital_unknown = True
        elif qty > 0 and start:
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
    # ⚠ A REFUSED POSITION LEAVES BOTH SIDES. Keeping its (untrustworthy) capital in the
    # denominator would spread its error across every other row's weight.
    led.avg_capital_eur = round(sum(max(p.avg_capital_eur, 0.0)
                                    for p in led.positions if not p.capital_unknown), 2)
    if led.avg_capital_eur > 0:
        for p in led.positions:
            if p.capital_unknown:
                continue
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
    if p.capital_unknown or p.avg_capital_eur <= 0:
        return None
    return p.result_eur / p.avg_capital_eur * 100


def _f(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None

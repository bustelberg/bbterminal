"""Did the trading help? — one position's year, split into "doing nothing" and "each decision".

WHY THIS EXISTS
    The holdings table shows two returns for a position and they disagree: `Instrument return` is
    what the INSTRUMENT did (AIRS's Beginwaarde restated to today's quantity, so your timing is
    erased on purpose), and `Money-weighted` is what YOUR money did (Modified Dietz, so timing is
    the whole point). Measured, KLA-Tencor is +54.37% as an instrument and +56.67% on the money.
    The gap says trading helped, and says nothing about WHICH trade or by how much.

    ⚠ `Instrument return` IS DELIBERATELY NOT CALLED A TIME-WEIGHTED RETURN. It erases timing with
    the same INTENT as a TWR, but it does not chain sub-period returns — it restates the opening
    quantity, which carries the bias the next warning describes. A true TWR does not have it.

THE DECOMPOSITION, AND IT IS EXACT

        actual result = buy-and-hold  +  SUM(effect of each trade)

    where the counterfactual is the position you actually held on 1 January, held unchanged:

        buy-and-hold   = qty_open x (price_now - price_open)
        a BUY  of q at p   ->  q x (price_now - p)     it rose after you bought  -> gain
        a SELL of q at p   ->  q x (p - price_now)     it fell after you sold    -> gain

    The identity is not approximate and not asserted — it drops straight out of the algebra:

        actual - buyhold = (qty_now - qty_open) x price_now - SUM(buys) + SUM(sells)
                         = SUM q_buy x (price_now - p_buy) + SUM q_sell x (p_sell - price_now)

    Measured 2026-08-05, residual 0.00 on every position tried:

        KLA-Tencor      buyhold +10,129  trades +7,196  = +17,325   actual +17,325
        Adobe Systems   buyhold  -2,615  trades +3,028  =    +413   actual    +413
        ASML Holding    buyhold +34,174  trades      0  = +34,174   actual +34,174

    Adobe is the case that shows why this is worth having: buy-and-hold LOST money, and two
    correctly-timed decisions turned it positive.

⚠⚠ THE "ACTUAL" HERE IS THE ECONOMIC RESULT AND IT IS NOT THE `Result` COLUMN. That column is
    AIRS's restated figure — `Huidige waarde - Beginwaarde`, where Beginwaarde prices TODAY's share
    count at the 1 January price. For a position bought into during the year that values the new
    shares at January's price rather than what you paid, so it overstates by
    `q_bought x (p_buy - price_open)`: on KLA, EUR 1,146. Both numbers are correct answers to
    different questions, and at BOOK level the difference nets out against the cash line (which is
    why the table still reconciles to `beleggingsresultaat`). At POSITION level they differ, and
    this module says so rather than letting a reader find two results for one holding.

⚠ EVERYTHING IS IN EUR, so the currency leg is already inside every figure. A trade priced in USD
    and a value in EUR would decompose into nonsense; the Transacties sheet carries `Waarde EUR`
    for exactly this reason.

⚠ A SPLIT IS RESCALED BEFORE ANY OF THIS, never during. Pre-split quantities and prices are in the
    old basis, and `q x (price_now - p)` mixes bases silently if they are not converted first —
    the same defect that put 17 points on KLA's money-weighted return. `airs_capital.detect_split`
    proves the ratio; this module only consumes it.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TradeEffect:
    """One decision, and what it was worth against not having made it."""

    datum: str | None
    kind: str                      # 'buy' | 'sell'
    quantity: float                # in TODAY's share basis
    price_eur: float               # per share, EUR, in today's basis
    amount_eur: float              # what changed hands, always positive
    # ⚠ Against DOING NOTHING, not against a perfect decision. A buy gains if the price rose after
    # it; a sell gains if the price fell after it. There is no skill claim here — a lucky call and
    # a good one produce the same number, and this module does not pretend to tell them apart.
    effect_eur: float
    # ── The same effect as a RATE and as a WEIGHT. They answer different questions and a reader
    #    given only one will ask the other.
    #
    # `move_pct` — the effect per euro that changed hands, which reduces exactly to the price move
    # since the decision: a BUY is `price_now/p - 1` (what it made since you bought), a SELL is
    # `1 - price_now/p` (what it avoided since you sold). Signed so favourable is positive in both
    # directions. ⚠ It says how GOOD the decision was, and nothing about whether it mattered — a
    # brilliant call on 3 shares scores the same as one on 3,000.
    move_pct: float | None = None
    # `effect_pp` — the effect over the value of the position on 1 January, so it says how MUCH the
    # decision mattered. ⚠ Every line divided by that ONE denominator, which is what keeps the
    # decomposition exact in points as well as in euros:
    #     buy_hold_pct + SUM(effect_pp) = actual_pct
    # None where nothing was held at the open (`open_value_eur` is 0) — there is no base to be a
    # share of, and a 0 there would read as "this decision did not matter".
    effect_pp: float | None = None
    # True where the trade was in the pre-split basis and had to be converted.
    rescaled: bool = False


@dataclass
class TimingAnalysis:
    name: str
    qty_open: float = 0.0
    qty_now: float = 0.0
    price_open_eur: float = 0.0
    price_now_eur: float = 0.0
    buy_hold_eur: float = 0.0
    trades: list[TradeEffect] = field(default_factory=list)
    # The economic result — see the module warning: NOT the `Result` column.
    actual_eur: float = 0.0
    # ⚠ ASSERTED EVERY TIME. Three lines that do not add up are not a decomposition of anything.
    residual_eur: float = 0.0
    reconciles: bool = False
    # AIRS's restated result, and the gap to the economic one, so the modal can name it rather
    # than leave a reader with two numbers.
    airs_result_eur: float | None = None
    restatement_eur: float | None = None
    income_eur: float = 0.0
    # ── The one denominator every percentage here divides by: what the position you actually held
    #    on 1 January was worth. ⚠ NOT `start_value_eur`, which is AIRS's figure restated to
    #    TODAY's share count — dividing by that would give a "buy-and-hold return" for shares that
    #    were not held, which is the restatement bug wearing a percent sign.
    open_value_eur: float | None = None
    buy_hold_pct: float | None = None
    timing_pp: float | None = None
    # ⚠ THE MONEY'S RETURN ON THE CAPITAL IT STARTED WITH — it is NOT the `Money-weighted`
    # column, which is Modified Dietz over the TIME-WEIGHTED average capital. Same spirit, different
    # denominator, so they will differ where money went in mid-year; presenting either as the other
    # would put a third number on the screen claiming to be the second.
    actual_pct: float | None = None
    split_ratio: float | None = None
    # ⚠ THE WINDOW, CARRIED, so a timeline can place each decision on it. Without the bounds the
    # UI has to guess where "the start of the year" sits relative to the first trade, and a
    # timeline whose axis is inferred is a picture of an assumption.
    period_start: str | None = None
    period_end: str | None = None

    @property
    def timing_eur(self) -> float:
        """What all the trading was worth, together."""
        return round(sum(t.effect_eur for t in self.trades), 2)


RECONCILES_EUR = 0.5


def analyse_timing(name: str, qty_now: float, start_value_eur: float, current_value_eur: float,
                   trades: list, *, split_ratio: float | None = None,
                   split_date: str | None = None, income_eur: float = 0.0,
                   airs_result_eur: float | None = None,
                   period_start: str | None = None,
                   period_end: str | None = None) -> TimingAnalysis | None:
    """Split one held position's year into buy-and-hold plus per-trade effects.

    `trades` are `airs_transacties.Trade` for this instrument. Returns None where the position
    cannot be placed on one basis — no quantity, no opening value, or a corporate action whose
    ratio could not be proven (see `airs_capital.detect_split`).
    """
    if qty_now <= 0 or start_value_eur <= 0 or current_value_eur is None:
        return None
    price_open = start_value_eur / qty_now
    price_now = current_value_eur / qty_now
    a = TimingAnalysis(name=name, qty_now=qty_now, price_open_eur=price_open,
                       price_now_eur=price_now, income_eur=round(income_eur, 2),
                       airs_result_eur=airs_result_eur, split_ratio=split_ratio,
                       period_start=period_start, period_end=period_end)

    bought = sold = 0.0
    spent = received = 0.0
    for t in trades:
        if t.quantity <= 0 or t.eur <= 0:
            continue
        # ⚠ CONVERT FIRST. A pre-split trade is in the old basis; `q x (price_now - p)` would
        # otherwise compare a price ten times too large against a quantity ten times too small.
        # The EUR amount is unaffected — money does not have a share basis.
        rescaled = bool(split_ratio and split_date and (t.datum or "") < split_date)
        scale = split_ratio if rescaled else 1.0
        qty = t.quantity * (scale or 1.0)
        price = t.eur / qty
        if t.kind == "buy":
            bought += qty
            spent += t.eur
            effect = qty * (price_now - price)
        else:
            sold += qty
            received += t.eur
            effect = qty * (price - price_now)
        a.trades.append(TradeEffect(datum=t.datum, kind=t.kind, quantity=qty,
                                    price_eur=price, amount_eur=round(t.eur, 2),
                                    effect_eur=round(effect, 2), rescaled=rescaled,
                                    move_pct=round(effect / t.eur * 100, 2)))

    # ⚠ CLAMPED AT 0. Buys exceeding today's count plus sales would imply a negative holding on
    # 1 January, which is not a position — it is a data problem, and a negative counterfactual
    # would look like an answer.
    a.qty_open = max(qty_now - bought + sold, 0.0)
    a.buy_hold_eur = round(a.qty_open * (price_now - price_open), 2)
    # What the money actually did: today's value, less what was put in, plus what came out.
    a.actual_eur = round(current_value_eur - a.qty_open * price_open - spent + received, 2)
    a.residual_eur = round(a.buy_hold_eur + a.timing_eur - a.actual_eur, 2)
    a.reconciles = abs(a.residual_eur) < RECONCILES_EUR

    # ── The same three lines as percentages, over ONE base. Because it is one base, the identity
    #    carries through the division: buy_hold_pct + SUM(effect_pp) = actual_pct. Giving each line
    #    its own denominator is how a "decomposition" stops adding up.
    # ⚠ THE EUR IDENTITY IS EXACT; THE PERCENT ONE IS EXACT ONLY BEFORE ROUNDING. Three figures
    #    each rounded to 2dp can miss by a hundredth (Adobe: -8.43 + 9.77 = 1.34 against an actual
    #    of 1.33). `reconciles` is therefore asserted on the EUR line and never on this one — a
    #    tolerance loose enough to absorb rounding is loose enough to hide a real break.
    open_value = a.qty_open * price_open
    if open_value > 0:
        a.open_value_eur = round(open_value, 2)
        a.buy_hold_pct = round(a.buy_hold_eur / open_value * 100, 2)
        a.actual_pct = round(a.actual_eur / open_value * 100, 2)
        for tr in a.trades:
            tr.effect_pp = round(tr.effect_eur / open_value * 100, 2)
        a.timing_pp = round(sum(tr.effect_pp or 0.0 for tr in a.trades), 2)

    if airs_result_eur is not None:
        # ⚠ The restatement gap, named. AIRS prices the shares you bought later at JANUARY's price,
        # so its result exceeds the economic one by q_bought x (p_buy - price_open).
        a.restatement_eur = round((airs_result_eur - income_eur) - a.actual_eur, 2)
    return a

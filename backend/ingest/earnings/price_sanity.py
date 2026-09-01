"""Does the vendor's price history agree with our own? A test for a series that STOPPED MOVING.

⚠⚠ IT EXISTS BECAUSE A PAYLOAD-SHAPE HEURISTIC DOES NOT WORK, AND THAT IS MEASURED. The motivating
case is Diploma plc (`LSE:DPLM`): GuruFocus returned a complete statements payload whose
`Month End Stock Price` is **0 for 1998-2013** and then **frozen at 11.1 from 2016-09 to 2023-03**,
while the real share price went £8.79 to £28.10, before stepping 3.81x in one period. Two obvious
detectors were tried against the 1,782 companies that hold a price series, and both fail:

  * "mostly zeros" fires on **Alphabet (46%), CRH (52%), NetEase (77%), Sony Financial (95%)** —
    GuruFocus zero-fills the quarterly block for periods it does not publish, so zeros are normal.
  * "frozen for many periods" cannot be told apart from a genuine trading halt. The threshold that
    catches Diploma (14 frozen) also refuses **Nebius Group (11 frozen)**, whose price is
    legitimately flat through its ~2.5-year suspension.

⚠⚠ SO THE TEST IS AGREEMENT BETWEEN TWO INDEPENDENT SOURCES. We hold a yfinance series for 1,721 of
those companies through the ISIN bridge, and it is a genuinely separate observation of the same
security. A real halt makes BOTH series flat and passes; a vendor reporting a stale number while
the stock moved makes them disagree, which nothing about the payload alone could reveal.

⚠⚠ AND IT COMPARES **RETURNS**, NOT LEVELS — a level test was written first and had to be thrown
away, because it measured OUR defects as loudly as the vendor's. Two of them, both known:

  * **Our closes are not split-adjusted** (see the ⚠⚠ in CLAUDE.md — ingest only fetches dates
    newer than our stored max, so a vendor's retroactive split rewrite is never re-read), while
    GuruFocus's are. A level-ratio test therefore reported the SPLIT RATIO as a disagreement, and
    the first run flagged **3M, Rockwell Automation, Ciena, Reliance and Invesco** — none of them
    a vendor problem, several of them arguably ours.
  * **The vendor's history simply starts later than ours.** CRH showed "0 on 44 dates" ending in
    2009, STMicroelectronics 14 ending in 2001. That is coverage, not contradiction.

A return test is immune to both: one split is one bad period out of a hundred, and a period the
vendor does not cover has no return at all. What it still catches is the Diploma signature — the
vendor reporting NO CHANGE across a span in which the stock demonstrably moved — which is not
something a split or a coverage gap can produce.

⚠ MEASURED OVER THE WHOLE DATABASE: of the 1,721 companies holding both a vendor series and one of
ours, **exactly one is flagged** — Diploma, on 10 of its 25 comparable periods — and that stays
true at every `REAL_MOVE` from 3% to 15%. A detector with one true positive and no false ones is
worth having; the three it replaced each flagged dozens of healthy companies.

Pure: no database, no network. The caller supplies both series.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field

#: A vendor move at or below this is "the vendor says nothing happened".
#: ⚠ NOT ZERO. A frozen series repeats a value exactly, but rounding to two decimals on a large
#: price can leave a whisker of movement, and a rule that demanded an exact repeat would miss it.
FLAT_MOVE = 0.005

#: …while OUR series moved at least this much over the same span.
#:
#: ⚠⚠ SWEPT AGAINST THE WHOLE DATABASE, NOT CHOSEN. The first value tried was 20%, and it MISSED
#: THE ONE CASE THIS MODULE EXISTS FOR: these prints are semi-annual, and Diploma compounded at
#: roughly 8-10% per half-year, so almost none of its individual spans cleared 20% even though the
#: stock tripled across them. A threshold set by intuition about "a big move" was reasoning about
#: the wrong horizon.
#:
#: ⚠ THE SWEEP ALSO SHOWS THE RULE IS INSENSITIVE, WHICH IS THE REASSURING PART. Over 1,721
#: companies holding both series, the count tripping `> MAX_STALE_PERIODS` is **1 at every setting
#: from 3% to 15%** — always Diploma, never anything else — and Diploma's own stale count moves
#: 12 / 12 / 10 / 9 / 6 across that range. 8% sits in the middle with the true positive at 10 of 25
#: periods, well clear of the threshold, and no false positive anywhere in the range.
REAL_MOVE = 0.08

#: How many such periods before it is a finding rather than an oddity.
#:
#: ⚠ ONE IS NOT EVIDENCE — a single stale print happens, and our own series carries unadjusted
#: splits which can produce one. Diploma has ten.
MAX_STALE_PERIODS = 4


@dataclass
class Verdict:
    """`ok=False` means the vendor reported no movement across spans in which the stock moved."""
    ok: bool = True
    reason: str = ""
    #: Periods where both series had a usable, non-zero level at each end.
    compared: int = 0
    #: …of which the vendor was flat while we moved.
    stale: int = 0
    detail: list[str] = field(default_factory=list)


def _asof(ours: list[tuple[str, float]], when: str, _dates: list[str] | None = None) -> float | None:
    """Our last close at or before `when`.

    ⚠ AT OR BEFORE, NEVER THE NEAREST — a month-end print compared against a close from the
    following week would import a real price move as a disagreement, in a direction that depends on
    the month.

    ⚠ BISECTED, NOT SCANNED. `asset_price` holds ~5,900 bars per instrument and the audit asks ~50
    questions of each across 1,721 companies; a linear scan per lookup is ~500M string comparisons
    and turns a report into something nobody runs. ISO dates sort lexicographically, so the bisect
    needs no parsing.
    """
    dates = _dates if _dates is not None else [d for d, _ in ours]
    i = bisect_right(dates, when)
    return ours[i - 1][1] if i else None


def compare(vendor: list[tuple[str, float]],
            ours: list[tuple[str, float]],
            *, min_points: int = 6) -> Verdict:
    """Do these two price histories agree about when this stock moved?

    `vendor` and `ours` are `[(iso date, value)]`. Units need not match and need not be constant:
    only the RATIO of consecutive levels within each series is used, so a scale, a currency and a
    single split all cancel.

    ⚠ IT ABSTAINS RATHER THAN GUESSES. Too few comparable periods is `ok=True` WITH A REASON — "it
    is fine" and "I cannot tell" must not both be a silent pass, and judging on thin evidence would
    fire on every recent listing.
    """
    v = Verdict()
    if not vendor or not ours:
        v.reason = "no overlap - one of the two series is empty"
        return v
    ours = sorted(ours)

    # ⚠ ZEROS ARE DROPPED, NOT TREATED AS A PRICE. A zero is the vendor's way of saying "no figure
    # for this period"; used as a level it would manufacture a -100% return and then a +infinite one.
    pts = [(d, val) for d, val in sorted(vendor) if val]

    stale: list[str] = []
    for (d0, v0), (d1, v1) in zip(pts, pts[1:]):
        a, b = _asof(ours, d0), _asof(ours, d1)
        if a is None or b is None or a <= 0 or b <= 0:
            continue
        v.compared += 1
        vendor_move = abs(v1 / v0 - 1.0)
        our_move = abs(b / a - 1.0)
        if vendor_move <= FLAT_MOVE and our_move >= REAL_MOVE:
            stale.append(f"{d0} to {d1}: the vendor moved {vendor_move * 100:.1f}% "
                         f"while we moved {our_move * 100:.0f}%")

    v.stale = len(stale)
    v.detail = stale[:3]

    if v.compared < min_points:
        v.reason = f"only {v.compared} comparable periods - not enough to judge"
        return v
    if v.stale > MAX_STALE_PERIODS:
        v.ok = False
        v.reason = (f"the vendor reported no movement across {v.stale} of {v.compared} periods "
                    f"in which our own closes moved at least {REAL_MOVE:.0%}")
        return v
    v.reason = f"agree on {v.compared - v.stale} of {v.compared} periods"
    return v

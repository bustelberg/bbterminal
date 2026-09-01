"""Two independent price histories either agree about when a stock moved, or they do not.

⚠⚠ THE POINT OF THIS MODULE IS THAT THE OBVIOUS TESTS DO NOT WORK, and these cases pin why. The
motivating defect is Diploma plc: GuruFocus returned a full statements payload for `LSE:DPLM` — an
exchange outside our subscription — whose price column is 0 for fifteen years and then frozen for
seven while the stock tripled. Three detectors were tried against real data and discarded:

  * "mostly zeros" fires on Alphabet (46%), CRH (52%), NetEase (77%) — the vendor zero-fills
    quarters it does not publish, so zeros are ordinary.
  * "frozen for many periods" cannot be distinguished from a real trading halt: the threshold that
    catches Diploma also refuses Nebius Group, frozen through a genuine ~2.5-year suspension.
  * "the two series' LEVELS differ in shape" measures OUR OWN defects — our closes are not
    split-adjusted where the vendor's are, so the first live run flagged 3M, Rockwell, Ciena,
    Reliance and Invesco, none of them a vendor problem.

Hence: two sources, compared on RETURNS. `TestAHaltPasses` and `TestASplitPasses` are the two cases
that make the design worth its complexity — they are exactly what the discarded rules got wrong.

Unit-only: pure functions over lists, no database and no network.
"""
from __future__ import annotations

from ingest.earnings.price_sanity import MAX_STALE_PERIODS, compare


def series(start_year: int, values: list[float]) -> list[tuple[str, float]]:
    """A vendor print per year."""
    return [(f"{start_year + i}-06-30", v) for i, v in enumerate(values)]


def ours(start_year: int, values: list[float]) -> list[tuple[str, float]]:
    """Our own closes, dated just before each vendor print so the as-of lookup has something."""
    return [(f"{start_year + i}-06-29", v) for i, v in enumerate(values)]


RISING = [10, 12, 15, 18, 22, 26, 31, 37, 44, 53]


class TestTheHappyCase:
    def test_two_sources_that_agree_pass(self):
        v = compare(series(2010, RISING), ours(2010, RISING))
        assert v.ok, v.reason
        assert v.stale == 0

    def test_a_CONSTANT_SCALE_DIFFERENCE_IS_NOT_A_DISAGREEMENT(self):
        """⚠⚠ GuruFocus quotes Diploma in GBP where we hold GBp — a flat 100x — and an ADR trades at
        a fixed multiple of its ordinary. Only ratios within each series are used, so any constant
        cancels and none of those hundreds of companies is a finding."""
        for scale in (100.0, 0.01, 3.0):
            v = compare(series(2010, [x * scale for x in RISING]), ours(2010, RISING))
            assert v.ok, f"scale {scale}: {v.reason}"
            assert v.stale == 0


class TestTheDiplomaCase:
    def test_a_frozen_vendor_series_against_a_moving_one_fails(self):
        """The real shape: the stock more than triples while the vendor reports no change at all."""
        v = compare(series(2010, [11.1] * 10), ours(2010, RISING))
        assert not v.ok
        assert "no movement" in v.reason
        assert v.stale > MAX_STALE_PERIODS

    def test_the_evidence_names_the_periods(self):
        """⚠ A VERDICT WITHOUT DATES CANNOT BE CHECKED. The reader has to be able to open the two
        series at the span named and see it."""
        v = compare(series(2010, [11.1] * 10), ours(2010, RISING))
        assert v.detail and "2010-06-30 to 2011-06-30" in v.detail[0]

    def test_leading_vendor_zeros_are_not_counted_as_movement(self):
        """⚠ A ZERO IS "NO FIGURE", NOT A PRICE. Used as a level it would manufacture a -100% return
        and then an infinite one, so the run of zeros would itself look like wild disagreement —
        and every company whose vendor history starts later than ours would be flagged. That is the
        false positive that hit CRH (44 dates) and STMicroelectronics (14) on the first live run."""
        v = compare(series(2000, [0.0] * 10) + series(2010, RISING), ours(2000, list(range(1, 21))))
        assert v.ok, v.reason


class TestAHaltPasses:
    """⚠⚠ THE CASE A SHAPE-ONLY RULE GETS WRONG, AND THE REASON THIS TAKES TWO SERIES. Nebius
    Group's price is legitimately frozen through its suspension — and so is OURS, because the stock
    genuinely did not trade. Two sources flat TOGETHER agree, and agreement is the question."""

    def test_both_sources_frozen_together_is_agreement(self):
        flat = [20, 20, 20, 20, 20, 20, 25, 30, 36, 43]
        v = compare(series(2010, flat), ours(2010, flat))
        assert v.ok, v.reason
        assert v.stale == 0


class TestASplitPasses:
    """⚠⚠ THE CASE THE **LEVEL** RULE GOT WRONG. Our closes are not split-adjusted (ingest only
    fetches dates newer than our stored max, so a vendor's retroactive rewrite is never re-read)
    while the vendor's are — so at a split our series steps and theirs does not. On returns that is
    ONE bad period out of many, which is why the threshold is a count and not a single event."""

    def test_one_split_in_our_series_is_not_a_finding(self):
        vendor_side = [10, 12, 15, 18, 22, 26, 31, 37, 44, 53]
        # our series halves at the 2-for-1 and carries on
        our_side = [10, 12, 15, 18, 11, 13, 15.5, 18.5, 22, 26.5]
        v = compare(series(2010, vendor_side), ours(2010, our_side))
        assert v.ok, v.reason

    def test_one_stale_print_is_not_a_finding(self):
        vendor_side = [10, 12, 12, 18, 22, 26, 31, 37, 44, 53]
        v = compare(series(2010, vendor_side), ours(2010, RISING))
        assert v.ok, v.reason
        assert v.stale == 1


class TestItAbstainsRatherThanGuessing:
    def test_too_few_comparable_periods(self):
        """⚠ "FINE" AND "CANNOT TELL" MUST NOT BOTH BE A SILENT PASS — a young listing has almost no
        overlap, and judging on that would fire on every recent IPO."""
        v = compare(series(2020, [10, 11, 12]), ours(2020, [10, 11, 12]))
        assert v.ok
        assert "not enough" in v.reason

    def test_an_empty_series_either_side(self):
        assert compare([], ours(2010, RISING)).reason.startswith("no overlap")
        assert compare(series(2010, RISING), []).reason.startswith("no overlap")

    def test_a_vendor_period_before_our_history_is_skipped(self):
        """⚠ OUR OWN START DATE IS NOT EVIDENCE ABOUT THE VENDOR."""
        v = compare(series(1990, RISING) + series(2010, RISING), ours(2010, RISING))
        assert v.compared < len(RISING) * 2
        assert v.ok, v.reason


class TestTheAsOfRule:
    def test_our_close_is_taken_at_or_before_the_vendor_date(self):
        """⚠ NEVER THE NEAREST. A month-end print compared against the following week's close would
        import a real price move as a disagreement, in a direction that depends on the month."""
        v = compare(series(2010, RISING),
                    [("2010-07-15", 999.0)] + ours(2010, RISING))
        assert v.ok, v.reason

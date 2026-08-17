"""The order companies are fetched in — least recently checked first, ties broken at random.

⚠⚠ THE PROBLEM IS ABOUT RUNS THAT DO NOT FINISH. The work list came out in `company_id` order, so a
press that is cancelled — or capped by `limit`, or killed by a deploy — always chewed through the
same front of the list. Press it three times for five minutes each and you have fetched the same
opening slice three times and never reached the tail: every call the `*_fetched_at` stamps saved on
repeat presses was being spent again on re-treading known ground.

⚠ RANDOM WAS THE ASK AND THIS IS STRICTLY BETTER AT IT. Shuffling gives coverage `N(1-(1-m/N)^k)`
after k partial runs — it approaches everything and never arrives, and two consecutive presses still
overlap by chance. Ordering on "when did we last look" makes the frontier ADVANCE, because a company
just fetched is stamped and sorts to the back: full coverage in `ceil(N/m)` presses, zero overlap.
Same rule `ingest/phases/prices.py` already uses for the price refresh.
"""
from __future__ import annotations

import random

from routers._fundamental_fill import order_work

FIXED = random.Random(0)      # deterministic tie-breaks, so the assertions are about the ORDER


def _co(cid: int, *, fin=None, est=None, ind=None, needs=("fin", "est", "ind")) -> dict:
    row = {"company_id": cid, "company_name": f"Co {cid}",
           "financials_fetched_at": fin, "estimates_fetched_at": est,
           "indicators_fetched_at": ind}
    row.update({f"need_{k}": (k in needs) for k in ("fin", "est", "ind")})
    return row


def _ids(rows):
    return [c["company_id"] for c in rows]


class TestOldestFirst:

    def test_the_least_recently_checked_leads(self):
        work = [_co(1, fin="2026-08-17T09:00:00"),
                _co(2, fin="2026-06-01T09:00:00"),
                _co(3, fin="2026-07-15T09:00:00")]
        assert _ids(order_work(work, FIXED)) == [2, 3, 1]

    def test_never_checked_comes_before_everything(self):
        """⚠ A MISSING STAMP SORTS FIRST BY BEING THE EMPTY STRING — these are ISO timestamps, so
        lexical order IS chronological and `""` precedes every real one. No special case needed."""
        work = [_co(1, fin="2020-01-01T00:00:00"), _co(2, fin=None), _co(3, fin="2019-01-01")]
        assert _ids(order_work(work, FIXED))[0] == 2

    def test_a_finished_run_pushes_its_companies_to_the_back(self):
        """The property the whole thing exists for: fetch the front, stamp it, and the NEXT press
        starts where this one stopped rather than at the same place again."""
        work = [_co(i, fin=f"2026-08-0{i}T00:00:00") for i in (1, 2, 3, 4)]
        first = _ids(order_work(work, FIXED))[:2]
        assert first == [1, 2]
        # …those two are now stamped today; re-order the same list.
        done = {1, 2}
        work2 = [_co(c["company_id"],
                     fin="2026-08-17T12:00:00" if c["company_id"] in done
                         else c["financials_fetched_at"])
                 for c in work]
        assert _ids(order_work(work2, FIXED))[:2] == [3, 4], (
            "the second press re-fetched what the first one had just done")


class TestItRanksOnTheFeedsTHISRunWillFetch:
    """⚠ A company whose statements are due but whose estimates were checked an hour ago must be
    ranked on the STATEMENTS stamp — the other one is not what this press is about."""

    def test_a_feed_we_are_not_fetching_is_ignored(self):
        # Co 1 is fetching statements only, and its statements stamp is ancient.
        old_fin = _co(1, fin="2020-01-01T00:00:00", est="2026-08-17T09:00:00", needs=("fin",))
        # Co 2 is fetching estimates only, and its estimates stamp is recent.
        new_est = _co(2, fin="2019-01-01T00:00:00", est="2026-08-17T09:00:00", needs=("est",))
        assert _ids(order_work([new_est, old_fin], FIXED)) == [1, 2]

    def test_an_absent_flag_counts_as_fetch_it(self):
        """`ingest_company` reads `c.get(flag, True)` — an unprobed feed means "fetch it", so it has
        to weigh in the ordering too, or a forced run (which sets no flags) would order on nothing."""
        bare = {"company_id": 9, "financials_fetched_at": None,
                "estimates_fetched_at": None, "indicators_fetched_at": None}
        recent = _co(1, fin="2026-08-17", est="2026-08-17", ind="2026-08-17")
        assert _ids(order_work([recent, bare], FIXED))[0] == 9


class TestTheRandomTieBreak:
    """⚠ NOT DECORATION. Every never-asked company has the same key, and a company that FAILS is
    never stamped — so without jitter the failures, and anything the vendor has no answer for, would
    sit at the identical front position press after press. It is what stops a deterministic order
    from becoming a deterministic rut."""

    def test_equal_keys_do_not_come_out_in_the_same_order_every_time(self):
        work = [_co(i) for i in range(1, 21)]        # all never-asked -> all tied
        seen = {tuple(_ids(order_work(work, random.Random(s)))) for s in range(6)}
        assert len(seen) > 1, "tied companies came out in one fixed order on every run"

    def test_the_tie_break_never_outranks_the_stamp(self):
        """Jitter must reorder EQUALS only — a company checked today must not jump ahead of one last
        checked in 2019 because a random draw went its way."""
        work = [_co(1, fin="2026-08-17T09:00:00"), _co(2, fin="2019-01-01T00:00:00")]
        for seed in range(25):
            assert _ids(order_work(work, random.Random(seed))) == [2, 1]


class TestItIsANonDestructiveReordering:

    def test_every_company_survives_exactly_once(self):
        work = [_co(i, fin=None if i % 3 else f"2026-08-{i:02d}") for i in range(1, 16)]
        out = order_work(work, FIXED)
        assert sorted(_ids(out)) == sorted(_ids(work))
        assert len(out) == len(work)

    def test_an_empty_list_is_fine(self):
        assert order_work([], FIXED) == []

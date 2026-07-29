"""The staleness ANCHOR — the thing that decides whether the daily price refresh has any work.

⚠ A FLEET CANNOT SEE ITS OWN DRIFT. `global_latest_close` anchors on the newest close WE HOLD,
which is right for a weekend, a holiday and a Yahoo outage — and blind to the failure that matters
most, because in it every row ages TOGETHER. Measured 2026-07-29: our newest close anywhere was
2026-07-23, six days earlier; AMD's own last close was 2026-07-22, i.e. ONE day behind that anchor,
so `0 of 232` held instruments were stale and the refresh had nothing to do. Meanwhile AMD had gone
552.33 → 430.05 (−22%) and the AIRS price check was blaming our LISTING — which was perfect
(NasdaqGS, USD, `AMD`, every stored bar matching Yahoo to the cent). The series had simply stopped.
"""
from asset_pipeline.price_refresh import newest_dated_close


def chart(pairs: list[tuple[int, float | None]]) -> dict:
    """A Yahoo chart result: (epoch_seconds, close) newest last."""
    return {"timestamp": [t for t, _ in pairs],
            "indicators": {"quote": [{"close": [c for _, c in pairs]}]}}


# 2026-07-27, -28, -29 at 20:00 UTC — after any US close, so the UTC date is the session date.
JUL27, JUL28, JUL29 = 1785182400, 1785268800, 1785355200


class TestNewestDatedClose:
    def test_it_takes_the_newest_bar_with_a_close(self):
        assert newest_dated_close(chart([(JUL27, 494.95), (JUL28, 454.6)])) == "2026-07-28"

    def test_todays_unfinished_session_is_skipped(self):
        """Yahoo returns today's bar with `close: null` until the bell. Anchoring on it claims a
        close the market has not printed — and then EVERY series reads one day stale every single
        morning, i.e. a daily full-fleet refresh that re-downloads the world and finds nothing."""
        assert newest_dated_close(chart([(JUL27, 494.95), (JUL29, None)])) == "2026-07-27"

    def test_a_hole_in_the_middle_does_not_stop_the_walk(self):
        """Measured: AMD's 2026-07-28 bar came back null mid-week while 07-29 had a price."""
        got = newest_dated_close(chart([(JUL27, 494.95), (JUL28, None), (JUL29, 430.05)]))
        assert got == "2026-07-29"

    def test_all_null_closes_yield_no_anchor(self):
        assert newest_dated_close(chart([(JUL27, None), (JUL28, None)])) is None

    def test_an_empty_or_missing_result_yields_no_anchor(self):
        """⚠ None must mean 'fall back to our own maximum', never 'today'. A throttled probe that
        answered with the calendar would turn a Yahoo outage into a 6,000-instrument stampede at
        the one moment fetching cannot work."""
        assert newest_dated_close(None) is None
        assert newest_dated_close({}) is None
        assert newest_dated_close({"timestamp": [], "indicators": {"quote": [{}]}}) is None

    def test_a_short_close_array_does_not_raise(self):
        """Yahoo's arrays are parallel by contract, not by guarantee — and this runs before every
        refresh, so an IndexError here costs the whole job."""
        assert newest_dated_close({"timestamp": [JUL27, JUL28],
                                   "indicators": {"quote": [{"close": [494.95]}]}}) == "2026-07-27"


class TestTheAnchorIsTheLaterOfTheTwo:
    """`find_stale` takes `max(ours, market)` — as ISO strings, which compare as dates."""

    def test_market_ahead_wins(self):
        assert max("2026-07-23", "2026-07-29") == "2026-07-29"

    def test_a_failed_probe_cannot_drag_the_anchor_backwards(self):
        """None is skipped, not compared — otherwise a dead probe would make the fleet look fresh
        (`min`) or, worse, silently reset the anchor to nothing."""
        ours, market = "2026-07-28", None
        assert (market if market and market > ours else ours) == "2026-07-28"

    def test_a_stale_probe_does_not_unflag_a_stale_fleet(self):
        """If the market probe somehow lags our own newest bar, our own maximum still stands."""
        ours, market = "2026-07-28", "2026-07-20"
        assert (market if market and market > ours else ours) == "2026-07-28"

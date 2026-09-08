"""`_warn_once` — the reason the earnings log stopped repeating itself.

⚠ THE POINT IS NOT VOLUME FOR ITS OWN SAKE. A log nobody reads is worth the same as no log, and
these lines were drowning the scheduler's own output: `_blend_prewarm` walks ~1,500 ACWI
constituents across 12 endpoints on startup and after every fundamentals write, re-detecting and
re-announcing the same level shifts every time.
"""
from __future__ import annotations

import logging

import routers.earnings as E


def _fresh():
    E._WARNED.clear()


class TestItSpeaksOnce:
    def test_the_first_call_logs(self, caplog):
        _fresh()
        with caplog.at_level(logging.WARNING):
            E._warn_once("k", "[earnings] %s happened", "thing")
        assert "thing happened" in caplog.text

    def test_the_second_call_with_the_same_key_says_nothing(self, caplog):
        """⚠ THE WHOLE FEATURE. Same company, same metric, next request — the fact has not changed,
        so repeating it only pushes something else off the screen."""
        _fresh()
        with caplog.at_level(logging.WARNING):
            E._warn_once("k", "[earnings] %s happened", "thing")
            # ⚠ caplog collects for the WHOLE test, not just this block — without the clear the
            # first call's record is still there and the assertion passes for the wrong reason.
            caplog.clear()
            E._warn_once("k", "[earnings] %s happened", "thing")
        assert caplog.text == ""

    def test_a_different_key_still_speaks(self, caplog):
        """⚠ IT DEDUPLICATES, IT DOES NOT SILENCE. A second company with a real level shift is a
        different fact and must still reach the terminal."""
        _fresh()
        with caplog.at_level(logging.WARNING):
            E._warn_once("a", "[earnings] first")
            caplog.clear()
            E._warn_once("b", "[earnings] second")
        assert "second" in caplog.text

    def test_it_stays_at_WARNING(self, caplog):
        """⚠ NOT DOWNGRADED TO DEBUG — that was the other way to make the noise stop, and it makes
        the signal stop too: uvicorn leaves the root logger at WARNING, so a debug line is
        invisible in exactly the terminal this was cleaning up. A kept level shift WILL step an
        index and somebody should see it, once."""
        _fresh()
        with caplog.at_level(logging.DEBUG):
            E._warn_once("k", "[earnings] noisy")
        assert [r.levelno for r in caplog.records] == [logging.WARNING]


class TestTheCallSites:
    def test_the_three_noisy_ones_are_converted(self):
        """⚠ Pinned on source because the alternative is driving a blend, which needs a database.
        These are the three lines that were repeating; a fourth appearing later should be a
        deliberate choice, not a copy-paste of `_log.warning`."""
        import inspect  # noqa: PLC0415

        src = inspect.getsource(E)
        assert src.count("_warn_once(") >= 4          # the def + three call sites
        # The exact messages that were flooding the terminal must no longer go straight to _log.
        assert '_log.warning("[earnings] %s: level shift kept' not in src
        assert '_log.warning("[earnings] no TTM rule' not in src

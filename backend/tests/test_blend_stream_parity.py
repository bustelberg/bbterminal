"""The SSE blend and the plain blend must reach `_blend_rows` with the SAME inputs.

⚠⚠ THE DEFECT THIS PINS PUT TWO CONSTRUCTIONS IN ONE CHART. `_blend_metrics_events` called
`_blend_rows(rows, covered, None, cadence)` — no caps, no EUR totals — while
`/fundamental-blend-metrics` built both. On /management-dashboard's Long Equity tab the BOOK is
loaded SSE-first (`blendMetrics.ts` streams for per-holding progress) and the ACWI BENCHMARK beside
it is a plain POST, so the book drew the averaged growth chain and the index drew the euro
aggregate, in the same chart, with nothing on screen saying so. On ACWI FCF the two constructions
differ by ~11.5pp/yr (+19.1% averaged against +7.56% summed) — a gap large enough to read as alpha.

⚠ AND IT MADE THE BOOK'S OWN LINE DEPEND ON WHETHER SSE WORKED: the stream's fallback is the plain
POST, which aggregates. Exactly the failure `_blend_metrics_events` already documents for the
CADENCE, one construction over — a second copy of "what the blend needs" is how it came back.

⚠⚠ IT IS ASSERTED ON THE CALL, NOT ON THE SOURCE TEXT. The test that let the AIRS valuation-memo
outage run was `"…clear()" in inspect.getsource(...)`: green throughout, because it could only ever
confirm that ONE caller did the right thing, which is the precise shape of this bug too. So both
endpoints are DRIVEN here and what `_blend_rows` received is compared.

⚠ `asyncio.run`, NOT `pytest.mark.asyncio` — this suite has no pytest-asyncio, and the async
generators are driven directly the way `test_sse_stream.py` drives its own.

Pure — every read is stubbed; no DB, no network.
"""
from __future__ import annotations

import asyncio

import pytest

_COVERED = [{"company_id": 1, "weight_pct": 60.0, "name": "Alpha"},
            {"company_id": 2, "weight_pct": 40.0, "name": "Beta"}]
_COV = {"rows": [], "covered_pct": 100.0}

#: Stand-ins for the two things the streaming path used to drop. IDENTITY is what is asserted —
#: the point is that both endpoints hand `_blend_rows` whatever `_blend_extras` returned, not that
#: they compute the same numbers twice.
_CAPS = {"sentinel": "caps"}
_TOTALS = {"annuals__Income Statement__Revenue": {1: {"2024-12-31": 1.0}}}


@pytest.fixture
def earnings(monkeypatch):
    """`routers.earnings` with every read stubbed, recording what reached `_blend_rows`."""
    from routers import earnings as e

    calls: list[dict] = []
    extras: list[list] = []

    async def _inputs(_body):
        return list(_COVERED), dict(_COV)

    def _extras(body, _covered, metrics):
        extras.append([body.cadence, list(metrics)])
        return _CAPS, _TOTALS

    def _rows(_rows, _covered, caps=None, cadence="annual", totals=None):
        calls.append({"caps": caps, "cadence": cadence, "totals": totals})
        return {"metrics": [], "blend_notes": {}}

    monkeypatch.setattr(e, "_blend_inputs", _inputs)
    monkeypatch.setattr(e, "_blend_extras", _extras)
    monkeypatch.setattr(e, "_blend_rows", _rows)
    monkeypatch.setattr(e, "_company_metric_rows", lambda _cid: [])
    monkeypatch.setattr(e, "_ttm_metric_rows", lambda _cid: [])
    monkeypatch.setattr(e, "_ltm_blend_rows", lambda _ids, _m, _c: [])
    monkeypatch.setattr(e, "_bulk_blend_rows", lambda _ids, _m, _c: [])
    # ⚠ THROUGH `monkeypatch` LIKE EVERYTHING ELSE — a bare `e._recorded = …` on a module the whole
    # suite imports would outlive the test and be read by the next one.
    monkeypatch.setattr(e, "_recorded", calls, raising=False)
    monkeypatch.setattr(e, "_recorded_extras", extras, raising=False)
    return e


def _body(earnings, **kw):
    return earnings.FundamentalCoverageRequest(
        holdings=[{"isin": "US0000000001", "weight": 60.0}], **kw)


def _stream(earnings, body) -> list[str]:
    """Drain the SSE generator to its last frame."""
    async def run():
        return [frame async for frame in earnings._blend_metrics_events(body)]
    return asyncio.run(run())


def _plain(earnings, body):
    # ⚠ `request=None` IS FINE AND IS NOT A SHORTCUT: `cached_blend` caches only a UNIVERSE request
    # (`cache_key` returns None for a book), so a holdings body reaches the endpoint function
    # untouched — which is also how `scripts/profile_longequity_bench.py` calls it.
    return asyncio.run(earnings.fundamental_blend_metrics(body, None))


class TestTheStreamBlendsWhatThePlainEndpointBlends:
    def test_the_stream_passes_the_totals_through(self, earnings):
        """The regression itself: `None` here IS the averaged growth chain."""
        frames = _stream(earnings, _body(earnings))
        assert any("result" in f for f in frames)          # it really ran to a payload
        assert earnings._recorded[-1]["totals"] is _TOTALS
        assert earnings._recorded[-1]["caps"] is _CAPS

    def test_both_endpoints_reach_blend_rows_with_the_same_inputs(self, earnings):
        """⚠ A BOOK LOADS OVER WHICHEVER PATH WORKS, so the two cannot answer differently."""
        body = _body(earnings)
        _plain(earnings, body)
        _stream(earnings, body)
        plain, stream = earnings._recorded
        assert (plain["caps"], plain["totals"]) == (stream["caps"], stream["totals"])
        assert plain["cadence"] == stream["cadence"]

    def test_the_cadence_travels_too(self, earnings):
        """The same class of bug, fixed earlier and re-asserted beside its sibling."""
        _stream(earnings, _body(earnings, cadence="quarterly"))
        assert earnings._recorded[-1]["cadence"] == "quarterly"
        assert earnings._recorded_extras[-1][0] == "quarterly"


class TestTheTotalsCoverWhatWasActuallyRead:
    """⚠⚠ THE STREAM READS EVERY CODE REGARDLESS OF `metrics`, so it must ask for EVERY metric's
    euros. Handing it `body.metrics` would leave the codes it read but did not name on the growth
    chain, in the same response as ones on the aggregate — the same defect at a smaller scale.
    `[]` means "every aggregatable metric" to `_totals_for`.
    """

    def test_the_stream_asks_for_every_metric_even_when_narrowed(self, earnings):
        _stream(earnings, _body(earnings, metrics=["revenue"]))
        assert earnings._recorded_extras[-1][1] == []

    def test_the_plain_endpoint_asks_for_exactly_what_it_reads(self, earnings):
        """It narrows the READ to `metrics`, so it narrows the euros to match."""
        _plain(earnings, _body(earnings, metrics=["revenue"]))
        assert earnings._recorded_extras[-1][1] == ["revenue"]

"""AIRS portfolio scraper + AT&T performance Excel parser.

Endpoints:
    GET  /api/airs/portfolios               portfolios we already have data for (DB-served)
    GET  /api/airs/scan                     SSE: live Playwright scan of AirSPMS
    GET  /api/airs/portfolio/{name}         performance rows (DB cache or fresh download)
    POST /api/portfolios/parse              parse an uploaded AIRS Excel without persisting

`/api/portfolios/parse` is the drag-and-drop path on the frontend; the
other three back the broker-scan flow.
"""

from __future__ import annotations

import asyncio
import io
from routers import _airs_portfolio_store as store
from routers._sse import sse_event, sse_message
import queue as thread_queue
import threading
from datetime import UTC, datetime
from datetime import date as dt_date

import pandas as pd
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from airs_scanner import (
    download_portfolio_sync,
    count_model_portfolio_holdings_sync,
    fetch_model_portfolios_sync,
    fetch_portfolio_positions_sync,
    scan_portfolios_sync,
)
from deps import supabase
from portfolio import parse_airs_excel

router = APIRouter(tags=["airs"])


def _save_performance_to_db(portfolio_name: str, rows: list[dict]):
    """Upsert performance rows into the airs_performance table."""
    if not rows:
        return
    for r in rows:
        supabase.table("airs_performance").upsert({
            "portefeuille": portfolio_name,
            "periode": r["periode"],
            "beginvermogen": r["beginvermogen"],
            "koersresultaat": r["koersresultaat"],
            "opbrengsten": r["opbrengsten"],
            "beleggingsresultaat": r["beleggingsresultaat"],
            "eindvermogen": r["eindvermogen"],
            "rendement": r["rendement"],
            "cumulatief_rendement": r["cumulatief_rendement"],
        }, on_conflict="portefeuille,periode").execute()


def _parse_att_excel(content: bytes) -> list[dict]:
    """Parse AT&T Excel bytes into a list of performance row dicts."""
    df = pd.read_excel(io.BytesIO(content), engine="xlrd")
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "periode": str(r.get("Periode", ""))[:10],
            "beginvermogen": round(float(r["Beginvermogen"]), 2) if pd.notna(r.get("Beginvermogen")) else None,
            "koersresultaat": round(float(r["Koersresultaat"]), 2) if pd.notna(r.get("Koersresultaat")) else None,
            "opbrengsten": round(float(r["Opbrengsten"]), 2) if pd.notna(r.get("Opbrengsten")) else None,
            "beleggingsresultaat": round(float(r["Beleggingsresultaat"]), 2) if pd.notna(r.get("Beleggingsresultaat")) else None,
            "eindvermogen": round(float(r["Eindvermogen"]), 2) if pd.notna(r.get("Eindvermogen")) else None,
            "rendement": round(float(r["Rendement"]), 6) if pd.notna(r.get("Rendement")) else None,
            "cumulatief_rendement": round(float(r["Cumulatief rendement"]), 6) if pd.notna(r.get("Cumulatief rendement")) else None,
        })
    return rows


@router.get("/api/airs/portfolios")
async def airs_portfolios_from_db():
    """Portfolios we already have performance data for, with latest YTD."""
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("portefeuille,cumulatief_rendement,periode,fetched_at")
            .order("portefeuille")
            .order("periode", desc=True)
            .execute()
        )
        # Dedupe to latest row per portfolio.
        seen: dict[str, dict] = {}
        for r in (resp.data or []):
            name = r["portefeuille"]
            if name not in seen:
                seen[name] = {
                    "portefeuille": name,
                    "cumulatief_rendement": r["cumulatief_rendement"],
                    "periode": r["periode"],
                    "fetched_at": r["fetched_at"],
                }
        return list(seen.values())
    except Exception:
        return []


async def _airs_scan_stream():
    q: thread_queue.Queue = thread_queue.Queue()

    def send_event(msg_type: str, **kwargs):
        payload = {"type": msg_type, **kwargs}
        q.put(sse_event(payload))

    def run_scanner():
        try:
            scan_portfolios_sync(send_event)
        except Exception as e:
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    thread = threading.Thread(target=run_scanner, daemon=True)
    thread.start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/airs/scan")
async def airs_scan():
    return StreamingResponse(
        _airs_scan_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _model_portfolios_stream():
    """SSE because it is SLOW and chatty: one request per list page, plus one per row whose
    name the list truncated (the full name lives only on the edit page). ~95 portfolios is
    a couple of minutes of authenticated round-trips, so it streams progress rather than
    hanging a GET."""
    q: thread_queue.Queue = thread_queue.Queue()

    def send_event(msg_type: str, **kwargs):
        q.put(sse_event({"type": msg_type, **kwargs}))

    def run():
        try:
            # Two phases, deliberately. The LIST is fast (~6s) and is emitted as
            # "portfolios" the moment it's ready, so the table renders. Counting each
            # portfolio's holdings is an edit-page GET + an XLS download per row and takes
            # minutes — it streams "count" events into the already-visible table instead of
            # holding the whole thing back for its slowest part.
            #
            # Both phases WRITE as they go, rather than at the end: a scan that dies halfway
            # should leave behind the portfolios it did reach, not nothing.
            rows = fetch_model_portfolios_sync(send_event)
            store.save_portfolios(rows)
            count_model_portfolio_holdings_sync(
                rows, send_event,
                # Counting a portfolio means downloading its XLS — so persisting the
                # positions costs no extra AIRS traffic, and `isin` is the whole prize.
                on_positions=store.save_positions,
                on_error=store.save_positions_error,
            )
            send_event("done", count=len(rows), portfolios=rows)
        except Exception as e:  # noqa: BLE001 — surface it to the client, don't 500 the stream
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    threading.Thread(target=run, daemon=True).start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/airs/model-portfolios/scan")
async def airs_model_portfolios_scan():
    """Every model portfolio from Stamgegevens > Onderhoud portefeuilles > Model
    portefeuilles, with its FULL name (the list page truncates them). Persists as it goes."""
    return StreamingResponse(
        _model_portfolios_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class StoredModelPortfolio(BaseModel):
    """A stored portfolio row. `holdings` is derived by the view from the positions, so it
    cannot drift from them — and it keeps three absences apart that are NOT the same thing:

      has_fixed_model=false   -> NO MODEL EXISTS (a `normaal`/`meervoudig` portfolio). AIRS
                                 stores no composition at all; "0 holdings" would describe a
                                 model that isn't there.
      positions_scanned_at=None -> never counted. Unknown, not zero.
      no_snapshot=true        -> we looked, and AIRS had no DATED composition: its date
                                 dropdown held nothing but the empty "today" placeholder.
                                 Measured on BUS_DUTD_DEF_AFS + EuropaTopSelect OFF FX, both
                                 of which I first mis-reported as "0 holdings".
      holdings=0              -> a real, EMPTY fixed model. Not currently observed on any
                                 portfolio, but expressible — and it must stay distinct from
                                 the three absences above.

    `holdings` counts DISTINCT ISINs: a portfolio can list one instrument on two lines
    (VTopSelectie OFF FX holds CapitaLand at 2% and again at 3%), and that is one instrument.
    """

    id: int
    name: str
    truncated: bool = False
    omschrijving: str | None = None
    portfolio_type: str | None = None
    fixed_datum: str | None = None
    has_fixed_model: bool = False
    no_snapshot: bool = False
    holdings: int | None = None
    positions_datum: str | None = None
    positions_scanned_at: str | None = None
    positions_error: str | None = None
    scanned_at: str | None = None


@router.get("/api/airs/model-portfolios", response_model=list[StoredModelPortfolio])
async def airs_model_portfolios_stored():
    """The stored portfolios — an instant DB read. The page opens on this; `/scan` is the
    explicit refresh, because re-scraping AirSPMS costs minutes."""
    return await asyncio.to_thread(store.load_portfolios)


class ModelPortfolioPerformance(BaseModel):
    """One model portfolio's performance, in EUR: YTD, since-inception, Sharpe, Sortino.

    ⚠ `ytd_pct` IS NOT ALWAYS A FULL YEAR. It is a buy-and-hold of the composition WE HOLD,
    which is the CURRENT one — AIRS keeps only 2-3 snapshot dates and no monthly history, so
    January's composition is not recoverable. The window therefore opens at
    `max(Jan 1, inception)`, never before the weights existed, and `ytd_from` is that date:

      * `model_changed_in_period` false (29 of 56) — the model has held these weights since
        before Jan 1. `ytd_from` is Jan 1 and this is a true, full YTD.
      * true (27 of 56) — the model is YOUNGER than the year, so `ytd_from` is its inception
        and the figure covers a PARTIAL year. Realized, not backtested — but do not rank it
        against a 12-month return without noticing (MoTopSelectie_FX has held its weights for
        eight days: +0.51%. Priced back to Jan 1 it would read +75.85%, on a basket it never
        held, and be the best portfolio in the list).

    `since_model_pct` is the same composition's return over its WHOLE life (`model_effective` —
    its inception), not clipped to this year. For a model younger than the year the two windows
    coincide and the two numbers are equal, by construction.

    `sharpe` / `sortino` ride that SAME window, annualized from the daily EUR curve at rf = 0.
    A ratio is only as honest as the return underneath it, and a YTD-anchored one is a backtest
    for half the list. They are NULL — not zero — below `MIN_STAT_DAYS` (20) daily returns: a
    ratio off a model defined last week is noise with two decimals, and it would render in the
    same column, same font, as one measured over two years. `stat_days` is how many it had.

    `ytd_pct` is NULL when `low_coverage` — under 60% of the model's weight is priceable, so a
    renormalised return would be an invention (TOPS_OFF_BEH once reported "+0.00%" off its 1%
    cash line while 99% of it, in structured products, was silently dropped). The since-
    inception figures carry their OWN floor (`since_covered_pct`), because a holding that had
    not listed yet at inception is unpriceable there whatever its coverage at Jan 1 was.
    """

    portfolio_id: int
    name: str
    model_effective: str | None = None
    model_changed_in_period: bool = False
    ytd_pct: float | None = None
    ytd_from: str | None = None
    since_model_pct: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    # Geometric annualized return over the since-inception window. NULL under a YEAR of trading
    # days — annualizing a shorter period extrapolates it (+11.20% over 99 days compounds to
    # +30.6%/yr), which is why fund reporting shows a cumulative return there instead. That
    # cumulative number is `since_model_pct`, and it is always present.
    cagr_pct: float | None = None
    ann_vol_pct: float | None = None
    stat_days: int = 0
    # How long the model has been running: inception -> today, in calendar years. The unit the
    # ratios above have to be read against — and the reason a CAGR can be absent (under 1.00).
    years_running: float | None = None
    # DISTINCT instruments with a Yahoo price series, and those without. Both EXCLUDE cash (it
    # has no ISIN and no series, and the `holdings` count they are read against excludes it
    # too), and both count distinct ISINs rather than rows — a model may list one instrument on
    # two lines. So `resolved + unresolved == holdings`, exactly.
    resolved_holdings: int = 0
    unresolved_holdings: int = 0
    # ⚠ How many holdings were marked at an INTERPOLATED opening price rather than a real close.
    # Non-zero means part of this return is an estimate — it is 23.6% of BUS_OBL_HighY, whose
    # iShares Euro HY line is mapped to a US OTC listing that trades a handful of times a year.
    interpolated_holdings: int = 0
    priced_holdings: int = 0        # every leg the curve holds — cash INCLUDED
    unpriced_holdings: int = 0
    covered_pct: float | None = None
    since_covered_pct: float | None = None
    low_coverage: bool = False
    partial_coverage: bool = False
    cash_pct: float = 0.0


@router.get("/api/airs/model-portfolios/performance",
            response_model=list[ModelPortfolioPerformance])
async def airs_model_portfolio_performance(year: int | None = None):
    """YTD (EUR) for every stored model portfolio. Read `ModelPortfolioPerformance`'s
    docstring — for half of them the number is a backtest, not a track record."""
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        compute_portfolio_performance_async,
    )

    return await compute_portfolio_performance_async(year)


class PortfolioCorrelationMatrix(BaseModel):
    """Pairwise Pearson correlation of the LISTED model portfolios' daily EUR returns, for two
    windows. Same return series the /portfolios YTD column is read off, correlated pairwise-
    complete. Covers exactly the > 5-holding models the table shows by default ("42 of 95").

    `ytd` / `trailing_12m` are NxN over `portfolio_ids` (row i = column i = `portfolio_ids[i]`,
    `labels[i]`). A cell is `null` when the pair share fewer than `min_overlap_days` common daily
    returns (or a side has no priceable series / zero variance). The diagonal is 1.0 where the
    portfolio has enough of its own returns, else null. `*_obs` is each portfolio's daily-return
    count in that window."""

    portfolio_ids: list[int]
    labels: list[str]
    as_of: str
    min_overlap_days: int
    ytd: list[list[float | None]]
    ytd_obs: list[int]
    trailing_12m: list[list[float | None]]
    trailing_12m_obs: list[int]


@router.get("/api/airs/model-portfolios/correlations",
            response_model=PortfolioCorrelationMatrix)
async def airs_model_portfolio_correlations(year: int | None = None):
    """YTD + trailing-12m return-correlation matrices over the listed (> 5-holding) models."""
    from routers._airs_portfolio_correlation import (  # noqa: PLC0415
        compute_portfolio_correlations_async,
    )

    return await compute_portfolio_correlations_async(year)


class PortfolioAnalysisRow(BaseModel):
    bucket: str
    portfolio_pct: float = 0.0
    benchmark_pct: float = 0.0
    diff_pct: float = 0.0              # the TILT — the reason the two are side by side


class PortfolioAnalysisAxis(BaseModel):
    axis: str                          # sector | region | currency
    rows: list[PortfolioAnalysisRow]


class PortfolioAnalysisReturns(BaseModel):
    """The model's EUR return beside the benchmark's — over the SAME windows, both times.

    ⚠ A BENCHMARK MEASURED OVER A DIFFERENT WINDOW IS NOT A BENCHMARK, IT IS A NUMBER. A model's
    "YTD" opens at `max(1 Jan, its inception)`, and for the 27 models younger than the year that
    is NOT 1 January. Putting a 9-day portfolio return beside the index's full-year return and
    calling the gap out-performance would be nonsense that looks exactly like a finding. So the
    index is priced from the model's OWN `ytd_from`, and again from its OWN inception.

    `ytd_is_since` is true when the model is younger than the year: the two windows coincide, so
    the two rows are the same number by construction, and the UI says so.
    """

    ytd_from: str | None = None
    since_from: str | None = None
    portfolio_ytd_pct: float | None = None
    benchmark_ytd_pct: float | None = None
    ytd_excess_pct: float | None = None
    portfolio_since_pct: float | None = None
    benchmark_since_pct: float | None = None
    since_excess_pct: float | None = None
    ytd_is_since: bool = False


class ModelPortfolioAnalysis(BaseModel):
    """A model portfolio's composition beside a benchmark's, on ONE set of buckets.

    Both sides are classified from `asset_grid`'s yfinance attributes, joined by ISIN — the
    portfolio lives in the ISIN world and the benchmark in the `company` world, and putting two
    different sector taxonomies in one chart invents differences that are not there. (All 493
    SP500 members are present in `asset_grid` with a sector, so nothing is lost.)

    ⚠ FUNDS ARE NOT LOOKED THROUGH, and the payload says so rather than pretending. An ETF's
    listing tells you nothing about what it holds — 24 of the 26 held ETFs have a "sector" of
    literally `etf` or `Equity`; an Amsterdam-listed MSCI World ETF is not European exposure; and
    quoted in EUR it still holds mostly USD assets. So every fund lands in ONE bucket, "Fund (not
    looked through)", on ALL THREE axes. A 40%-ETF portfolio shows a 40% bar that means "we
    cannot see inside this" — true, and more useful than a confident wrong split.
    """

    portfolio_id: int
    name: str | None = None
    as_of: str | None = None
    benchmark: str
    benchmark_members: int = 0
    holdings: int = 0
    covered_pct: float = 0.0
    benchmark_covered_pct: float = 0.0
    # Rows priced on a venue whose currency differs from the company's own — the wrong-listing
    # bug, surfaced rather than absorbed. 40 of the S&P's 491 sit on European/Canadian lines.
    foreign_listings: int = 0
    benchmark_foreign_listings: int = 0
    # ⚠ How much of the INDEX we could price. ACWI's missing names go a whole country at a time
    # (GuruFocus sells no UK/India; yfinance has them, but some were never ingested), and a
    # cap-weighted index renormalised over the rest does not lose that weight — it redistributes
    # it into everything else. Stated, never assumed to be 100%.
    benchmark_universe_members: int = 0
    benchmark_priced: int = 0
    benchmark_coverage_pct: float | None = None
    returns: PortfolioAnalysisReturns | None = None
    axes: list[PortfolioAnalysisAxis] = []


@router.get("/api/airs/model-portfolios/{portfolio_id}/analysis",
            response_model=ModelPortfolioAnalysis)
async def airs_model_portfolio_analysis(portfolio_id: int, benchmark: str = "SP500"):
    """Sector / region / currency split of one model portfolio, beside the benchmark's."""
    from routers._airs_portfolio_analysis import (  # noqa: PLC0415
        compute_portfolio_analysis_async,
    )

    return await compute_portfolio_analysis_async(portfolio_id, benchmark)


class AttributionName(BaseModel):
    isin: str | None = None
    name: str | None = None
    ticker: str | None = None
    weight_pct: float = 0.0
    return_pct: float | None = None
    contribution_pct: float = 0.0
    # True when this company is held on BOTH sides — in the model AND the benchmark bucket.
    # Matched by company (same_company), not ISIN, so a share class still counts as the same name.
    # Only populated for the per-bucket holdings lists; the contributor/detractor lists leave it
    # false.
    in_both: bool = False


class AttributionBucket(BaseModel):
    bucket: str
    portfolio_weight_pct: float = 0.0
    benchmark_weight_pct: float = 0.0
    portfolio_return_pct: float | None = None
    benchmark_return_pct: float | None = None
    allocation_pct: float = 0.0
    selection_pct: float = 0.0
    interaction_pct: float = 0.0
    total_pct: float = 0.0
    # The names BEHIND this bucket, for the click-through detail: the model's holdings in it, and
    # the index's constituents in it. Weights are the RAW (un-renormalised) ones, so they line up
    # with the composition chart. `benchmark_holdings` is capped to the largest few — an index
    # sector can hold ~70 names — with `benchmark_holdings_count` the true total.
    portfolio_holdings: list[AttributionName] = []
    benchmark_holdings: list[AttributionName] = []
    benchmark_holdings_count: int = 0


class AttributionExcluded(BaseModel):
    bucket: str
    name: str | None = None
    isin: str | None = None
    weight_pct: float = 0.0
    return_pct: float | None = None
    reason: str | None = None       # fund | cash | unpriced | unclassified


class ModelPortfolioAttribution(BaseModel):
    """WHY a model beat or lagged the index — Brinson-Fachler, plus the names that drove it.

    An excess return is a fact, not an explanation: "-11.60% vs ACWI" says nothing about whether
    the failed bet was the SECTORS chosen or the STOCKS chosen inside them. Those are different
    mistakes with different fixes.

        allocation  = (w_p - w_b) x (R_b,bucket - R_b_total)   the right buckets?
        selection   =  w_b        x (R_p,bucket - R_b,bucket)  the right names inside them?
        interaction = the cross term

    ⚠ THE IDENTITY IS ASSERTED, NOT ASSUMED: sum(allocation + selection + interaction) == excess.
    `residual_pct` and `reconciles` carry the proof. Three columns that do not sum to the excess
    are not a decomposition of it.

    ⚠ FUNDS AND CASH ARE EXCLUDED. An ETF has no sector — the benchmark's weight in the fund
    bucket is zero, so Brinson would report holding a world tracker as a *sector bet*.
    `attributable_pct` says how much of the model the table explains.

    ⚠ `unpriced_pct` IS NOT THE SAME AS `excluded_pct`, AND IT IS THE DANGEROUS ONE. A fund is
    excluded because it is not a sector bet. An UNPRICED equity is excluded because we failed to
    price it — and its sector then reads as UNOWNED, so the allocation effect on that row is a
    FALSE finding. (Measured: a model holding 6% Healthcare, unpriceable, was credited +1.73pp of
    allocation for "avoiding" Healthcare.) `unpriced_buckets` names the rows to discount.
    """

    portfolio_id: int
    name: str | None = None
    benchmark: str
    benchmark_coverage_pct: float | None = None
    window: str                      # ytd | since
    axis: str                        # sector | region | currency
    start: str | None = None
    portfolio_return_pct: float = 0.0
    benchmark_return_pct: float = 0.0
    excess_pct: float = 0.0
    attributed_pct: float = 0.0
    residual_pct: float = 0.0
    reconciles: bool = False
    attributable_pct: float = 0.0
    excluded_pct: float = 0.0
    excluded_return_pct: float | None = None
    unpriced_pct: float = 0.0
    unpriced_buckets: list[str] = []
    excluded: list[AttributionExcluded] = []
    rows: list[AttributionBucket] = []
    top_contributors: list[AttributionName] = []
    top_detractors: list[AttributionName] = []
    missed_winners: list[AttributionName] = []


@router.get("/api/airs/model-portfolios/{portfolio_id}/attribution",
            response_model=ModelPortfolioAttribution)
async def airs_model_portfolio_attribution(
    portfolio_id: int, benchmark: str = "SP500", window: str = "ytd", axis: str = "sector",
):
    """Brinson-Fachler attribution of one model against a benchmark, over one window."""
    from routers._airs_portfolio_attribution import (  # noqa: PLC0415
        compute_attribution_async,
    )

    return await compute_attribution_async(portfolio_id, benchmark, window, axis)


class ModelPortfolioPosition(BaseModel):
    """One row of the portfolio's XLS export. `isin` is the point of the whole exercise —
    it is the exact join into `asset_execution`, and it's the identifier the AIRS
    *holdings* sheet never gave us (that one only has a fund NAME).

    The price marks are the ARITHMETIC BEHIND the portfolio's YTD, one holding at a time: each
    is bought at its last close on or before `ytd_from` and held to its latest close, and
    `return_pct` is exactly the quantity the portfolio figure weights together.

    ⚠ `start_price_eur` / `end_price_eur` are in EUR, not the listing's currency, because
    `return_pct` is an EUR return and carries the FX leg. Printing the native closes as the
    arithmetic would show two numbers whose ratio is not the third — a USD holding can rise in
    dollars and fall in euros. The native closes ride along (`*_price_local`, `currency`) for a
    tooltip, never as the sum.

    All of them are NULL for a holding with no price series (an unresolved ETF, a structured
    product) and for the cash line — which has no ISIN, and is not an instrument.
    """

    fonds: str | None = None
    isin: str | None = None            # NULL for the cash line ("Liquiditeiten")
    percentage: float | None = None
    valuta: str | None = None
    categorie: str | None = None
    sector: str | None = None
    regio: str | None = None
    # True when this ISIN is already an instrument in our grid (`asset_execution`).
    known_instrument: bool = False

    currency: str | None = None        # the LISTING's currency (may differ from AIRS `valuta`)
    # The holding's LATEST close, returned even when no marks could be computed — it is the only
    # thing that separates "the price series is STALE" from "this holding is broken". A series
    # whose last close predates the window has no price inside it, so no return over it exists;
    # the mapping is fine (Meta Platforms is correctly on META and simply hasn't been refreshed).
    last_close: str | None = None
    start_date: str | None = None      # last close on or before the window opened
    start_price_eur: float | None = None
    start_price_local: float | None = None
    # ⚠ The opening price is an ESTIMATE, not a close: this holding's series has no price near
    # the anchor (it trades rarely, or is pointed at a listing that does), so the value was
    # linearly interpolated between the two real closes bracketing the date — `start_gap_days`
    # apart. `start_price_local` is NULL for these: there was no trade that day, and printing a
    # neighbouring day's local price would dress the estimate up as an observation.
    start_interpolated: bool = False
    start_gap_days: int = 0
    end_date: str | None = None        # its latest close (can lag: vendors publish unevenly)
    end_price_eur: float | None = None
    end_price_local: float | None = None
    return_pct: float | None = None    # EUR, start -> end
    # These marks are a LOOK-THROUGH, not a traded price: this row is a certificate wrapping
    # another model (see `linked_portfolio_id`), and its Start/End are that model's basket indexed
    # to 100 at the window open — only the return between them is a real number. Rendered distinctly
    # so a synthetic index is never mistaken for a share price.
    lookthrough: bool = False

    # The model portfolio this holding IS. Some positions are not instruments at all but other
    # models, wrapped as a Leonteq certificate ("Star Selection Index" IS StarTopSelectie OFF
    # FX). NULL when it is a plain instrument — which is almost every row.
    linked_portfolio_id: int | None = None
    linked_portfolio_name: str | None = None
    # 'manual' — a human set it, and it is authoritative (including a manual NULL: "this is NOT
    # a portfolio", which has to survive a re-read or a wrong guess could never be dismissed).
    # 'auto'   — our educated guess, with the confidence that earned it.
    link_source: str | None = None
    link_confidence: float | None = None   # 0-1; NULL for a manual link — a choice is not a guess
    link_reason: str | None = None


class ModelPortfolioPositions(BaseModel):
    portfolio: str
    portfolio_id: int
    datum: str | None = None           # the snapshot actually used
    dates: list[str]                   # every snapshot AIRS offers, for a date picker
    rows: list[ModelPortfolioPosition]
    matched: int                       # how many ISINs we already hold
    unmatched: int
    # The day the per-row price marks are measured FROM — `max(1 Jan, this composition's date)`,
    # the same anchor the row's YTD uses. Stated, because "since when" is half of what a return
    # means and the answer is not 1 January for half the portfolios.
    ytd_from: str | None = None
    # When this came from OUR cache rather than a live AirSPMS fetch, and when it was taken.
    # The UI says so — a cached answer presented as fresh is how a stale holding gets trusted.
    cached_at: str | None = None


def _shape_positions(raw: dict) -> ModelPortfolioPositions:
    from deps import IN_CHUNK_SIZE  # noqa: PLC0415
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        compute_holding_marks,
        ytd_anchor_for,
    )

    rows = raw["rows"]
    isins = sorted({str(r.get("ISINCode")).strip() for r in rows if r.get("ISINCode")})

    known: set[str] = set()
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        chunk = isins[i:i + IN_CHUNK_SIZE]
        got = (supabase.table("asset_execution").select("isin")
               .in_("isin", chunk).execute().data or [])
        known.update(r["isin"] for r in got)

    # Which of these rows are themselves model portfolios. NEVER cached, for the same reason
    # `known_instrument` isn't: the guess is a fuzzy match against the CURRENT portfolio list,
    # so a stored one would be wrong the moment a portfolio is renamed or gains a composition.
    # A stored *manual* link wins over the guess — see `resolve_links`. Resolved BEFORE the marks
    # because a certificate row's marks come from the model it links to (the look-through).
    from routers._airs_portfolio_links import link_key, resolve_links  # noqa: PLC0415
    _lrows = [{"isin": (str(r["ISINCode"]).strip() if r.get("ISINCode") else None),
               "fonds": (str(r["Fonds"]).strip() if r.get("Fonds") else "")} for r in rows]
    links = resolve_links(supabase, raw["portfolio_id"], _lrows)
    _pf_names = {p["id"]: p["name"] for p in (
        supabase.table("airs_model_portfolio").select("id,name").execute().data or [])}

    # Anchored on the composition BEING SHOWN — for the default (newest) snapshot that is the
    # portfolio's `positions_datum`, so these marks are the ones the table's YTD was computed
    # from and the two reconcile. Pick a historical snapshot and the anchor moves with it, which
    # is right: those are different weights, and their window opened when they did.
    #
    # A row linked to another model is a certificate with no Yahoo price of its own; `linked`
    # tells `compute_holding_marks` to price it by looking THROUGH to that model's basket, so its
    # once-dead Start/End/Return columns fill with the wrapped model's return over this window.
    #
    # NEVER cached. A price mark is true for one day; the composition it prices is not.
    anchor = ytd_anchor_for(raw.get("datum")) if raw.get("datum") else None
    linked_map: dict[str, int] = {}
    for _lr in _lrows:
        _lk = links.get(link_key(_lr["isin"], _lr["fonds"]))
        if _lr["isin"] and _lk and _lk.linked_portfolio_id:
            linked_map[_lr["isin"]] = _lk.linked_portfolio_id
    marks = (compute_holding_marks(isins, anchor, linked=linked_map)
             if (anchor and isins) else {})

    out: list[ModelPortfolioPosition] = []
    for r in rows:
        isin = (str(r["ISINCode"]).strip() if r.get("ISINCode") else None) or None
        m = marks.get(isin) if isin else None
        fonds = (str(r["Fonds"]).strip() if r.get("Fonds") else None)
        lk = links.get(link_key(isin, fonds))
        out.append(ModelPortfolioPosition(
            fonds=fonds,
            isin=isin,
            linked_portfolio_id=(lk.linked_portfolio_id if lk else None),
            linked_portfolio_name=(
                _pf_names.get(lk.linked_portfolio_id) if lk and lk.linked_portfolio_id else None),
            link_source=(lk.source if lk else None),
            link_confidence=(lk.confidence if lk else None),
            link_reason=(lk.reason if lk else None),
            percentage=(float(r["Percentage"]) if r.get("Percentage") is not None else None),
            valuta=(str(r["valuta"]).strip() if r.get("valuta") else None),
            categorie=(str(r["Beleggingscategorie"]).strip() if r.get("Beleggingscategorie") else None),
            sector=(str(r["Beleggingssector"]).strip() if r.get("Beleggingssector") else None),
            regio=(str(r["regio"]).strip() if r.get("regio") else None),
            known_instrument=bool(isin and isin in known),
            **(m or {}),
        ))

    matched = sum(1 for r in out if r.known_instrument)
    return ModelPortfolioPositions(
        portfolio=raw["portfolio"], portfolio_id=raw["portfolio_id"],
        datum=raw["datum"], dates=raw["dates"], rows=out,
        matched=matched, unmatched=len([r for r in out if r.isin]) - matched,
        ytd_from=anchor,
        cached_at=raw.get("cached_at"),
    )


def _live_positions(portfolio_id: int, datum: str | None) -> dict:
    """Fetch from AIRS and REFRESH THE CACHE with what came back — a live read that left the
    stored copy behind would guarantee the two disagree."""
    raw = fetch_portfolio_positions_sync(portfolio_id, datum=datum)
    # Only the DEFAULT snapshot is cached: we store one composition per portfolio (the newest
    # with rows). Persisting an ad-hoc historical `datum` the user picked would overwrite the
    # current one with an old one — the cache would silently rot backwards.
    if datum is None:
        try:
            store.save_positions(portfolio_id, raw["datum"], raw["rows"], raw.get("dates"))
        except Exception:  # noqa: BLE001 — a cache write must never fail the read
            pass
    return raw


@router.get("/api/airs/model-portfolios/{portfolio_id}/positions",
            response_model=ModelPortfolioPositions)
async def airs_model_portfolio_positions(
    portfolio_id: int, datum: str | None = None, refresh: bool = False,
):
    """One model portfolio's positions — the XLS export that DOES carry an ISIN (the AIRS
    *holdings* sheet does not; it has only a fund name).

    SERVED FROM OUR CACHE by default: the scan already downloaded this XLS to count the
    portfolio's holdings, so re-scraping AirSPMS on every expand is pure waste (and a
    several-second wait on an authenticated round-trip). Goes to AIRS only when:
      * `refresh=true`   — the user explicitly wants the current truth, or
      * `datum` is given — a historical snapshot, of which we cache only the newest, or
      * we have nothing stored for this portfolio yet.

    A cached answer carries `cached_at` and the UI says so. A cached response presented as
    fresh is exactly how a stale holding gets trusted.

    `known_instrument` is NEVER cached — it is a join against `asset_execution`, which grows
    every time we add an instrument, so it is recomputed on every read. Cached, a "not in
    grid" flag would be wrong the moment the grid catches up.
    """
    if not refresh and datum is None:
        cached = await asyncio.to_thread(store.load_positions, portfolio_id)
        if cached is not None:
            return await asyncio.to_thread(_shape_positions, cached)

    raw = await asyncio.to_thread(_live_positions, portfolio_id, datum)
    return await asyncio.to_thread(_shape_positions, raw)


class LinkablePortfolio(BaseModel):
    id: int
    name: str
    omschrijving: str | None = None
    positions: int          # how many holdings it has — a 0 has nothing to look through to


class SetLinkRequest(BaseModel):
    isin: str | None = None
    fonds: str
    # NULL is a real answer: "this holding is explicitly NOT a portfolio". It has to be storable,
    # or a wrong guess could only be re-pointed at another portfolio, never dismissed.
    linked_portfolio_id: int | None = None


class LinkableContext(BaseModel):
    options: list[LinkablePortfolio]
    # holding ISIN -> the portfolios that already HOLD it. A link to one of those is a cycle, so
    # the row's dropdown drops them. Keyed by ISIN and small: few holdings are portfolios.
    excluded_by_isin: dict[str, list[int]]


@router.get("/api/airs/model-portfolios/{portfolio_id}/linkable",
            response_model=LinkableContext)
async def airs_linkable_portfolios(portfolio_id: int):
    """What the rows of this portfolio may be linked TO — every model except the ones a link to
    would be a cycle: the portfolio itself (no self-reference), and per row, any portfolio that
    already HOLDS that holding (TOPS_STS_L holds 'Star Selection Index' at 100% — a link there
    walks straight back to the row you started from).

    ONE call for the whole table. Per row it would be ~30 requests to open one portfolio.
    """
    from routers._airs_portfolio_links import linkable_context  # noqa: PLC0415
    return await asyncio.to_thread(linkable_context, supabase, portfolio_id)


def _save_link(owner_id: int, body: SetLinkRequest) -> dict:
    from routers._airs_portfolio_links import link_key, linkable_portfolios  # noqa: PLC0415

    target = body.linked_portfolio_id
    if target is not None:
        # Re-validate SERVER-SIDE. The dropdown already excludes these, but a cycle written
        # through the API is a cycle all the same — and a look-through that loops does not
        # fail loudly, it recurses.
        allowed = {p["id"] for p in linkable_portfolios(supabase, owner_id, body.isin)}
        if target not in allowed:
            raise HTTPException(
                400,
                "That portfolio cannot be linked here: it is either this portfolio itself or one "
                "that already holds this position — either way the link would be a cycle.",
            )

    key = link_key(body.isin, body.fonds)
    existing = (supabase.table("airs_model_portfolio_link")
                .select("id,isin,fonds").execute().data or [])
    row_id = next((r["id"] for r in existing if link_key(r.get("isin"), r.get("fonds")) == key),
                  None)
    payload = {"isin": body.isin, "fonds": body.fonds, "linked_portfolio_id": target,
               "updated_at": datetime.now(UTC).isoformat()}
    if row_id:
        supabase.table("airs_model_portfolio_link").update(payload).eq("id", row_id).execute()
    else:
        supabase.table("airs_model_portfolio_link").insert(payload).execute()
    return {"ok": True, "linked_portfolio_id": target}


@router.put("/api/airs/model-portfolios/{portfolio_id}/link")
async def airs_set_portfolio_link(portfolio_id: int, body: SetLinkRequest):
    """Point a holding at the model portfolio it IS (or, with a null target, record that it is
    NOT one). Stored against the HOLDING, not the (parent, holding) pair: 'Star Selection Index'
    is `StarTopSelectie OFF FX` in all 11 models that hold it, and eleven copies of one fact are
    eleven chances to disagree. So this edit takes effect in every portfolio holding it."""
    return await asyncio.to_thread(_save_link, portfolio_id, body)


@router.delete("/api/airs/model-portfolios/{portfolio_id}/link")
async def airs_clear_portfolio_link(portfolio_id: int, isin: str | None = None, fonds: str = ""):
    """Forget the human decision for this holding and fall back to the automatic guess. This is
    NOT the same as linking it to nothing — that is a decision too, and is stored as a null."""
    from routers._airs_portfolio_links import link_key  # noqa: PLC0415

    def _run() -> dict:
        key = link_key(isin, fonds)
        rows = (supabase.table("airs_model_portfolio_link")
                .select("id,isin,fonds").execute().data or [])
        for r in rows:
            if link_key(r.get("isin"), r.get("fonds")) == key:
                supabase.table("airs_model_portfolio_link").delete().eq("id", r["id"]).execute()
        return {"ok": True}

    return await asyncio.to_thread(_run)


@router.get("/api/airs/portfolio/{portfolio_name}")
async def airs_portfolio_download(
    portfolio_name: str,
    datum_van: str | None = None,
    datum_tot: str | None = None,
    refresh: bool = False,
):
    """Return performance data. Serves from DB cache unless refresh=true or no cache."""
    today = dt_date.today()
    if not datum_van:
        datum_van = f"{today.year}-01-01"
    if not datum_tot:
        datum_tot = today.isoformat()

    # Check what we already have in DB.
    db_rows: list[dict] = []
    needs_refresh = True
    try:
        resp = await asyncio.to_thread(
            lambda: supabase.table("airs_performance")
            .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement,fetched_at")
            .eq("portefeuille", portfolio_name)
            .order("periode")
            .execute()
        )
        db_rows = resp.data or []
        if db_rows and not refresh:
            last_fetched = db_rows[-1].get("fetched_at", "")[:10]
            needs_refresh = last_fetched != today.isoformat()
    except Exception:
        pass  # table may not exist yet

    if needs_refresh:
        try:
            content = await asyncio.to_thread(download_portfolio_sync, portfolio_name, datum_van, datum_tot)
            fresh_rows = await asyncio.to_thread(_parse_att_excel, content)
        except Exception as e:
            if db_rows:
                rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
                return {
                    "portfolio_name": portfolio_name,
                    "datum_van": datum_van,
                    "datum_tot": datum_tot,
                    "rows": rows,
                    "cached": True,
                }
            raise HTTPException(status_code=500, detail=f"Download failed: {e}")

        try:
            await asyncio.to_thread(_save_performance_to_db, portfolio_name, fresh_rows)
        except Exception:
            pass

        try:
            resp = await asyncio.to_thread(
                lambda: supabase.table("airs_performance")
                .select("periode,beginvermogen,koersresultaat,opbrengsten,beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement")
                .eq("portefeuille", portfolio_name)
                .order("periode")
                .execute()
            )
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": resp.data or fresh_rows,
                "cached": False,
            }
        except Exception:
            return {
                "portfolio_name": portfolio_name,
                "datum_van": datum_van,
                "datum_tot": datum_tot,
                "rows": fresh_rows,
                "cached": False,
            }

    rows = [{k: v for k, v in r.items() if k != "fetched_at"} for r in db_rows]
    return {
        "portfolio_name": portfolio_name,
        "datum_van": datum_van,
        "datum_tot": datum_tot,
        "rows": rows,
        "cached": True,
    }


# ─── Vermogensoverzicht (holdings) — daily scheduled scrape + store ──────────


@router.post("/api/airs/vermogen/refresh")
async def airs_vermogen_refresh():
    """Trigger the per-portfolio Vermogensoverzicht refresh now (the /airs-
    portfolio "Refresh now" button). Re-discovers the live portfolio list, then
    downloads + stores each portfolio's holdings. Runs in a daemon thread and
    returns immediately; poll `/api/airs/vermogen/status` for progress."""
    from airs_vermogen import _STATUS, run_airs_vermogen_refresh_sync  # noqa: PLC0415

    if _STATUS.get("running"):
        return {"status": "busy", "message": "A refresh is already running"}
    threading.Thread(
        target=run_airs_vermogen_refresh_sync,
        kwargs={"triggered_by": "manual"},
        daemon=True,
        name="airs-vermogen-manual",
    ).start()
    return {"status": "started"}


@router.get("/api/airs/vermogen/status")
async def airs_vermogen_status():
    """Status of the Vermogensoverzicht refresh job: in-flight progress, last
    result, the freshest stored snapshot date, and the next scheduled run."""
    from airs_vermogen import get_status  # noqa: PLC0415
    from scheduler import list_scheduled_jobs  # noqa: PLC0415

    status = await asyncio.to_thread(get_status)
    next_run_at = None
    for j in list_scheduled_jobs():
        if j.get("id") == "airs_vermogen_refresh":
            next_run_at = j.get("next_run_at")
            break
    return {**status, "next_run_at": next_run_at}


@router.get("/api/airs/vermogen/{portfolio_name}")
async def airs_vermogen_holdings(portfolio_name: str, as_of: str | None = None):
    """Stored Vermogensoverzicht holdings for a portfolio — the latest snapshot
    by default, or a specific `as_of` date. Returns `{portfolio_name,
    as_of_date, holdings: [...]}` (holdings shaped like `/api/portfolios/parse`)."""
    def _q() -> dict:
        date_q = as_of
        if not date_q:
            latest = (
                supabase.table("airs_holding")
                .select("as_of_date").eq("portefeuille", portfolio_name)
                .order("as_of_date", desc=True).limit(1).execute()
            )
            if not latest.data:
                return {"portfolio_name": portfolio_name, "as_of_date": None, "holdings": []}
            date_q = latest.data[0]["as_of_date"]
        rows = (
            supabase.table("airs_holding")
            .select("holding_name, quantity, currency, weight, start_value_eur, "
                    "current_value_eur, ytd_return_eur, ytd_return_pct, ytd_return_local_pct")
            .eq("portefeuille", portfolio_name).eq("as_of_date", date_q)
            .order("current_value_eur", desc=True).execute()
        ).data or []
        return {"portfolio_name": portfolio_name, "as_of_date": date_q, "holdings": rows}

    return await asyncio.to_thread(_q)


@router.get("/api/airs/crm-relaties")
async def airs_crm_relaties():
    """The latest stored CRM 'Alle relaties' export, parsed on the fly from the
    raw .xls in `airs_crm_relaties_raw` into a generic `{columns, rows}` table
    (whatever columns the export has). Empty until the daily job has run it."""
    import base64 as _b64  # noqa: PLC0415

    def _q() -> dict:
        latest = (
            supabase.table("airs_crm_relaties_raw")
            .select("as_of_date, filename, content_base64, byte_size, retrieved_at")
            .order("as_of_date", desc=True).limit(1).execute()
        ).data
        if not latest:
            return {"as_of_date": None, "columns": [], "rows": [], "row_count": 0}
        r = latest[0]
        raw = _b64.b64decode(r["content_base64"])
        try:
            df = pd.read_excel(io.BytesIO(raw), engine="xlrd")  # AIRS exports .xls (BIFF)
        except Exception:
            df = pd.read_excel(io.BytesIO(raw))  # fall back to pandas auto-detect (.xlsx)
        columns = [str(c) for c in df.columns]
        # to_json handles NaN→null, dates→iso, numpy→native; round-trip to plain dicts.
        import json as _json  # noqa: PLC0415
        rows = _json.loads(df.to_json(orient="records", date_format="iso"))
        return {
            "as_of_date": r["as_of_date"],
            "retrieved_at": r.get("retrieved_at"),
            "byte_size": r.get("byte_size"),
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }

    try:
        return await asyncio.to_thread(_q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse CRM relaties: {e}")


@router.post("/api/portfolios/parse")
async def parse_portfolio(file: UploadFile = File(...)):
    content = await file.read()
    try:
        holdings = await asyncio.to_thread(parse_airs_excel, content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    total_start = sum(h.start_value_eur for h in holdings if h.start_value_eur is not None)
    total_current = sum(h.current_value_eur for h in holdings if h.current_value_eur is not None)
    total_ytd_eur = round(total_current - total_start, 2) if total_start else None
    total_ytd_pct = round((total_current - total_start) / abs(total_start), 6) if total_start else None

    return {
        "holdings": [
            {
                "holding_name": h.holding_name,
                "quantity": h.quantity,
                "currency": h.currency,
                "weight": h.weight,
                "start_value_eur": h.start_value_eur,
                "current_value_eur": h.current_value_eur,
                "ytd_return_eur": h.ytd_return_eur,
                "ytd_return_pct": h.ytd_return_pct,
                "ytd_return_local_pct": h.ytd_return_local_pct,
            }
            for h in holdings
        ],
        "total_start_eur": round(total_start, 2) if total_start else None,
        "total_current_eur": round(total_current, 2) if total_current else None,
        "total_ytd_eur": total_ytd_eur,
        "total_ytd_pct": total_ytd_pct,
    }

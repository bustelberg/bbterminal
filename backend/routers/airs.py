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
import re
from routers import _airs_portfolio_store as store
from routers._asset_financials import BasketRequest, PerformanceResponse, PriceSeriesResponse
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
            # The flows. Without these the two returns we store disagree with no stated cause.
            "stortingen": r.get("stortingen"),
            "onttrekkingen": r.get("onttrekkingen"),
            "koersresultaat": r["koersresultaat"],
            "opbrengsten": r["opbrengsten"],
            # The two terms that close beleggingsresultaat = koers + opbrengsten - kosten + rente.
            "kosten": r.get("kosten"),
            "mutatie_opgelopen_rente": r.get("mutatie_opgelopen_rente"),
            "beleggingsresultaat": r["beleggingsresultaat"],
            "eindvermogen": r["eindvermogen"],
            "rendement": r["rendement"],
            "cumulatief_rendement": r["cumulatief_rendement"],
        }, on_conflict="portefeuille,periode").execute()


def _parse_att_excel(content: bytes) -> list[dict]:
    """Parse the ATT (Rendementen) Excel bytes into a list of performance row dicts.

    The sheet has TWELVE columns and we long read seven of them:

        Periode · Beginvermogen · Stortingen · Onttrekkingen · Koersresultaat ·
        Opbrengsten · Kosten · Mutatie opgelopen rente · Beleggingsresultaat ·
        Eindvermogen · Rendement · Cumulatief rendement

    ⚠ EVERY ROW IS ONE MONTH, AND EVERY MONEY COLUMN IS THAT MONTH'S. The sheet is a
    chain — each row's `Beginvermogen` is the previous row's `Eindvermogen` — so the year
    is the SUM of these rows, not the last of them. `_airs_accounts._year_perf` is the only
    place that assembly happens; read its docstring before using anything here.

    ⚠ `Stortingen`/`Onttrekkingen` are the flows, and they are the term that closes the
    year: eind - begin - stortingen + onttrekkingen == sum(beleggingsresultaat), to -0.00
    on AITopSelectie. They are NOT why `rendement` and `cumulatief_rendement` differ — that
    is the month-vs-year thing above, and AITopSelectie has zero flows all year.

    ⚠ `Mutatie opgelopen rente` closes the per-row result:

        beleggingsresultaat = koersresultaat + opbrengsten + mutatie_opgelopen_rente

    measured on BUS_BepOffensief_Dyn (-1358.33 + 1734.67 + 21.37 = 397.71 exactly). Where
    it is 0 the shorter form holds exactly too (23343.756055 + 1706.29 = 25050.05), which
    is how checking a single portfolio talks you into the wrong rule.

    ⚠ `Kosten` is 0.00 on every portfolio read so far, so its SIGN is unverified — the
    identity cannot tell `- kosten` from `+ kosten` while the term is zero. Parsed and
    stored; kept out of arithmetic until a book with real costs turns up.
    """
    df = pd.read_excel(io.BytesIO(content), engine="xlrd")

    def num(r, col, digits=2):
        """`col` off the row, rounded — or None. Absent column and absent value are the same
        answer here: we did not read a number. Never 0, which would be a claim."""
        v = r.get(col)
        return round(float(v), digits) if pd.notna(v) else None

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "periode": str(r.get("Periode", ""))[:10],
            "beginvermogen": num(r, "Beginvermogen"),
            "stortingen": num(r, "Stortingen"),
            "onttrekkingen": num(r, "Onttrekkingen"),
            "koersresultaat": num(r, "Koersresultaat"),
            "opbrengsten": num(r, "Opbrengsten"),
            "kosten": num(r, "Kosten"),
            "mutatie_opgelopen_rente": num(r, "Mutatie opgelopen rente"),
            "beleggingsresultaat": num(r, "Beleggingsresultaat"),
            "eindvermogen": num(r, "Eindvermogen"),
            "rendement": num(r, "Rendement", 6),
            "cumulatief_rendement": num(r, "Cumulatief rendement", 6),
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
    # A name WE chose. NULL = none chosen, fall back to `name` (AIRS's 24-char code). It sits
    # BESIDE the code rather than replacing it: the code is what you search for in AIRS itself.
    # Never written by the scan — see the migration header; putting it in that payload wipes
    # every chosen name.
    display_name: str | None = None
    # The risk profile this model is offered at — Offensief / Beperkt Offensief / Neutraal /
    # Defensief — DERIVED from AIRS's name, not stored (see `load_portfolios`). `null` means the
    # model is not offered at one: the themed TopSelectie and WTS funds, and
    # Risicodragend/Risicomijdend, which are a different axis. Same classifier the correlation
    # matrix filters on, so the two panels cannot disagree.
    variant: str | None = None
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


class SetDisplayNameRequest(BaseModel):
    # `None` (or "") CLEARS the chosen name and falls back to AIRS's code. Distinguishing the two
    # would be a distinction without a difference here: there is no such thing as a deliberate
    # blank label, and storing "" would render an empty cell that reads as a bug rather than as
    # a choice. (Contrast the link table, where a stored NULL genuinely means "explicitly not a
    # portfolio" and MUST be separable from "never decided".)
    display_name: str | None = None


@router.put("/api/airs/model-portfolios/{portfolio_id}/display-name")
async def airs_set_portfolio_display_name(portfolio_id: int, body: SetDisplayNameRequest):
    """Give a model a human name, or clear it back to AIRS's code with a null/empty value.

    Admin-only by default: `_USER_WRITE_PREFIXES` is empty, so the auth gate 403s a non-admin
    write to any /api/airs path without this needing its own check.
    """
    def _run() -> dict:
        name = (body.display_name or "").strip() or None
        res = (supabase.table("airs_model_portfolio")
               .update({"display_name": name}).eq("id", portfolio_id).execute())
        if not res.data:
            raise HTTPException(404, f"No model portfolio with id {portfolio_id}.")
        return {"ok": True, "id": portfolio_id, "display_name": name}

    return await asyncio.to_thread(_run)


class PortfolioPerfSources(BaseModel):
    """As-of dates of the inputs behind this row's numbers — for per-value traceability on the
    grid. Every field is an already-loaded date SURFACED, not recomputed: the model figures
    (YTD / Since / Sharpe / Sortino) are a yfinance close series converted at FX, over a
    composition we scraped from AIRS, so those three dates are what "as of when" means here."""

    yf_close: str | None = None      # latest yfinance close date behind this model's return
    fx: str | None = None            # latest FX rate date used for the EUR conversion
    model_scan: str | None = None    # when the composition itself was last scraped from AIRS


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
    # Where these numbers came from, as-of when — for per-value traceability on the grid.
    sources: PortfolioPerfSources | None = None


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
    # What to CALL each model: its chosen `display_name`, else AIRS's code. An axis needs a
    # label, so unlike the /portfolios table this cannot render a "—" for an unnamed model.
    labels: list[str]
    # AIRS's own name, aligned to `labels`. The label is for reading; this is for finding the
    # model in AIRS itself once someone has renamed it.
    codes: list[str] = []
    # Each model's risk profile — Offensief / Beperkt Offensief / Neutraal / Defensief — aligned
    # to `labels`, so the matrix can be filtered to one profile and its rows become comparable.
    # `null` means the model is NOT offered at a risk profile (8 of the 42: the themed TopSelectie
    # and WTS funds, and Risicodragend/Risicomijdend, which are a different axis). That is an
    # answer about the product, not a classification failure. See `_airs_portfolio_variant`.
    variants: list[str | None] = []
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

    # Where the PRIMARY portfolio numbers come from: "model" (yfinance reconstruction) or "book"
    # (AIRS's own cumulatief_rendement). The benchmark is yfinance either way.
    source: str = "model"
    # True when a paired AIRS book exists — so the UI can explain a blank 'book' return as "no
    # paired book" rather than a computation failure.
    book_available: bool | None = None
    ytd_from: str | None = None
    since_from: str | None = None
    portfolio_ytd_pct: float | None = None
    # As-of dates behind the numbers, for the per-value provenance ⓘ. `portfolio_as_of` is the
    # yfinance close date (model source) or the AIRS book snapshot date (book source); the benchmark
    # is always yfinance.
    portfolio_as_of: str | None = None
    benchmark_as_of: str | None = None
    # The yfinance model YTD, ALWAYS carried (even when `source=book` makes the primary the book),
    # so the Book-vs-strategy drift tile reads the strategy number regardless of the toggle.
    strategy_ytd_pct: float | None = None
    benchmark_ytd_pct: float | None = None
    ytd_excess_pct: float | None = None
    portfolio_since_pct: float | None = None
    benchmark_since_pct: float | None = None
    since_excess_pct: float | None = None
    ytd_is_since: bool = False

    # ── The BOOK, beside the STRATEGY this modal describes ──────────────────────────────────
    # This modal is the FIXED portfolio (weights, yfinance). The row that opened it is the
    # DYNAMIC book (real positions, AIRS). `book_gap_pct` = strategy − book is the drift between
    # them, and it is the whole reason the two-table split exists.
    #
    # ⚠ `book_comparable` GUARDS THE SUBTRACTION. The book is always the calendar year; the
    # model's YTD is partial for 9 of 28. Where the windows differ, `book_gap_pct` is None and
    # `book_reason` says why — a gap across two windows is not drift.
    book_portefeuille: str | None = None
    book_ytd_pct: float | None = None
    book_as_of: str | None = None            # the AIRS book snapshot date — for the drift tile's ⓘ
    book_comparable: bool | None = None
    book_gap_pct: float | None = None
    book_reason: str | None = None


class PortfolioAllocationSlice(BaseModel):
    """One asset-class slice of the portfolio's OWN composition (no benchmark side).

    AIRS's `categorie` says what a holding INVESTS IN (an equity ETF is AAND, a bond ETF is OBL);
    the ETF flag is the orthogonal wrapper axis. So only EQUITY is split into direct vs ETF — a
    bond ETF is Bonds, not "ETF Bonds". Buckets: Equity | ETF Equity | Bonds | Alternatives | Cash
    (Real estate folds into Alternatives) | Unclassified.
    """
    bucket: str
    pct: float
    # The bucket's value-weighted YTD price return (from the paired book); null when no book.
    return_pct: float | None = None


class BookHoldingDetail(BaseModel):
    """One paired-book holding, for a non-equity sleeve's contribution + currency view.

    `weight_pct` is the holding's OPENING value (beginwaarde) as a share of the whole book, so that
    within ANY bucket, Σ (weight_pct / Σ_bucket weight_pct) · return_pct reproduces that bucket's
    START-weighted return (Σnow/Σstart−1) exactly — the true value change, not the current-value
    weighting that lets a big winner dominate. `currency` is the holding's quote currency (a fair
    first-order FX signal for a bond/ETF sleeve — NOT folded to Unclassified like the fund axes).
    """
    name: str | None = None
    isin: str | None = None
    bucket: str
    currency: str | None = None
    weight_pct: float
    return_pct: float | None = None


class ModelPortfolioAnalysis(BaseModel):
    """A model portfolio's composition beside a benchmark's, on ONE set of buckets.

    Both sides are classified from `asset_grid`'s yfinance attributes, joined by ISIN — the
    portfolio lives in the ISIN world and the benchmark in the `company` world, and putting two
    different sector taxonomies in one chart invents differences that are not there. (All 493
    SP500 members are present in `asset_grid` with a sector, so nothing is lost.)

    ⚠ FUNDS ARE NOT LOOKED THROUGH, and the payload says so rather than pretending. An ETF's
    listing tells you nothing about what it holds — 24 of the 26 held ETFs have a "sector" of
    literally `etf` or `Equity`; an Amsterdam-listed MSCI World ETF is not European exposure; and
    quoted in EUR it still holds mostly USD assets. So every fund folds into "Unclassified" on ALL
    THREE axes — a 40%-ETF portfolio shows a 40% Unclassified bar meaning "we cannot see inside
    this", true and more useful than a confident wrong split.
    """

    portfolio_id: int | None = None    # null for an ad-hoc basket (a stock / group has no id)
    name: str | None = None
    as_of: str | None = None
    benchmark: str
    # Which side the portfolio bars weight by: "model" (nominal strategy weights) or "book" (the
    # paired AIRS book's actual EUR holdings). Only the WEIGHTS change — classification and the
    # benchmark are identical, because the benchmark can only be priced in the yfinance world and
    # a book-vs-yfinance-index comparison would not be a comparison. `weight_note` is set only
    # when "book" was requested but no priced book exists, so the model weights are shown instead.
    weight_basis: str = "model"
    weight_note: str | None = None
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
    # The portfolio's OWN asset-class split, weighted by the active basis (model % or book EUR). A
    # composition read, not a benchmark comparison — so no benchmark side. Empty for an ad-hoc
    # basket (no AIRS `categorie` to split equity ETFs by).
    allocation: list[PortfolioAllocationSlice] = []
    # Per-holding book detail — the source for a non-equity sleeve's contribution breakdown +
    # currency chart (where sector/region/SP500 say nothing). Empty for a basket or an unpaired model.
    book_holdings: list[BookHoldingDetail] = []


@router.get("/api/airs/model-portfolios/{portfolio_id}/analysis",
            response_model=ModelPortfolioAnalysis)
async def airs_model_portfolio_analysis(portfolio_id: int, benchmark: str = "SP500",
                                        weight_by: str = "model", source: str = "model",
                                        bucket: str | None = None):
    """Sector / region / currency split of one model portfolio, beside the benchmark's.

    `weight_by=book` weights the portfolio bars by the paired AIRS book's actual EUR holdings
    instead of the model's nominal weights; the benchmark and the classification are unchanged.

    `source=book` reads the RETURN numbers from AIRS's own book (`cumulatief_rendement` + the
    VOLK per-holding results) instead of the yfinance model reconstruction. The benchmark stays
    yfinance either way, so the two are comparable.

    `bucket` (an allocation label — Equity, Bonds, …) filters the CHART axes to that asset-class
    sleeve; the `allocation` bar itself stays over the whole model so a reader can re-select.
    """
    from routers._airs_holding_isin import BUCKET_ORDER  # noqa: PLC0415
    from routers._airs_portfolio_analysis import (  # noqa: PLC0415
        compute_portfolio_analysis_async,
    )

    basis = weight_by if weight_by in ("model", "book") else "model"
    src = source if source in ("model", "book") else "book"
    bucket_filter = bucket if bucket in BUCKET_ORDER else None
    return await compute_portfolio_analysis_async(portfolio_id, benchmark, basis, src, bucket_filter)


@router.get("/api/airs/model-portfolios/{portfolio_id}/risk-windows",
            response_model=PerformanceResponse)
async def airs_model_portfolio_risk_windows(portfolio_id: int):
    """Whole-portfolio returns+risk over 2/4/8-year windows — the Analyse modal's Risk section.

    The model's holdings priced as ONE value-weighted daily EUR basket (yfinance / `asset_price`),
    so it is the same metric table an instrument or a sleeve gets. yfinance-only by nature (AIRS has
    no daily history); 404 when the portfolio has no priceable holdings."""
    from routers._airs_portfolio_analysis import (  # noqa: PLC0415
        compute_portfolio_risk_windows_async,
    )

    return await compute_portfolio_risk_windows_async(portfolio_id)


@router.get("/api/airs/model-portfolios/{portfolio_id}/price-series",
            response_model=PriceSeriesResponse)
async def airs_portfolio_price_series(portfolio_id: int):
    """The whole portfolio's price-steadiness series (Fundamental → Stock price) — its holdings as
    one value-weighted EUR index. Same shape as the basket / single-instrument endpoints."""
    from routers._airs_portfolio_analysis import portfolio_basket_request  # noqa: PLC0415
    from routers._asset_financials import _basket_price_series  # noqa: PLC0415

    req = await asyncio.to_thread(portfolio_basket_request, portfolio_id)
    return await asyncio.to_thread(_basket_price_series, req)


@router.get("/api/airs/model-portfolios/{portfolio_id}/owner-earnings-stream")
async def airs_portfolio_owner_earnings_stream(portfolio_id: int):
    """SSE: the whole portfolio's blended owner-earnings (Fundamental → Owner earnings), streaming
    per-holding progress then the result — its holdings run through the same basket blender."""
    from fastapi.responses import StreamingResponse  # noqa: PLC0415

    from routers._airs_portfolio_analysis import portfolio_basket_request  # noqa: PLC0415
    from routers._asset_financials import _basket_owner_earnings_events  # noqa: PLC0415

    req = await asyncio.to_thread(portfolio_basket_request, portfolio_id)
    return StreamingResponse(_basket_owner_earnings_events(req), media_type="text/event-stream")


@router.post("/api/airs/basket/analysis", response_model=ModelPortfolioAnalysis)
async def airs_basket_analysis(req: BasketRequest, benchmark: str = "SP500"):
    """Composition + return of an ARBITRARY basket (a single stock, a group) beside the benchmark —
    the same payload as the model-portfolio analysis, so ONE Analyse view serves a stock (a basket
    of one) and a portfolio alike. yfinance only (a basket has no AIRS book)."""
    from routers._airs_portfolio_analysis import (  # noqa: PLC0415
        compute_basket_analysis_async,
    )

    return await compute_basket_analysis_async(req.holdings, benchmark, req.label)


class AttributionName(BaseModel):
    isin: str | None = None
    # The CANONICAL label (asset_grid, joined by ISIN) — the model side and the index side speak
    # one vocabulary here, so the same security cannot appear under two names in one comparison.
    name: str | None = None
    # AIRS's OWN label for the row ("AMD", "Applied"), where there is one — the identity this
    # holding has in AIRS itself. Kept beside `name` rather than replaced by it; index rows and
    # the contributor lists leave it null.
    airs_name: str | None = None
    ticker: str | None = None
    weight_pct: float = 0.0
    return_pct: float | None = None
    contribution_pct: float = 0.0
    # True when this company is held on BOTH sides — in the model AND the benchmark bucket.
    # Matched by ISIN first, then by company, so BOTH "AMD" == "Advanced Micro Devices Inc" (one
    # ISIN, two names) and Alphabet class A == class C (two ISINs, one business) resolve. See
    # `_airs_portfolio_attribution._overlaps` — neither key alone is sufficient.
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
    # "model" (yfinance reconstruction) or "book" (paired AIRS book's actual holdings + returns).
    source: str = "model"
    # Set when there is nothing to attribute (e.g. `source=book` but no paired book).
    note: str | None = None
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
    source: str = "book",
):
    """Brinson-Fachler attribution of one model against a benchmark, over one window.

    ⚠ THE DEFAULT IS `book` — THE BEGINWAARDE START WEIGHTS, NOT THE MODEL'S DESIGN PERCENTAGES.
    An attribution weighted by the design % (a flat 5.00% per name) decomposes a portfolio nobody
    held: it assumes every position opened the year at its target weight and never drifted. Only
    the start weights reproduce the book's realised return, because
    `Σ start_i·ret_i / Σ start_i == (Σ cur − Σ start) / Σ start` is an identity — so with any
    other weighting the "excess" being decomposed is not the excess the book earned.

    `source=model` still gives the yfinance reconstruction of the model's nominal composition,
    which is the right question for an unlinked model — it is just not what the book did.
    """
    from routers._airs_portfolio_attribution import (  # noqa: PLC0415
        compute_attribution_async,
    )

    src = source if source in ("model", "book") else "model"
    return await compute_attribution_async(portfolio_id, benchmark, window, axis, src)


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


def _shape_book_positions(portfolio_id: int) -> ModelPortfolioPositions:
    """The paired AIRS BOOK's own holdings, with AIRS's own per-holding EUR values — the
    book-source twin of `_shape_positions`.

    Where the model path shows the model's composition priced from yfinance (per-SHARE closes),
    this shows what the book ACTUALLY holds, valued by AIRS: `start_price_eur` / `end_price_eur`
    are the Beginwaarde / Huidige waarde (position VALUES over the calendar year, 1 Jan -> the
    snapshot), and `return_pct` is the VOLK price return between them. Rows differ from the model's
    — a book holds a different set than the composition it tracks — which is the point of the toggle.

    ISIN comes from the price-gated name matcher (`resolve_account_isins`), the same bridge every
    other book surface uses. Returns an empty set (never the model's) when no book is paired, so
    the toggle can say "no book" rather than silently showing the wrong thing.
    """
    from deps import IN_CHUNK_SIZE  # noqa: PLC0415
    from routers._airs_account_links import list_account_links  # noqa: PLC0415
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    pf = (supabase.table("airs_model_portfolio").select("id,name")
          .eq("id", portfolio_id).limit(1).execute().data or [])
    pf_name = pf[0]["name"] if pf else str(portfolio_id)

    link = next((a for a in list_account_links()["accounts"]
                 if a.get("model_portfolio_id") == portfolio_id), None)
    if not link:
        return ModelPortfolioPositions(
            portfolio=pf_name, portfolio_id=portfolio_id, datum=None, dates=[], rows=[],
            matched=0, unmatched=0, ytd_from=None, cached_at=None)

    res = resolve_account_isins(link["portefeuille"])
    brows = res.get("rows") or []
    as_of = res.get("as_of")
    jan1 = f"{dt_date.today().year}-01-01"
    # ⚠ START-of-window (Beginwaarde) weights, not current. This table's contract is that weighting
    # the Return column by the percentages reproduces the portfolio return exactly — and that only
    # holds with start weights: current-value weights overweight the winners (a holding that
    # doubled now carries ~2x its starting share), reading +58.75% against the book's true +44.99%
    # price return. Same look-ahead bias the benchmark and the book attribution both avoid.
    total = sum(float(r.get("start_value_eur") or 0) for r in brows) or 1.0

    isins = sorted({r["isin"] for r in brows if r.get("isin")})
    known: set[str] = set()
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        got = (supabase.table("asset_execution").select("isin")
               .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
        known.update(r["isin"] for r in got)

    out: list[ModelPortfolioPosition] = []
    for r in brows:
        isin = r.get("isin")
        cur_v = r.get("current_value_eur")
        start_v = r.get("start_value_eur")
        start_price = end_price = None
        start_date = end_date = None
        ret = None
        if cur_v is not None:
            end_price = float(cur_v)
            end_date = as_of
        if start_v is not None and float(start_v) > 0 and cur_v is not None:
            # Beginwaarde / Huidige waarde are POSITION VALUES, not per-share prices — the return
            # between them is the VOLK price return, exactly what the book figure weights together.
            start_price = float(start_v)
            start_date = jan1
            ret = (float(cur_v) / float(start_v) - 1.0) * 100.0
        out.append(ModelPortfolioPosition(
            fonds=r.get("holding_name"),
            isin=isin,
            percentage=((float(start_v) / total * 100.0)
                        if start_v and float(start_v) > 0 else None),
            valuta=r.get("currency"),
            categorie=r.get("asset_class"),
            sector=r.get("sector"),
            regio=None,
            known_instrument=bool(isin and isin in known),
            currency=r.get("currency"),
            last_close=as_of,
            start_date=start_date,
            start_price_eur=start_price,
            end_date=end_date,
            end_price_eur=end_price,
            return_pct=ret,
        ))

    matched = sum(1 for r in out if r.known_instrument)
    return ModelPortfolioPositions(
        portfolio=pf_name, portfolio_id=portfolio_id, datum=as_of, dates=[], rows=out,
        matched=matched, unmatched=len([r for r in out if r.isin]) - matched,
        ytd_from=jan1, cached_at=None)


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
    source: str = "model",
):
    """One model portfolio's positions — the XLS export that DOES carry an ISIN (the AIRS
    *holdings* sheet does not; it has only a fund name).

    `source=book` instead returns the paired AIRS BOOK's own holdings, with AIRS's own per-holding
    EUR values (Beginwaarde / Huidige waarde) — a different set of rows than the model composition,
    and never cached (the caching below is for the model XLS path).

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
    if source == "book":
        return await asyncio.to_thread(_shape_book_positions, portfolio_id)

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


@router.post("/api/airs/portfolios/{portefeuille}/refresh")
async def airs_portfolio_refresh(portefeuille: str):
    """Re-scan ONE portfolio's AIRS Rendement + Vermogensoverzicht and store both — the per-row
    Refresh on the overview table. Awaited (a few seconds: two downloads), so the client can
    re-fetch the row on success. Serialized against the full scan via the module lock; returns
    `{status: busy}` if a fleet refresh is in flight."""
    from airs_vermogen import refresh_one_portfolio  # noqa: PLC0415

    return await asyncio.to_thread(refresh_one_portfolio, portefeuille)


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


class AirsAccount(BaseModel):
    """One AIRS account's YEAR, on AIRS's own numbers.

    ⚠ EVERY MONEY FIELD HERE IS THE YEAR'S, SUMMED ACROSS AIRS'S MONTHLY ROWS. One ATT row is
    one MONTH — reading the freshest as "the year" served AITopSelectie's July price result of
    -130,063 where the year made +420,225: wrong sign, third of the size, beside a +42% YTD.
    `_airs_accounts._year_perf` does the assembly; read it before adding a field here.

    ⚠ `ytd_pct` IS `cumulatief_rendement` — AIRS's own, flow-aware, and never
    `end_value_eur / begin_value_eur - 1`.

    ⚠ `latest_month_pct` IS NOT A RIVAL YTD. It is AIRS's `rendement` off the newest row: the
    latest month's return. It was once served as `value_ratio_pct` and described as "the wrong
    number", on the theory that deposits inflated it — but AITopSelectie has zero deposits in
    every month of 2026 and still reads -5.85% there against +46.12% for the year. Different
    windows, both correct.
    """

    portefeuille: str
    periode: str | None = None          # the newest month's END
    as_of: str | None = None            # the holdings snapshot we hold, if any
    months: int | None = None           # monthly rows behind these figures
    begin_value_eur: float | None = None       # the YEAR's opening (the first month's)
    end_value_eur: float | None = None
    ytd_pct: float | None = None        # cumulatief_rendement — the answer
    latest_month_pct: float | None = None      # rendement, newest row — a different window
    price_result_eur: float | None = None      # koersresultaat — the "price gains", year
    income_eur: float | None = None            # opbrengsten — dividends/coupons, year
    investment_result_eur: float | None = None  # beleggingsresultaat, year
    costs_eur: float | None = None             # kosten (0 everywhere measured; sign unverified)
    accrued_interest_change_eur: float | None = None   # mutatie opgelopen rente
    deposits_eur: float | None = None          # stortingen
    withdrawals_eur: float | None = None       # onttrekkingen
    # AIRS's own identity, asserted not assumed:
    #   end - begin - deposits + withdrawals == investment_result_eur
    # A month missing from our copy shortens the year while still looking like one.
    residual_eur: float | None = None
    reconciles: bool | None = None
    holdings: int | None = None


class AirsAccountHolding(BaseModel):
    holding_name: str
    quantity: float | None = None
    currency: str | None = None
    weight: float | None = None
    start_value_eur: float | None = None       # Beginwaarde lopend jaar EUR
    current_value_eur: float | None = None     # Huidige waarde EUR
    ytd_return_eur: float | None = None
    # ⚠ None = UNDEFINED, not 0%. A position not held at the year's open (or a cash line) has a
    # Beginwaarde of 0; dividing by it is infinite and calling it flat is a claim.
    ytd_return_pct: float | None = None
    ytd_return_local_pct: float | None = None
    # AIRS's OWN figures — populated by scans from 2026-07-17 on; NULL in older snapshots.
    cost_basis_local: float | None = None      # Kostprijs lopend jaar
    current_price_local: float | None = None   # Huidige koers
    airs_weight: float | None = None           # Weging
    fund_result_eur: float | None = None       # Fondsresultaat — the performance leg (EUR)
    fx_result_eur: float | None = None         # Valutaresultaat — the FX leg (EUR)
    airs_result_pct: float | None = None       # Resultaat in % (a PERCENT, not a fraction)
    # ── The DIRECT result: what the instrument PAID, from the Mutaties journal (`MUT`) ──────────
    # ⚠ TWO COLUMNS, NOT A NET. `dividend_eur` is GROSS; `dividend_tax_eur` is the withholding,
    # NEGATIVE as AIRS books it, so net is their sum. They are separate because "this US name lost
    # 15% and this Dutch one lost nothing" is a fact about the holding, not a rounding detail.
    # ⚠ None, never 0.0 — "paid nothing" and "this book's journal has not been scanned" are
    # different claims and only one of them is safe to make.
    dividend_eur: float | None = None
    dividend_tax_eur: float | None = None
    dividend_payments: int | None = None
    # This book own model weight, from its OWN MODEL report — no fixed<->dynamic pairing, so
    # no name guess that could file a book money under another strategy. None = the model does
    # not name this holding (drift), never 0%.
    model_pct: float | None = None
    # AIRS's own `Werkelijk percentage` from the same sheet — what the book ACTUALLY holds, as the
    # model report computes it. ⚠ Not the same field as `weight`/`airs_weight`, which come from the
    # Vermogensoverzicht: two reports, two dates, so they can legitimately differ.
    model_actual_pct: float | None = None
    model_drift_pct: float | None = None


class AirsAccountDetail(BaseModel):
    """One account's freshest snapshot.

    ⚠ THE ROWS DO NOT SUM TO `ytd_pct`, AND THAT IS CORRECT. Each row is a PRICE return (AIRS
    restates `Beginwaarde lopend jaar` to the current quantity, so a purchase does not contaminate
    it). The account's figure is flow-aware AND includes `income_eur`, which no price return
    contains. The /portfolios MODEL view has the opposite property — its holdings weight exactly
    to its total — so a reader arriving from there will expect these to tie.
    """

    portefeuille: str
    as_of: str | None = None
    ytd_pct: float | None = None
    price_result_eur: float | None = None
    income_eur: float | None = None
    # ⚠ INCOME THE TABLE CANNOT SHOW. A position sold during the year paid real dividends and has
    # no holding row left to carry them — measured, 3 of 27 funds and EUR 1,010 of
    # BUS_Neutraal_Dyn's EUR 12,031. Summing the Direct result column and calling it the book's
    # income understates it, so the difference is stated instead of hidden.
    dividend_sold_eur: float | None = None
    dividend_sold_tax_eur: float | None = None
    dividend_sold_funds: list[str] | None = None
    rows: list[AirsAccountHolding] = []


@router.get("/api/airs/accounts", response_model=list[AirsAccount])
async def airs_accounts():
    """The AIRS ACCOUNTS — what the books actually made, on AIRS's own EUR values.

    A different object from the model portfolios: a model is a COMPOSITION (weights), which AIRS
    has nothing to value — of 58 models and 39 valued accounts, the overlap is zero. The models
    answer "would this strategy work" (and need yfinance, since nothing else can price a set of
    weights); these answer "what did this book make", and AIRS is the system of record.
    """
    from routers._airs_accounts import list_accounts_async  # noqa: PLC0415

    return await list_accounts_async()


@router.get("/api/airs/accounts/{portefeuille}/holdings", response_model=AirsAccountDetail)
async def airs_account_holdings(portefeuille: str):
    """One account's positions, with AIRS's own EUR values — including the Leonteq certificates
    Yahoo cannot price at all (TOPS_OFF_BEH_DYN: AIRS values 7 of 7 where the yfinance path
    prices 0 of 9)."""
    from routers._airs_accounts import account_holdings_async  # noqa: PLC0415

    return await account_holdings_async(portefeuille)


class AirsAccountModelLink(BaseModel):
    """One account, and the model it runs.

    `source` says where the pairing came from and is the whole point of the row:
      manual — a human decided (always wins, including a decision of "none")
      guess  — an exact stem match, recomputed on every read, never stored
      none   — nobody has decided and we will not guess
    """

    portefeuille: str
    ytd_pct: float | None = None
    months: int | None = None
    model_portfolio_id: int | None = None
    model_name: str | None = None
    model_positions: int | None = None
    source: str
    reason: str | None = None


class AirsModelChoice(BaseModel):
    id: int
    name: str
    positions: int


class AirsAccountModelLinks(BaseModel):
    accounts: list[AirsAccountModelLink]
    models: list[AirsModelChoice]


class AirsAccountLinkRequest(BaseModel):
    # None IS a value: "this account runs none of our models" (every benchmark). Distinct from
    # DELETE, which forgets the decision and lets the guess speak again.
    model_portfolio_id: int | None = None
    note: str | None = None


class AirsPortfolioOverview(BaseModel):
    """A portfolio: your name for it, AIRS's numbers for it.

    ⚠ `link_source` IS PART OF THE ROW, NOT A DETAIL. `name` comes from the Fixed portfolio this
    book is paired with, and 27 of 28 pairings are an unconfirmed name match. A wrong pairing
    puts a real book's money under another strategy's name, and — because the risk variants of a
    strategy hold the SAME instruments — nothing else on the row would look wrong.
    """

    name: str
    description: str | None = None
    dynamic_portefeuille: str
    fixed_name: str | None = None
    fixed_portfolio_id: int | None = None
    fixed_type: str | None = None
    isins: int | None = None            # None = unlinked; NOT 0
    link_source: str
    link_reason: str | None = None
    as_of: str | None = None
    periode: str | None = None
    months: int | None = None
    ytd_pct: float | None = None
    latest_month_pct: float | None = None
    price_result_eur: float | None = None
    income_eur: float | None = None
    investment_result_eur: float | None = None
    deposits_eur: float | None = None
    withdrawals_eur: float | None = None
    begin_value_eur: float | None = None
    end_value_eur: float | None = None
    holdings: int | None = None
    reconciles: bool | None = None
    residual_eur: float | None = None


@router.get("/api/airs/portfolios/overview", response_model=list[AirsPortfolioOverview])
async def airs_portfolios_overview():
    """Every AIRS book in one table: named by the Fixed portfolio it runs, valued by AIRS.

    The Fixed side has the ISINs and your nickname and AIRS values none of it; the Dynamic side
    has the money and no ISIN. Overlap between the two: zero. This is the pair, composed.
    """
    from routers._airs_overview import list_overview_async  # noqa: PLC0415

    return await list_overview_async()


class AirsHoldingIsin(BaseModel):
    """One account holding, with the ISIN we believe it is — and how much to believe it.

    ⚠ `verdict` IS THE FIELD THAT MATTERS, AND `name_score` IS NOT.
      ok             the implied price agrees with that ISIN's own close (FX-converted)
      price_mismatch it does NOT — the ISIN is not what the book holds, or the book drifted
      unpriced       we have no series for it, so there is NOTHING confirming the name match
      unmatched      NO ISIN: the model has no position for this holding
      cross_listed   the prices differ, and they are SUPPOSED to — this ISIN's execution row is
                     deliberately served by another instrument (`asset_isin_alias`), e.g. an ADR
                     priced from the main company's listing. Not a fault, and not a pass either. at all

    `unpriced` is not a pass. The name matched and nothing checked it — which for a fund is
    exactly where the Acc/Inc share-class trap lives.

    ⚠ `unmatched` AND `price_mismatch` ARE OPPOSITE FINDINGS, NOT DEGREES OF ONE. A mismatch means
    the row pairing is RIGHT and the ISIN on it is wrong (a share class, a venue) — a finding
    about the model. `unmatched` means the pairing itself was refused: the name says a different
    instrument and the price independently agrees, which is what a STALE model snapshot looks like
    when the book has since swapped a position. `rejected_isin`/`rejected_fonds` name the leftover
    we declined; re-scan the model portfolio to fix it.
    """

    holding_name: str
    lines: int = 1                 # >1 = AIRS billed this instrument on several rows
    # The asset-class label — Equity | Equity ETF | Bonds | Alternatives | Cash | Unclassified
    # (Unclassified = genuinely unsure). ⚠ From the ASSET GRID and the name only: AIRS's own
    # `categorie` came from the paired model position and went with the pairing (2026-07-23).
    bucket: str | None = None
    # True when the bucket above came from a MANUAL override (asset_bucket_override), not the
    # calculated class — so the UI can badge it and offer "revert to Auto".
    bucket_overridden: bool | None = None
    # Geography straight from the execution instrument's yfinance fields (asset_grid), joined by
    # ISIN. `region` is the MSCI ACWI region. ⚠ For an ETF these describe its LISTING, not what it
    # holds — the grid cannot look inside a fund.
    sector: str | None = None
    country: str | None = None
    continent: str | None = None
    region: str | None = None
    is_etf: bool | None = None
    quantity: float | None = None
    currency: str | None = None
    weight: float | None = None
    current_value_eur: float | None = None
    start_value_eur: float | None = None
    ytd_return_eur: float | None = None
    isin: str | None = None
    # WHERE the identity came from. The three are not equally strong and the digits look the same:
    #   book     AIRS's own `ISIN-code` on the holding — exact, nothing inferred
    #   override supplied BY HAND, for a row AIRS gives no ISIN for
    # None when the row has no ISIN at all. There is no third source: the name match is gone.
    isin_source: str | None = None
    # True = the ISIN was supplied BY HAND (airs_holding_isin_override) because the model has no
    # position for this holding. A pinned identity is not a match and must not read as one — but it
    # is still price-checked, so `verdict` means exactly what it means on every other row.
    isin_overridden: bool | None = None
    isin_override_note: str | None = None
    implied_price_eur: float | None = None
    our_price_eur: float | None = None
    price_ratio: float | None = None
    verdict: str
    our_instrument: str | None = None
    # Set when this ISIN is deliberately served by ANOTHER ISIN's instrument (an ADR priced from
    # the main company's listing). ⚠ The two do not trade at the same number — TSMC is 1 ADR = 5
    # ordinary shares — so a price difference on such a row is expected, not a finding.
    served_by: str | None = None


class AirsHoldingSegment(BaseModel):
    """One asset class within a portfolio: the exposure, and what it returned.

    ⚠ `return_pct` AND `weight_pct` DO NOT COVER THE SAME HOLDINGS. A holding with no opening
    value has an undefined return but real exposure, so it counts in the weight and not in the
    return — otherwise its whole value reads as gain (cash is exactly this, and so is a short:
    Nestle India at -3,504 shares). `priced_value_eur` states how much the return spans.

    ⚠ It is a PRICE return, like the rows it is built from: no income, not flow-aware. The
    segments do not sum to the portfolio's own figure.
    """

    asset_class: str
    holdings: int
    value_eur: float | None = None
    # Sums the Start column beneath it — including the zeros of holdings with no opening value.
    # So value - start != gain wherever such a holding sits: the gap is exposure, not gain.
    start_value_eur: float | None = None
    weight_pct: float | None = None
    gain_eur: float | None = None
    # The gain split into its two legs, summed across the segment — the performance (stock) leg and
    # the currency leg. NULL until a scan from 2026-07-17 on has populated the per-holding figures.
    fund_eur: float | None = None        # Σ Fondsresultaat — the performance leg (EUR)
    fx_eur: float | None = None          # Σ Valutaresultaat — the FX leg (EUR)
    return_pct: float | None = None
    priced_value_eur: float | None = None
    etf_value_eur: float | None = None   # ETFs are counted here, never bucketed as a sibling


class AirsModelPositionLeftover(BaseModel):
    fonds: str | None = None
    isin: str | None = None
    percentage: float | None = None


class AirsAccountIsins(BaseModel):
    portefeuille: str
    model_name: str | None = None
    model_source: str | None = None
    as_of: str | None = None
    reason: str | None = None
    rows: list[AirsHoldingIsin] = []
    segments: list[AirsHoldingSegment] = []
    unmatched_model_positions: list[AirsModelPositionLeftover] = []


@router.get("/api/airs/accounts/{portefeuille}/isins", response_model=AirsAccountIsins)
async def airs_account_isins(portefeuille: str):
    """An account's holdings with an ISIN attached to each, price-checked.

    The account has the money and no ISIN; its model has the ISINs and nothing AIRS values.
    This joins them row-by-row inside the pair confirmed on `/account-model-links`, and then
    REFUSES TO TRUST ITS OWN NAME MATCH: every row is checked against the instrument's own
    close, because a name cannot see a share class (IE00BNDS1P30 vs IE00BNDS1Q47 are both
    "Vanguard ESG Global Corporate Bond UCITS ETF EUR Hedged" — Acc and Inc, €4.79 vs €3.99,
    and they compound differently).
    """
    from routers._airs_holding_isin import resolve_account_isins_async  # noqa: PLC0415

    return await resolve_account_isins_async(portefeuille)


class HoldingIsinOverride(BaseModel):
    holding_name: str
    # The ISIN to pin, or null/empty to CLEAR the override (back to matching against the model).
    isin: str | None = None
    note: str | None = None


# An ISIN is 12 chars: 2-letter country, 9 alphanumeric, 1 check digit. Checked because this field
# is hand-typed, and a malformed value would sit in the table looking like an identity while
# matching no instrument — the row would read "we know what this is" and price nothing.
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


@router.post("/api/airs/holding-isin-override")
async def set_holding_isin_override(body: HoldingIsinOverride):
    """Pin (or clear) the ISIN of an account holding the model has no position for.

    Keyed by holding NAME, so one entry fixes every book that holds it (the measured case appears
    in four). ⚠ It decides IDENTITY ONLY: the pinned ISIN is price-checked like any other, so a
    wrong one comes back `price_mismatch` rather than being trusted because a human typed it.
    """
    name = (body.holding_name or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="holding_name is required")
    isin = (body.isin or "").strip().upper() or None
    if isin and not _ISIN_RE.match(isin):
        raise HTTPException(status_code=422, detail=f"{isin!r} is not a well-formed ISIN")

    def _apply() -> None:
        if isin is None:
            supabase.table("airs_holding_isin_override").delete().eq("holding_name", name).execute()
        else:
            supabase.table("airs_holding_isin_override").upsert(
                {"holding_name": name, "isin": isin, "note": (body.note or "").strip() or None,
                 "updated_at": datetime.now(UTC).isoformat()},
                on_conflict="holding_name").execute()

    await asyncio.to_thread(_apply)
    return {"holding_name": name, "isin": isin}


class AssetBucketOverride(BaseModel):
    isin: str
    # The Class to pin, or null/empty to CLEAR the override (revert to the calculated class).
    bucket: str | None = None


@router.post("/api/airs/asset-bucket-override")
async def set_asset_bucket_override(body: AssetBucketOverride):
    """Manually pin (or clear) a holding's Class. Keyed by ISIN — a property of the instrument,
    remembered forever, and it beats the calculated `classify_bucket`. A null/empty bucket deletes
    the override (revert to Auto). Returns `{isin, bucket}` (bucket null when cleared)."""
    from routers._airs_holding_isin import BUCKET_ORDER  # noqa: PLC0415

    isin = (body.isin or "").strip()
    if not isin:
        raise HTTPException(status_code=422, detail="isin is required")
    bucket = (body.bucket or "").strip() or None

    def _apply() -> None:
        if bucket is None:
            supabase.table("asset_bucket_override").delete().eq("isin", isin).execute()
        else:
            supabase.table("asset_bucket_override").upsert(
                {"isin": isin, "bucket": bucket, "updated_at": datetime.now(UTC).isoformat()},
                on_conflict="isin").execute()

    if bucket is not None and bucket not in BUCKET_ORDER:
        raise HTTPException(status_code=422,
                            detail=f"bucket must be one of {BUCKET_ORDER} (or null to clear)")
    await asyncio.to_thread(_apply)
    return {"isin": isin, "bucket": bucket}


@router.get("/api/airs/account-model-links", response_model=AirsAccountModelLinks)
async def airs_account_model_links():
    """Which MODEL is each AIRS ACCOUNT running — decided, guessed, or neither.

    This is the only bridge between the ISINs (models have them, and AIRS values nothing) and
    the money (accounts have it, and carry no ISIN). It cannot be derived: the holdings do not
    identify the model — BUS_FTS_Bepoff/DEF/NEU_AFS hold the IDENTICAL 27 ISINs — so the name
    is the only discriminator, and the name is four conventions and a typo. Hence a guess that
    refuses rather than approximates, plus a stored human decision.
    """
    from routers._airs_account_links import list_account_links_async  # noqa: PLC0415

    return await list_account_links_async()


@router.put("/api/airs/account-model-links/{portefeuille}", response_model=dict)
async def set_airs_account_model_link(portefeuille: str, body: AirsAccountLinkRequest):
    """Record which model an account runs. `model_portfolio_id: null` means "explicitly none"."""
    from routers._airs_account_links import set_account_link_async  # noqa: PLC0415

    return await set_account_link_async(portefeuille, body.model_portfolio_id, body.note)


@router.delete("/api/airs/account-model-links/{portefeuille}", response_model=dict)
async def clear_airs_account_model_link(portefeuille: str):
    """Forget the decision — the guess speaks again. NOT the same as storing "none"."""
    from routers._airs_account_links import clear_account_link_async  # noqa: PLC0415

    return await clear_account_link_async(portefeuille)

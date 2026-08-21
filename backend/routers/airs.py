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
import gzip
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
from fastapi import APIRouter, File, HTTPException, Request, Response, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from airs_scanner import (
    download_portfolio_sync,
    count_model_portfolio_holdings_sync,
    fetch_model_portfolios_sync,
    fetch_portfolio_positions_sync,
    scan_portfolios_sync,
)
from deps import IN_CHUNK_SIZE, supabase
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


@router.post("/api/airs/model-portfolios/scan/job")
async def airs_model_portfolios_scan_job():
    """The model-portfolio scan as a CANCELLABLE JOB — phase two of the portfolios page's
    "Refresh all".

    ⚠⚠ THIS IS THE HALF OF THAT BUTTON THAT RAN BLIND. Phase one (the account scan) has been a job
    since 2026-08-13: a toast with `i/n`, the account in flight, a working Cancel, and it survives a
    reload. Phase two — this — was `runSSE` straight into `console.warn`, so for the MINUTES it runs
    (an edit-page GET plus an XLS download for each of ~58 fixed portfolios) the only thing on
    screen saying anything was happening at all was the button's own label. Navigate away and the
    work was invisible; reload and it was unrecoverable; there was no way to stop it. One button
    reporting two ways, and the slower way was the silent one.

    ⚠ THE SCAN ITSELF IS UNCHANGED — the same `fetch_model_portfolios_sync` +
    `count_model_portfolio_holdings_sync` the SSE endpoint above and the scheduler both call, with
    two optional hooks. A streaming copy for the job path is exactly the drift `scan_one`'s
    docstring warns about.

    ⚠ CANCEL STOPS BETWEEN PORTFOLIOS, and the summary says where. A portfolio's XLS is downloaded,
    counted and persisted as a unit; everything already counted is kept.

    ⚠ `busy` IS AN ANSWER, NOT AN ERROR — and here it guards a REAL hazard rather than a
    bookkeeping one: this shares ONE authenticated AirSPMS session with the account scan, which
    must not be driven by two threads at once. ⚠ IT IS A PARTIAL GUARD AND SAYING SO IS THE POINT:
    `_LOCK` is held by `run_airs_vermogen_refresh_sync` and `refresh_one_portfolio`, so this cannot
    collide with either — but the scheduler's own model-scan ticks (`scheduler.py`) do not take it,
    and neither does the SSE endpoint above, so those two can still overlap this. Non-blocking, so
    it can never deadlock the session it exists to protect.
    """
    import jobs as job_registry  # noqa: PLC0415
    from jobs import JobCancelled  # noqa: PLC0415

    from airs_vermogen import _LOCK  # noqa: PLC0415

    def _work(ctx) -> str:
        if not _LOCK.acquire(blocking=False):
            return "Another AIRS scan is already running — nothing was re-read"
        # ⚠ THE BAR'S POSITION IS CARRIED, NOT RE-DERIVED PER EVENT. `ctx.progress` writes
        # `done`/`total` onto the job every time, so a narration line emitted mid-count with a
        # freshly-zeroed pair would blank a bar that is genuinely at 40/58 — and the next `count`
        # would snap it back. The list phase legitimately has no denominator; 0 renders as
        # indeterminate (`JobToaster`: `total > 0 ? pct : null`), which is what it is.
        at = {"done": 0, "total": 0}

        def on_event(kind: str, **kw) -> None:
            msg = str(kw.get("message") or "")
            # ⚠ `n` ARRIVES ONE LINE BEFORE THE FIRST `i`. The count phase announces its
            # denominator ("counting holdings for 58 fixed portfolios") before it downloads
            # anything, so adopting it from ANY event that carries it puts a real 0/58 on the bar
            # for the ~10s of the first XLS instead of an indeterminate one.
            if kw.get("n"):
                at["total"] = int(kw["n"])
            if kind == "count":
                at["done"] = int(kw.get("i") or 0)
            elif kind == "portfolios":
                # It carries the whole roster as a payload; the job wants the one number.
                msg = f"{kw.get('count') or 0} portfolios listed — counting holdings…"
            # ⚠ NO `error` BRANCH, AND THAT IS NOT AN OVERSIGHT. Neither of these two scanners
            # reports a fault as an EVENT — they raise, which the job runner already turns into a
            # `failed` card. (The Front-Office scraper elsewhere in `airs_scanner` does emit one;
            # this path does not, and handling a kind that never arrives reads as coverage.)
            if msg:
                ctx.progress(at["done"], at["total"], msg)

        try:
            # ⚠ SAME REASON AS THE ACCOUNT SCAN'S FIRST LINE. `_session.get_response` logs in
            # lazily, so the first narrated event is the first LIST PAGE — everything before it
            # (launching the browser, signing in) would sit under "starting…". Naming the slow
            # thing up front is what separates "this takes a minute" from "this is stuck".
            ctx.progress(0, 0, "signing in to AirSPMS and reading the model-portfolio list…")
            rows = fetch_model_portfolios_sync(on_event)
            # ⚠ STORED BEFORE THE SLOW HALF, exactly as the SSE path does it. The list is complete
            # work in its own right — it is what gives every account its readable name — and a
            # cancel during the count must not throw it away.
            store.save_portfolios(rows)
            count_model_portfolio_holdings_sync(
                rows, on_event,
                on_positions=store.save_positions,
                on_error=store.save_positions_error,
                should_stop=lambda: ctx.cancelled,
            )
        finally:
            _LOCK.release()

        counted = sum(1 for p in rows if p.get("holdings") is not None)
        if ctx.cancelled:
            # ⚠ RAISED, NOT RETURNED — a returned string is a `done` job, and `done` is a word other
            # code ACTS ON. See the same fix on the account job: returning here would paint a green
            # card over a run that stopped early. `JobCancelled`'s message becomes the summary, so
            # nothing is lost by raising it.
            raise JobCancelled(f"Cancelled — {at['done']}/{at['total']} counted, "
                               f"{len(rows)} portfolios stored")
        return f"{len(rows)} portfolios, {counted} with a holdings count"

    job, reused = job_registry.start(
        "airs.models.scan", "Scan model portfolios", _work)
    return {"job_id": job.id, "label": "Scan model portfolios", "already_running": reused}


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


async def _portfolio_refresh_stream(portfolio_id: int):
    """The five refresh steps, one SSE line each — a worker thread pushes, the async side drains.
    Same shape as the model-portfolio scan, and for the same reason: an AIRS scrape plus a paced
    Yahoo call per holding is tens of seconds, which is not a request."""
    from routers._airs_full_refresh import refresh_portfolio_fully  # noqa: PLC0415

    q: thread_queue.Queue = thread_queue.Queue()

    def emit(msg_type: str, **kw):
        q.put(sse_event({"type": msg_type, **kw}))

    def run():
        try:
            # ⚠ BOTH HALVES, THROUGH THE ONE FUNCTION. This stream used to run the model half only
            # (composition -> instruments -> FX -> prices -> recompute) while /management-dashboard's
            # button ran the book half only, so "Refresh" meant different work on the two pages and
            # the Analyse modal inherited whichever one opened it.
            # ⚠ THE LINES ARE THE SAME LINES. `refresh_portfolio_fully` relays each half's own
            # narration, so the five model phases still arrive one SSE frame each and the book's
            # per-report lines join them — nothing here had to learn a second vocabulary.
            emit("done", summary=refresh_portfolio_fully(None, portfolio_id, on_event=emit))
        except Exception as e:  # noqa: BLE001 — surface it, don't 500 a stream mid-flight
            q.put(sse_message("error", f"{type(e).__name__}: {e}"))
        finally:
            q.put(None)

    threading.Thread(target=run, daemon=True).start()
    while True:
        item = await asyncio.to_thread(q.get)
        if item is None:
            break
        yield item


@router.get("/api/airs/model-portfolios/{portfolio_id}/refresh")
async def airs_model_portfolio_refresh(portfolio_id: int):
    """Re-acquire EVERY input behind one model's YTD, then rebuild the number. Streamed.

    A YTD has five inputs and only the first comes from AIRS:

        1. composition   AirSPMS                 weights + ISINs + the effective date
        2. instruments   Yahoo / OpenFIGI        ISIN -> symbol, currency (queue-paced)
        3. FX            ECB / pegs / Yahoo      rates covering the window — BOTH directions
        4. prices        Yahoo                   each holding's series brought current
        5. recompute     ours                    the YTD, with the per-leg arithmetic

    ⚠ WHICH IS WHY "REFRESH FROM AIRS" ALONE CANNOT FIX A WRONG RETURN. The per-row button
    re-scrapes step 1 and nothing else, so a disagreement caused by a missing price series or a
    short FX history survives any number of presses. This runs all four fetchable steps and then
    prints the arithmetic, so the input that differs is visible rather than inferred.

    ⚠ STEP 3 IS THE ONE NOTHING ELSE DOES. `sync_fx_rates_to_db` only extends FORWARD, so a
    currency whose stored history STARTS after the window opens is never repaired — and a
    holding with no rate on or before its opening bar is dropped, silently, which renormalises
    the return over the survivors and reads HIGH.
    """
    return StreamingResponse(
        _portfolio_refresh_stream(portfolio_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/airs/model-portfolios/{portfolio_id}/ytd-explain")
async def airs_model_portfolio_ytd_explain(portfolio_id: int, year: int | None = None):
    """The full derivation behind ONE model's YTD — for diffing two deployments.

    Same figure as the grid, by construction: it INSTRUMENTS `compute_portfolio_performance`
    rather than recomputing, and asserts that the per-leg contributions sum to the YTD
    (`portfolio.reconciles`). Returns three levels, and they are ordered by how far upstream the
    cause of a discrepancy would be:

      `load`      — what this deployment fetched: price transport (COPY vs paged PostgREST),
                    the price/FX windows, and the freshest close anywhere in the load. A whole
                    environment being a week stale is ONE date here, not 24 leg rows.
      `portfolio` — the composition's effective date, the anchor it implies, the weight that
                    could be priced, and the coverage the return was renormalised over.
      `legs`      — one row per composition line, by contribution: weight, the mark it was
                    bought at (date + EUR price, flagged if interpolated), the close it is
                    marked to, its EUR return and its contribution in percentage points.

    Untyped on purpose — it is a debug surface, so the payload can gain fields without a
    regenerated contract. Read-only: it prices the fleet exactly as the grid does and writes
    nothing.
    """
    from routers._airs_portfolio_perf import (  # noqa: PLC0415
        explain_portfolio_ytd_async,
    )

    return await explain_portfolio_ytd_async(portfolio_id, year)


class CorrelationInstrument(BaseModel):
    """One instrument that fed the correlation matrices, and how it was priced.

    ⚠ THREE STATES, AND COLLAPSING ANY TWO WOULD MISREAD THE MATRIX. Measured 2026-08-10 over the
    44 listed models and their 245 distinct ISINs:

        direct       230   an `asset_execution` with a yfinance series, EUR-converted per date
        lookthrough    9   a Leonteq certificate that IS another model; priced from that model's
                           own curve, because the certificate has no price of its own to fetch
        unpriced       6   no series at all — its weight is what the 60% coverage floor is
                           measured over, so these rows are the reason a portfolio can be refused

    The look-through nine (Star Selection, and the Europa/AI/Dividend/Familie/Merken/Momentum/
    Azie/Vastgoed TopSelectie certificates) look unpriceable in the database and are not. A table
    that showed only "priced / not priced" would report the largest of them — Star Selection, held
    by 12 models — as missing data.
    """

    isin: str
    # AIRS's own name for the line. This table is read against the AIRS model, so its vocabulary
    # wins; `asset_name` is ours, kept for the rows where the two disagree.
    name: str | None = None
    asset_name: str | None = None
    symbol: str | None = None
    currency: str | None = None
    analysis_id: int | None = None
    state: str                       # direct | lookthrough | unpriced
    # Which key in `series.values` charts this row. `null` for an unpriced instrument — there is
    # no series, and pointing at an empty one would draw a flat line where the answer is "none".
    series_key: str | None = None
    # The model a certificate wraps. Set even when that model could not be priced either, because
    # "wraps AI-TopSelectie, which is itself under-covered" is a different fact from "unknown".
    linked_portfolio_id: int | None = None
    linked_label: str | None = None
    # ⚠ EUR for a real listing, an INDEX BASED AT 100 for a look-through — not the same kind of
    # number, so the chart must label them differently. `null` when there is nothing to plot.
    unit: str | None = None
    # ⚠ THE VENUE'S MEDIAN DAILY TRADED VALUE, IN EUR — the column that tells you whether to
    # believe the row above it. A near-untraded listing still yields 251 bars a year, so nothing
    # else here would look wrong; its closes are simply stale against the real market, and a
    # correlation of DAILY RETURNS is the statistic that damages most. See `_median_adv` for the
    # measured case (Hermès on Hanover, EUR 4,946/day, held by 19 of 44 models).
    med_adv_eur: float | None = None
    # ⚠ WHERE THE NUMBERS CAME FROM — TWO VENDORS, NOT ONE. `price_source` is yfinance for every
    # priced row on this page: GuruFocus (`metric_data`) prices the /benchmarks index and the
    # momentum engine and NEVER enters this path, because the AIRS books live in the ISIN/asset
    # world. `fx_source` is the second vendor a "which source?" question usually forgets — a EUR
    # level for a USD holding is a yfinance close multiplied by an ECB rate. `null` on a EUR
    # holding (no conversion occurs); "per holding" on a look-through, which is a basket whose
    # constituents each convert on their own rate.
    price_source: str | None = None
    fx_source: str | None = None
    in_portfolios: int = 0           # DISTINCT models (one model may list an ISIN twice)
    weight_pct_sum: float = 0.0      # Σ of its weights across those models
    observations: int = 0
    first_date: str | None = None
    last_date: str | None = None


class CorrelationSeries(BaseModel):
    """Every charted series on ONE shared date axis — see `_series_block` for the measurement
    that chose this encoding over the obvious `[[date, value], …]` (452 KB raw against 1,270 KB).

    `values[key][i]` is the level on `dates[i]`, or `null` for a day that instrument did not
    trade. ⚠ A null is a foreign holiday, NOT a zero: the axis is the union of every instrument's
    trading days, so rendering nulls as 0 draws a spike to the floor on every one of them.
    """

    dates: list[str] = []
    values: dict[str, list[float | None]] = {}


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
    # Every instrument behind the numbers above, and its charted series. Shipped WITH the matrix
    # rather than from a second endpoint: both are built from one price load (the only expensive
    # part of this request), so a second endpoint would repeat that load AND open the door to the
    # table describing a slightly different set of inputs than the matrix consumed.
    instruments: list[CorrelationInstrument] = []
    series: CorrelationSeries = CorrelationSeries()


@router.get("/api/airs/model-portfolios/correlations",
            response_model=PortfolioCorrelationMatrix)
async def airs_model_portfolio_correlations(request: Request, year: int | None = None):
    """YTD + trailing-12m return-correlation matrices over the listed (> 5-holding) models,
    plus every instrument that fed them and its price series.

    ⚠ GZIPPED HERE RATHER THAN BY A `GZipMiddleware`, for the reason `/api/benchmarks/…/grid`
    records: this app is SSE-heavy, and compression sits between a stream and its client and
    buffers. The instrument series are ~450 KB of JSON and compress to ~207 KB; the matrices
    alone are a few KB. `Accept-Encoding` is honoured, not assumed — `/documentation` publishes
    curl quick-starts against this API, and curl does not send it by default.
    """
    from routers._airs_portfolio_correlation import (  # noqa: PLC0415
        compute_portfolio_correlations_async,
    )

    payload = await compute_portfolio_correlations_async(year)
    # ⚠ THE MODEL STILL VALIDATES. Returning a `Response` skips FastAPI's `response_model` check,
    # and this schema is what `npm run gen:types` builds the frontend's types from.
    body = PortfolioCorrelationMatrix.model_validate(payload).model_dump_json().encode()
    if "gzip" in (request.headers.get("accept-encoding") or "").lower():
        return Response(content=gzip.compress(body, 1), media_type="application/json",
                        headers={"Content-Encoding": "gzip"})
    return Response(content=body, media_type="application/json")


class CompositionHolding(BaseModel):
    """One holding behind a composition bar, at the weight that bar counted it at.

    ⚠ `weight_pct` IS A SHARE OF THE AXIS TOTAL, NOT OF THE PORTFOLIO — Σ over a bucket IS that
    bucket's `portfolio_pct`, exactly. The sector axis divides by the equity sleeve and the other
    two by every long position, so the SAME holding carries different weights on different axes and
    that is correct. See `_airs_portfolio_analysis._axis_holdings`.

    ⚠ IT IS ALSO NOT THE ATTRIBUTION TABLE'S WEIGHT, AND THE TWO ARE BOTH RIGHT. Attribution drops
    funds, cash and anything it could not price, then renormalises what remains to 100% and weights
    it by the position's value when the window OPENED. Measured on Bustelberg Offensief:
    Technology reads 36% here and 39.1% there. Neither is a rounding error and neither is wrong —
    they are shares of different denominators, which is precisely what this list exists to show.
    """

    name: str | None = None
    isin: str | None = None
    # Share of THIS axis's total. Σ over a bucket == that bucket's `portfolio_pct`.
    weight_pct: float = 0.0
    # The asset class (Equity / Bonds / Cash / …) — which is what decides whether a holding is in
    # the sector axis's denominator at all.
    asset_class: str | None = None
    # The bucket it was classified into, so a surprising placement can be seen rather than guessed.
    classified_as: str | None = None
    # The strategies it is reached through, when it came out of a looked-through certificate.
    via_names: list[str] = []


class PortfolioAnalysisRow(BaseModel):
    bucket: str
    portfolio_pct: float = 0.0
    benchmark_pct: float = 0.0
    diff_pct: float = 0.0              # the TILT — the reason the two are side by side
    # The rows the bar is the sum of. Empty for a bucket only the benchmark holds — an unowned
    # sector is a finding, not missing data.
    holdings: list[CompositionHolding] = []


class CompositionExcluded(BaseModel):
    """A holding this axis does not weigh, and why. `cash` · `unpriced` · `unclassified`.

    ⚠ TWO OF THESE THREE ARE ANSWERS, NOT GAPS. A fund, a bond and a cash line have no sector by
    definition — they are not Stocks in our own classification and already have their own slice of
    the allocation chart. Only `unpriced` is a real hole: a stock we hold, in a real sector, that
    we cannot price, so its bucket reads lower than it is. `asset_class` rides along precisely so
    the first kind can be shown as "this was never a stock" rather than as missing weight.
    """

    name: str | None = None
    isin: str | None = None
    # Its share of the whole book on the same basis — i.e. the weight the chart above does NOT show.
    weight_pct: float = 0.0
    # The Class it carries in our own system: Equity, Bonds, Cash, Alternatives…
    asset_class: str | None = None
    reason: str | None = None


class PortfolioAnalysisAxis(BaseModel):
    axis: str                          # sector | region | currency
    rows: list[PortfolioAnalysisRow]
    # ⚠ THE DENOMINATOR, IN WORDS — and it is now the ATTRIBUTION basis (start-of-window value over
    # the attributable holdings), so a bar equals its own Brinson row. Stated rather than implied,
    # because a percentage whose base is unstated is how two correct numbers read as a
    # contradiction. Says so explicitly when a portfolio falls back to the current-value basis.
    basis: str | None = None
    # How many positions that denominator spans.
    positions: int | None = None
    # ⚠ HOW MUCH OF THE BOOK THESE BARS SPEAK FOR. Never assumed to be 100. Most of the remainder
    # is normally funds, bonds and cash, which have no sector BY DEFINITION — informative, not a
    # fault. None on the fallback basis, where nothing is excluded.
    attributable_pct: float | None = None
    # ⚠ THE PART THAT IS ACTUALLY A GAP: real holdings, in real buckets, that we cannot price — so
    # their bucket reads lower than it is. This is what deserves a warning; `attributable_pct`
    # alone made a routine 13% in ETFs look like the same kind of problem.
    unpriced_pct: float | None = None
    # The holdings behind both, named — so the missing weight is visible, not inferred.
    excluded: list[CompositionExcluded] = []


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
    # (AIRS's own cumulatief_rendement). ⚠ The BENCHMARK no longer follows it: since 2026-08-19 the
    # headline benchmark is the index ETF's own price series from GuruFocus where one exists —
    # see `benchmark_source` below.
    source: str = "model"
    # True when a paired AIRS book exists — so the UI can explain a blank 'book' return as "no
    # paired book" rather than a computation failure.
    book_available: bool | None = None
    ytd_from: str | None = None
    since_from: str | None = None
    portfolio_ytd_pct: float | None = None
    # As-of dates behind the numbers, for the per-value provenance ⓘ. `portfolio_as_of` is the
    # yfinance close date (model source) or the AIRS book snapshot date (book source).
    # ⚠ `benchmark_as_of` describes the ATTRIBUTION panel's benchmark legs (still the yfinance
    # reconstruction), NOT the headline tile — that one is `benchmark_ytd_as_of`.
    portfolio_as_of: str | None = None
    benchmark_as_of: str | None = None
    # ⚠ WHERE THE HEADLINE BENCHMARK FIGURE COMES FROM, which is no longer the same place as the
    # attribution's legs. `benchmark_source` is "etf" (the index ETF's own price series, converted
    # to EUR at each mark's own rate — see `_benchmark_etf`) or "rebuild" (the constituent
    # reconstruction, which is all the AEX and any pre-inception window can have). The tile has to
    # be able to say which, because the two disagree by ~2.8pp on ACWI YTD and the reader can see
    # the other one in the attribution panel.
    benchmark_source: str | None = None
    benchmark_ticker: str | None = None
    benchmark_ytd_from: str | None = None       # the close it OPENED on, not the 1-Jan anchor
    benchmark_ytd_as_of: str | None = None      # the ETF's last bar; None on the rebuild path
    # The four numbers the ETF return is made of, so the tile's ⓘ can print the formula and then
    # the same formula with these filled in. ⚠ `*_fx` is the ETF currency PER EUR (1.1750 USD/EUR)
    # — the direction the formula divides by. All None on the rebuild path.
    benchmark_ytd_open_price: float | None = None
    benchmark_ytd_close_price: float | None = None
    benchmark_ytd_open_fx: float | None = None
    benchmark_ytd_close_fx: float | None = None
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
    # The bucket's own return — its euro Result over what it was worth when the year opened.
    # Null when no book. ⚠ A RATE ON ITS OWN DENOMINATOR, so these do NOT add across classes.
    return_pct: float | None = None
    # ⚠ THE SAME EUROS ON THE BOOK'S DENOMINATOR, in POINTS — this one DOES add.
    # ⚠ BUT NOT TO THE WHOLE BOOK: a position sold out during the year has no asset class, so no
    # slice can carry it. Measured on BUS_Offensief_Dyn the classes come to +8.211pp against a
    # book that made +5.827%, the missing -2.384pp being eight names it no longer holds
    # (`realised.positions`). The UI states that remainder rather than letting the parts quietly
    # miss the total.
    contribution_pct: float | None = None
    # ⚠ How many INDIVIDUAL holdings sit in this class, counted AFTER the certificates are looked
    # through. A weight alone cannot tell "66% in one bond ETF" from "66% across sixty names" —
    # they draw the same slice and are not the same portfolio. On ToppenbergBeheer Defensief the
    # Stocks sleeve is nine lines in AIRS and ~160 real companies underneath.
    holdings: int = 0


class HoldingSource(BaseModel):
    """One ROUTE into a holding, and how much of the book arrives that way.

    `label` is null for the book's own shares (held directly); otherwise it is the strategy whose
    certificate was looked through to reach the instrument.
    """

    label: str | None = None
    value_eur: float
    # A share of the WHOLE BOOK, not of the row — so the routes add up to the holding's
    # `weight_now_pct` and can be checked against the column next to them.
    weight_now_pct: float
    # What this route was worth when the window opened, and what IT returned — valued by the book
    # that actually holds it, so a wrapped leg carries that book's own figure.
    start_value_eur: float | None = None
    return_pct: float | None = None
    book: str | None = None                # the AIRS account that valued this route
    as_of: str | None = None               # ...and the snapshot it valued it at
    # ⚠ THAT BOOK'S OWN VALUATION — the numerator and denominator behind `return_pct`, so the card
    # can print the division instead of asserting its result. NOT this route's slice of our book:
    # on a split row the two diverge, and showing the slice beside the direct position's return
    # would put two numbers on screen whose ratio is not the third.
    book_start_value_eur: float | None = None
    book_current_value_eur: float | None = None
    book_income_eur: float | None = None
    # This route's share of the OPENING value of the routes that could be valued — the weight it
    # carried in `own_return_pct`. Null where the route had no return to contribute, which is also
    # how the card shows a reader which legs actually spoke.
    blend_weight_pct: float | None = None


class BookHoldingDetail(BaseModel):
    """One paired-book position — every LONG line, priced or not.

    ⚠ TWO WEIGHTS ON PURPOSE, AND THEY ARE NOT INTERCHANGEABLE.

    `weight_pct` is the holding's OPENING value (beginwaarde) as a share of the PRICED book, so that
    within ANY asset class, Σ (weight_pct / Σ_class weight_pct) · return_pct reproduces that class's
    START-weighted return (Σnow/Σstart−1) exactly — the true value change, not the current-value
    weighting that lets a big winner dominate. It is None where we could not price the position over
    the window, which is why it is nullable: a 0% there would read as "held nothing", not "unknown".

    `weight_now_pct` is the CURRENT value as a share of the WHOLE book — the very number the
    allocation chart is drawn from, so per-class subtotals in the holdings table equal the chart's
    slices to the decimal. Use this one for anything shown beside the chart; a table that disagrees
    with the chart above it is read as a bug in both.

    `weight_start_pct` is the THIRD weight, and it is the one that reconciles this table with the
    composition charts. ⚠ THREE WEIGHTS, ONE POSITION, ALL CORRECT:

      weight_now_pct    current EUR value ÷ the WHOLE book — what is held today.
      weight_start_pct  Beginwaarde ÷ the WHOLE book at the window's open. It is GRAFTED ON from
                        the very legs the sector/region/currency bars are built from, never
                        recomputed, so the table and the chart cannot disagree about January.
      the bar itself    `weight_start_pct` ÷ that axis's `attributable_pct` — a bar is a share of
                        the holdings that HAVE a bucket, not of the book.

    Measured on Bustelberg Offensief: ASML 7.02% now, ~5.00% at the start, 5.75% on the Technology
    bar. Dividing the FIRST by the Stocks slice and expecting the THIRD is the trap this column
    closes — ASML outgrew the book by ~40% over the window, so its current share is much the larger.

    ⚠ `weight_start_pct` IS NOT `weight_pct`. Same numerator, different denominator: `weight_pct`
    divides by the PRICED book (so a class's contribution reconciles), this divides by the whole
    book (so it sits honestly beside `weight_now_pct`). A `0.0` is a fact — bought after the window
    opened; `None` means no ISIN to join on (cash), never "zero".

    `currency` is the holding's quote currency (a fair first-order FX signal for a bond/ETF class —
    NOT folded to Unclassified like the fund axes).
    """
    name: str | None = None
    isin: str | None = None
    bucket: str
    # ⚠⚠ IS THIS A FUND, RATHER THAN A COMPANY — AND IT IS ON THE ROW BECAUSE THE BUCKET NO LONGER
    # SAYS. Until 2026-08-18 an equity ETF sat in its own `Equity ETF` bucket, so "bucket ==
    # Equity" doubled as "an operating company", and the Analyse modal gates owner-earnings
    # blending on exactly that. With the two merged into Stocks the guarantee is gone from the
    # bucket, so it travels here instead — otherwise the blender would be handed ETFs, which have
    # no earnings and which this app deliberately does not look through.
    is_fund: bool | None = None
    # ⚠ THE SECTOR CHART'S OWN BUCKET, NOT `asset_grid.sector` RAW. It runs through the identical
    # `_buckets` the bars and the benchmark use — canonicalised ("Financial Services" -> Financials,
    # the two Yahoo vocabularies), the ETF/asset-class leftovers ("etf", "Equity") stripped back to
    # Unclassified, a fund folded to Unclassified because its listing says nothing about what it
    # holds, and cash to Cash. A column that named a sector the bars have never heard of is exactly
    # the taxonomy split this module exists to prevent, one screen apart instead of one chart apart.
    sector: str | None = None
    currency: str | None = None
    # Which strategies put us in this instrument — the model portfolios whose certificates were
    # looked through to reach it. Empty when the position is held directly. More than one is
    # normal: NVIDIA arrives through three of ToppenbergBeheer Defensief's certificates.
    via_names: list[str] = []
    # ⚠ HOW MUCH CAME EACH WAY — `via_names` names the routes but cannot size them, and unsized
    # they mislead: MasterCard is EUR 50,489 of BUS_Offensief_Dyn's own shares against EUR 1,991
    # (3.8%) through the Star certificate, and a row chipped only "Star" reads as a position the
    # book does not hold itself. `label` is null for the book's own shares. The percentages are
    # shares of the BOOK and SUM to `weight_now_pct` by construction — the column beside them.
    sources: list[HoldingSource] = []
    weight_pct: float | None = None
    weight_now_pct: float = 0.0
    weight_start_pct: float | None = None
    return_pct: float | None = None
    # ── WHAT THIS POSITION ACTUALLY MADE, in euros, and its share of the book's year.
    # ⚠ THESE ADD UP AND THE WEIGHTS DO NOT, which is why they exist. `unrealised + realised +
    # income = result`, and Σ `contribution_pct` over every row — these plus the sold-out positions
    # in `realised.positions` — IS the book's own return (measured: 0.0000pp residual).
    #
    # ⚠ A EURO AMOUNT MAY BE SPLIT ACROSS A CERTIFICATE'S LEGS; A PERCENTAGE MAY NOT. A
    # looked-through row's `unrealised_eur` is its share of the certificate's value change — real
    # money, really this row's share — while its `return_pct` would be the WRAPPER's rate, which is
    # the documented lie (NVIDIA +0.08% against its own +2.82%). That is why the result columns are
    # in euros and the Return column stays `own_return_pct`.
    start_value_eur: float | None = None
    current_value_eur: float | None = None
    unrealised_eur: float | None = None
    # ⚠ Only on a position TRIMMED but still held. A name sold out entirely has no row here at all
    # — it is in `realised.positions` — and a looked-through leg is never sold on its own, because
    # AIRS trades the certificate. Null, never 0: "nothing was sold" and "the sale broke even" are
    # different facts.
    realised_result_eur: float | None = None
    income_eur: float | None = None
    result_eur: float | None = None
    contribution_pct: float | None = None
    # ── WHAT THE MONEY MADE, as against what the instrument did.
    # ⚠ `return_pct` / `own_return_pct` divide by AIRS's RESTATED `Beginwaarde` — today's quantity
    # priced in January — which erases your timing ON PURPOSE so the figure describes the stock.
    # This one divides by the capital actually tied up, weighted by when it went in (Modified
    # Dietz), with dividends net of withholding and anything realised on a mid-year sale already
    # in the numerator. Measured: KLA-Tencor +55.62% as an instrument, +30.94% on the money.
    #
    # ⚠ NULL FOR A LEG INSIDE A CERTIFICATE, and that is not a gap to fill. AIRS trades the
    # WRAPPER, so a stock reached through one has no buys or sells of its own — there is no "money
    # you put in" to divide by. 24 of BUS_Offensief_Dyn's 52 rows are in this position.
    avg_capital_eur: float | None = None
    money_weighted_return_pct: float | None = None
    # ⚠ WHICH of the two reasons the two fields above are blank. True = this position carries a
    # `Tt = D` (Deponering) row, so shares arrived without a purchase and its trade quantities and
    # its holding quantity are on different bases. False with a blank value = it is a leg inside a
    # certificate, which has no flows of its own at all. Same `None`, different facts.
    capital_unknown: bool = False
    # ── THE CERTIFICATE'S OWN INVESTED-CAPITAL FIGURES, for a leg that can never have its own.
    # ⚠⚠ THIS IS NOT THE LEG'S RETURN AND MUST NOT BE SHOWN IN THE LEG'S COLUMN. AIRS bought ONE
    # certificate, so every leg inside it shares a single flow history: put it in the column and
    # all 22 StarTopSelectie legs read −3.86%, which looks like 22 per-stock measurements and is
    # one measurement copied 22 times. Shopify did not return −3.86% on the money; the wrapper did.
    # Separate keys so the UI attributes it instead of asserting it.
    via_holding_names: list[str] = []
    via_holding_name: str | None = None
    # ── "lookthrough" means `money_weighted_return_pct` was measured in `capital_book` — the child
    # book that ACTUALLY bought this stock — rather than in the book being viewed, which only ever
    # bought the certificate. ⚠ It is therefore the STRATEGY's return on its money, not this book's:
    # this book's own experience depends on when IT bought the wrapper. Same compromise the Return
    # column already makes, so the two agree instead of each being wrong differently. The euro
    # capital is scaled to this book's slice of the child's position; the rate is not scaled.
    capital_source: str | None = None
    capital_book: str | None = None
    via_money_weighted_return_pct: float | None = None
    via_avg_capital_eur: float | None = None
    # ⚠ THE INSTRUMENT'S OWN EUR RETURN — NOT `return_pct`, AND THE DIFFERENCE IS THE WHOLE POINT.
    # `return_pct` is the book's value change, and the book does not know what NVIDIA did: it knows
    # what the CERTIFICATE holding NVIDIA did. Splitting that certificate's start and current value
    # by the same composition share hands every instrument behind it the wrapper's return — 135
    # stocks with 37 distinct returns, NVIDIA reporting +0.08% against its own +2.82%. So a
    # per-instrument figure is priced from the instrument's own EUR series over `own_return_from`
    # (the portfolio's YTD anchor: max(1 Jan, the composition's effective date)), through the same
    # marks that produce the arithmetic behind a portfolio's YTD elsewhere. Use THIS one per row;
    # `return_pct` only aggregates correctly, and only over a whole class.
    #
    # ⚠ BUT ONLY WHERE THE BOOK CANNOT ANSWER. The wrapper argument above is about LOOK-THROUGH
    # rows and was being applied to every row, including the ones AIRS values directly — and for
    # those AIRS knows the answer exactly. Fortinet in AITopSelectie OFF DYN is +111.74% by AIRS's
    # own Beginwaarde -> Huidige waarde plus its net dividend, and +108.65% off our yfinance
    # series. Both defensible; the modal showing one while the row that opened it shows the other
    # is not. So a directly-held row now reports AIRS's TOTAL return — the identical number the
    # expanded row's `Return` column computes — and only a look-through row (or one with no
    # opening value in the book) falls back to the instrument's own price series.
    #
    # `own_return_source` says which of the two this row got. Two rows in one column measured
    # different ways, with nothing on screen saying which, is the thing that change undid.
    own_return_pct: float | None = None
    own_return_from: str | None = None
    own_return_estimated: bool = False
    # Annualised volatility of this instrument's DAILY EUR close over the trailing 5 years.
    #
    # ⚠ null WHERE THERE IS NOT ENOUGH HISTORY, and that is not the same as low volatility — a
    # stock that listed last year has no five-year figure, and 0.0 in the column would read as
    # remarkably stable. See `_holding_risk`; the column renders a dash.
    vol_5y_pct: float | None = None
    # Beta of the same daily EUR returns against the SELECTED benchmark's investable tracker, over
    # the same 5 years and the dates both series share.
    #
    # ⚠⚠ IT MOVES WITH THE BENCHMARK PICKER. A beta is meaningless without naming what it is
    # against, so this is not a property of the instrument — request the modal with a different
    # `benchmark` and every value here changes.
    #
    # ⚠ null RATHER THAN 0. Beta 0 means "moves independently of the market", which is a strong
    # claim about a stock we simply could not measure.
    beta_5y: float | None = None
    # 12-1 momentum: the 12-month EUR price return EXCLUDING the most recent month, in %.
    #
    # ⚠⚠ NOT the strategy's `momentum_score`. That is a min-max normalisation ACROSS the universe
    # it was scored over, so the same stock scores differently against the S&P than against ACWI
    # and a holding in no universe has none at all — a ranking within a run, not a property of the
    # stock. This is absolute and needs no universe, which is what makes a column of them
    # comparable between stocks.
    #
    # ⚠ SKIPPING THE LAST MONTH IS THE DEFINITION, not a refinement: the most recent month
    # mean-REVERTS, so including it is what makes a raw 12-month return a poor momentum signal.
    # From `signal_engine.daily.compute_single_company_signals` — one definition, shared with
    # /signal-lab and the backtester.
    #
    # ⚠ IT NEEDS ONLY ~13 MONTHS, unlike the risk columns' four years, so a young listing can have
    # momentum and a dash for vol.
    mom_12_1_pct: float | None = None
    # ⚠ THE TWO PRICES BEHIND IT (EUR), so the ⓘ can show `from ÷ to − 1 = result` instead of
    # asserting the result. Null together with the figure, and null when the legs could not be
    # read even though it could — the tooltip then states the formula without substituting it.
    mom_12_1_from: float | None = None
    mom_12_1_to: float | None = None
    own_return_source: str | None = None      # "airs" | "yfinance" | None
    # ⚠ WHICH AIRS BOOK THE FIGURE CAME FROM, because it is no longer always this one. A leg held
    # only inside a certificate is valued by the account behind that certificate — the book that
    # actually holds the shares — and its answer can differ sharply from this book's for the same
    # instrument, because AIRS's Beginwaarde is the year-open value OR the PURCHASE value for a
    # position opened during the year. MasterCard: +2.14% in BUS_Offensief_Dyn (held since January)
    # against +17.62% in StarTopSelectie's book (bought later, cheaper). Both AIRS, both correct,
    # different questions — so the column names its source rather than leaving it inferable.
    # None on a yfinance row.
    own_return_book: str | None = None
    # ⚠ THE DATE THIS ROW'S RETURN IS AS-OF, PER ROW, because the two bases have different
    # clocks: an `airs` row is as-of the BOOK SNAPSHOT, a `yfinance` row as-of that instrument's
    # own last close, which can trail it by weeks. The payload's `as_of` is neither — it is the
    # model composition's effective date, and stamping the cards with it reported the row's own
    # +111.74% as 216 days old while the portfolios list called the same figure 2.
    own_return_as_of: str | None = None
    # The net dividend inside an `airs` figure (gross + withholding, which AIRS books negative).
    # None on a look-through row, and None when the journal has no line for the holding — "paid
    # nothing" and "we have not read the journal" are different claims.
    own_income_eur: float | None = None


class AllocationBand(BaseModel):
    """One cell of the allocation policy: what share this class may take in this risk profile.

    ⚠ EVERY PERCENT IS OPTIONAL, AND NULL IS NOT ZERO. "No policy recorded" and "hold none of this"
    are the same claim for a minimum and OPPOSITE claims for a default and a maximum, so an unset
    cell comes back null rather than 0 — a zeroed grid would publish a policy nobody wrote.

    ⚠ DECLARED ABOVE `ModelPortfolioAnalysis` BECAUSE THAT MODEL EMBEDS IT (the bands drawn over
    the allocation bars). Pydantic resolves the annotation when the class is built, so a definition
    further down the file is a NameError at import, not a forward reference.
    """

    variant: str            # a `_airs_portfolio_variant.VARIANTS` label
    bucket: str             # a BUCKET_ORDER key — "Equity", never the "Stocks" display label
    min_pct: float | None = None
    default_pct: float | None = None
    max_pct: float | None = None
    updated_at: str | None = None


class RealisedContributionLeg(BaseModel):
    """One name the book SOLD this year, and what that sale contributed to the year.

    ⚠ THERE IS NO WEIGHT HERE, AND ITS ABSENCE IS THE HONEST STATEMENT. A sold parcel's opening
    value is not recoverable from AIRS's data: `proceeds − Res. YtD` yields its COST BASIS, which
    for a parcel bought in February is real capital that did not exist on 1 January — feeding it
    in made the opening-capital gap WORSE (EUR 55,427 → EUR 377,776 on BUS_Offensief_Dyn), and
    partial sells make it unrecoverable in principle since AIRS restates `Beginwaarde` to the
    CURRENT quantity. A contribution needs no weight; an allocation effect does, which is why
    these legs may never enter the composition bars or Brinson.
    """

    fonds: str | None = None
    realised_ytd_eur: float | None = None
    # Share of the BOOK's year, in points, on its own opening capital.
    contribution_pct: float | None = None
    # ⚠ Decided by ABSENCE from the holdings, not by presence here — a sale is a realisation, not
    # a closure, and most sold names are still held (trimmed).
    closed_out: bool | None = None
    # Non-zero means part of this gain was made in earlier years and is correctly NOT in the year.
    prior_year_eur: float | None = None
    first: str | None = None
    last: str | None = None


class LedgerPosition(BaseModel):
    """One instrument's whole year — whether the book still holds it or not.

    ⚠ `contribution_pct` IS THE COLUMN THAT ADDS UP. Its sum over every position IS the book's own
    YTD (measured exactly: 5.8267 against AIRS's 5.826704, and 44.4624 against 44.462408).
    `weight_pct` is DESCRIPTIVE — how much of the year's capital this position occupied — so
    `contribution ≈ weight × return` holds only approximately, and the identity the table asserts
    is the contribution one.

    ⚠ `return_pct` IS ON AVERAGE CAPITAL, NOT THE INSTRUMENT'S PRICE RETURN. A name bought in June
    shows a larger percentage on the same euros than one held all year, because it answers "how
    hard did this money work" rather than "what did the instrument do". The Holdings table's own
    Return column is the other question and the two will differ.
    """

    name: str
    held: bool = False
    # ⚠ Decided by ABSENCE from the holdings, not by having sold — most sold names are trims.
    closed_out: bool = False
    # Value at the year's open. For a held row, `Beginwaarde` de-restated back to the quantity
    # actually owned then; for a sold-out row, `proceeds − Res. YtD` scaled to the shares held at
    # the open (AIRS does not publish its parcel matching, so that split is proportional).
    # ⚠⚠ NULLABLE, AND THE NULL IS THE POINT — a 0.0 here would be a claim that the position
    # opened at nothing and tied up nothing, which is exactly the false statement the refusal
    # exists to avoid. `None` means "its share count moved for a reason we do not interpret
    # (`Tt = D`, Deponering), so no quantity arithmetic on it is trustworthy". Typed as plain
    # floats these blanks raised a ResponseValidationError and took the whole modal down with a
    # 500 — the right failure for a wrong type, and a reminder that a refusal has to be modelled
    # as far as the wire, not just computed.
    opening_eur: float | None = None
    avg_capital_eur: float | None = None
    weight_pct: float | None = None
    # Why those three are blank, so the UI can say so rather than showing three unexplained dashes.
    capital_unknown: bool = False
    held_result_eur: float = 0.0
    realised_result_eur: float = 0.0
    income_eur: float = 0.0
    result_eur: float = 0.0
    contribution_pct: float | None = None
    return_pct: float | None = None
    sales: int = 0
    first_sale: str | None = None
    last_sale: str | None = None
    prior_year_eur: float = 0.0
    # ⚠⚠ THE INSTRUMENT'S OWN RISK FIGURES, ON A ROW THE BOOK NO LONGER HOLDS — and they are
    # meaningful precisely because they are properties of the INSTRUMENT, not of the position.
    # Momentum, volatility and beta are computed from our own daily EUR close series, which does
    # not stop when a book sells; the same three columns therefore say the same thing on a sold row
    # as on a held one, which is what makes the table readable straight down.
    #
    # ⚠ AS OF TODAY, NOT AS OF THE SALE. That is the same basis as every held row above — which is
    # the point, since a column comparable down its own length is worth more here than one that
    # answers "what was its beta the day I sold it". The UI says so on the cells.
    #
    # ⚠ `isin` IS RESOLVED, NOT CARRIED. A closed-out position has no holdings row and therefore no
    # ISIN of its own; this is what `_sold_position_isins` recovered, and it is on the wire so the
    # reader can see WHICH instrument the three numbers describe. Null when it could not be
    # resolved, and then the three are null too — never a figure for an instrument we cannot name.
    isin: str | None = None
    vol_5y_pct: float | None = None
    beta_5y: float | None = None
    mom_12_1_pct: float | None = None
    # ⚠ THE TWO PRICES BEHIND IT (EUR), so the ⓘ can show `from ÷ to − 1 = result` instead of
    # asserting the result. Null together with the figure, and null when the legs could not be
    # read even though it could — the tooltip then states the formula without substituting it.
    mom_12_1_from: float | None = None
    mom_12_1_to: float | None = None


class RealisedBlock(BaseModel):
    """What the paired book realised on sales this year — the leg the holdings table cannot show.

    ⚠ EVERY FIGURE SITS ON ONE DENOMINATOR, `basis_eur` (the book's own `beginvermogen`), so
    `held_pct + realised_pct + sold_income_pct == book_ytd_pct` exactly. The holdings table weights
    by each position's share of the PRICED HELD book, which is right for a class return and cannot
    carry a sold position at all — different question, different denominator.

    ⚠ `available: false` IS NOT "SOLD NOTHING". No pairing, no cached Transacties sheet, or a
    sheet we could not read — each has its own `note`, and an empty list presented as an answer
    would hide EUR 28,656 of realised loss on the book this was measured against.
    """

    available: bool = False
    portefeuille: str | None = None
    note: str | None = None
    basis_eur: float | None = None
    # ⚠⚠ False on a book with deposits or withdrawals: `result ÷ opening capital` is not a return
    # there, so the percentages are withheld and only the euro amounts stand. Contributions that
    # do not add to the figure they decompose are worse than none — each looks reasonable alone.
    comparable: bool | None = None
    held_pct: float | None = None
    realised_pct: float | None = None
    sold_income_pct: float | None = None
    total_pct: float | None = None
    held_eur: float | None = None
    realised_eur: float | None = None
    sold_income_eur: float | None = None
    book_ytd_pct: float | None = None
    residual_eur: float | None = None
    reconciles: bool | None = None
    holdings_as_of: str | None = None
    book_as_of: str | None = None
    dates_aligned: bool | None = None
    residual_reason: str | None = None
    # ⚠ WHAT THE WEIGHT-BASED VIEWS CANNOT SEE, on the ABSOLUTE result — a realised −28,656 against
    # a held +75,164 is not "negative coverage"; the question is how much of the movement happened
    # outside the holdings table. Measured 41% on BUS_Offensief_Dyn, which alone can reverse a
    # sector's verdict in an attribution built only on what is left.
    realised_share_of_result_pct: float | None = None
    legs: list[RealisedContributionLeg] = []

    # ── EVERY POSITION THE BOOK TOUCHED, held and sold, in ONE list.
    # ⚠ `weight_pct` is AVERAGE INVESTED CAPITAL (Modified Dietz), not a 1-January snapshot — the
    # only weight a sold position can carry, and the only one that describes a book that changed
    # during the year. AITopSelectie's equities were worth EUR 40,319 on 1 January against a
    # EUR 1,000,000 opening capital (it began the year in cash and deployed on 5 January), so a
    # start-weighted table would have called it 96% cash.
    positions: list[LedgerPosition] = []
    avg_capital_eur: float | None = None
    # ⚠ Σ average capital ÷ `beginvermogen`. REPORTED, never assumed to be 1: Modified Dietz
    # ignores the price path within a position and the de-restatement is its own approximation.
    # Measured 0.980 (BUS_Offensief) and 1.023 (AITopSelectie).
    capital_coverage_ratio: float | None = None
    ledger_result_eur: float | None = None


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
    # ⚠ THE COMPOSITION'S EFFECTIVE DATE — when the MODEL declared these weights. It is NOT the
    # date any figure on this screen is valued at, and using it as one is how the modal came to
    # report the portfolios row's own +111.74% as 216 days old while the row called it 2 days.
    as_of: str | None = None
    # The snapshot the BOOK valuations come from — the clock for the weight columns and for every
    # `own_return_source == "airs"` row. Null in model mode, where each row carries its own
    # `own_return_as_of` instead.
    holdings_as_of: str | None = None
    # ⚠⚠ WHEN *WE* LAST READ THAT BOOK — the second date, and the one that decides whether an old
    # `holdings_as_of` is AIRS's lag or ours. The modal wraps its whole subtree in
    # `ProvenanceFetchedAt` with it, so its ⓘ badges reach the same verdict as the row that opened
    # it. Without it every badge in here went amber on a book the row called current, and no
    # refresh could clear the warning because the fact that clears it was not in the payload.
    holdings_fetched_at: str | None = None
    # ⚠ WHICH BOOK IS "THIS" BOOK. Needed the moment a holding's Return could come from ANOTHER
    # account: a leg held only inside a certificate is valued by the book behind that certificate,
    # which reports its own `own_return_book`. Without this to compare against, either every AIRS
    # row has to be labelled with its source or none of them can be.
    book_portefeuille: str | None = None
    # The risk profile AIRS's own name says this model is offered at (Offensief / Beperkt
    # Offensief / Neutraal / Defensief), and the allocation policy recorded for it — the band each
    # class is SUPPOSED to sit in, drawn over the bar showing where it actually sits. `variant` is
    # null for the 8 models not offered at a profile at all, and `bands` is then empty: a product
    # with no risk profile has no policy, and inventing one would be worse than drawing nothing.
    variant: str | None = None
    bands: list[AllocationBand] = []
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
    # ── Look-through ───────────────────────────────────────────────────────────────────────
    # Some positions are not instruments: they are other model portfolios wrapped as a Leonteq
    # certificate. These charts are drawn over the stocks BEHIND them, not over the lines AIRS
    # stores — measured on ToppenbergBeheer Defensief, 9 of 12 positions and 44.56% of the
    # weight. Unexpanded it charted "Unclassified 100%" over 1% classified weight.
    #
    # ⚠ REPORTED, NOT SILENT. The composition table still shows the unexpanded rows, so without
    # this a reader cannot reconcile 168 holdings against the twelve in front of them, and cannot
    # tell a portfolio that genuinely holds 22 names from one holding three certificates.
    looked_through_pct: float = 0.0
    # Weight still inside a certificate we could NOT expand (its target has no stored
    # composition). Kept as an opaque leg rather than dropped — deleting it would shrink the
    # portfolio and everything else would renormalise over the gap, invisibly.
    opaque_pct: float = 0.0
    looked_through: list[dict] = []
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
    # ⚠ WHY `book_holdings` IS EMPTY, WHEN IT IS. Three different faults rendered as one
    # sentence — "No positions to show for this portfolio" — beside a portfolios list that
    # visibly HAS rows: the model is not paired with a book, the paired book has never been
    # scanned, or it was opened as an unpaired basket. Different remedies, and none of them was
    # on screen. Null when there IS a book view.
    book_note: str | None = None
    # ⚠ THE HALF OF THE YEAR THE HOLDINGS TABLE CANNOT SHOW. Everything above is built from
    # positions the book STILL HOLDS; a name sold in March has no row and its result is invisible
    # (BUS_Offensief_Dyn: EUR -28,656, 41% of the year's movement).
    realised: RealisedBlock | None = None
    # Milliseconds per phase of this request. The modal is seconds long and the browser could
    # only ever see the total — "Loading composition…" with nothing saying which of its eight
    # loads was responsible. Logged to the console on every open.
    timings_ms: dict[str, int] = {}


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


class HoldingTradeEffect(BaseModel):
    """One decision, and what it was worth against not having made it.

    ⚠ AGAINST DOING NOTHING, NOT AGAINST A PERFECT DECISION. A buy gains if the price rose after
    it; a sell gains if the price fell after it. A lucky call and a good one produce the same
    number — this makes no claim about skill.
    """

    datum: str | None = None
    kind: str
    quantity: float          # in TODAY's share basis
    price_eur: float         # per share, EUR, today's basis
    amount_eur: float
    effect_eur: float
    # The effect per euro that changed hands — i.e. how far the price moved in your favour since
    # the decision. Says how GOOD it was, and nothing about whether it mattered.
    move_pct: float | None = None
    # The effect over the position's value on 1 January — how MUCH it mattered. Null where nothing
    # was held at the open, since there is no base to be a share of.
    effect_pp: float | None = None
    # The trade was in a pre-split basis and had to be converted before it could be compared.
    rescaled: bool = False


class HoldingTiming(BaseModel):
    """One held position's year: what doing nothing would have made, and what each trade changed.

    ⚠ THE IDENTITY IS EXACT AND IS ASSERTED: `buy_hold_eur + timing_eur == actual_eur`. Measured
    2026-08-05, residual 0.00 on every position tried. Three lines that do not add up are not a
    decomposition, and `reconciles` is how the UI knows not to present them as one.

    ⚠⚠ `actual_eur` IS THE ECONOMIC RESULT AND IS NOT THE TABLE'S `Result` COLUMN. That column is
    AIRS's restated figure — `Huidige waarde − Beginwaarde`, where Beginwaarde prices TODAY's share
    count at the 1 January price, valuing shares bought later at January's price rather than what
    was paid. `restatement_eur` is the difference, named rather than left for a reader to find.
    """

    available: bool = False
    name: str
    portefeuille: str | None = None
    note: str | None = None
    qty_open: float = 0.0
    qty_now: float = 0.0
    price_open_eur: float = 0.0
    price_now_eur: float = 0.0
    buy_hold_eur: float = 0.0
    timing_eur: float = 0.0
    actual_eur: float = 0.0
    # ── The same three lines in percent, over ONE base: what the position held on 1 January was
    #    worth. Because it is one base the identity survives the division —
    #    `buy_hold_pct + timing_pp == actual_pct`. Null where nothing was held at the open.
    #    ⚠ `actual_pct` is NOT the `Money-weighted` column: that is Modified Dietz over the
    #    TIME-WEIGHTED average capital, a different denominator.
    open_value_eur: float | None = None
    buy_hold_pct: float | None = None
    timing_pp: float | None = None
    actual_pct: float | None = None
    residual_eur: float = 0.0
    reconciles: bool = False
    airs_result_eur: float | None = None
    restatement_eur: float | None = None
    income_eur: float = 0.0
    # The proven split ratio applied to the pre-event trades, or null if there was none.
    split_ratio: float | None = None
    # The window every figure here is measured over — carried so a timeline can place each
    # decision on a real axis rather than one inferred from the trades themselves.
    period_start: str | None = None
    period_end: str | None = None
    trades: list[HoldingTradeEffect] = []


@router.get("/api/airs/model-portfolios/{portfolio_id}/holding-timing",
            response_model=HoldingTiming)
async def airs_holding_timing(portfolio_id: int, name: str):
    """Why one holding's `Money-weighted` return differs from its `Instrument return` — trade by trade.

    `Instrument return` erases your timing on purpose (it divides by AIRS's opening value restated
    to today's quantity, so a share bought in June is still measured from January); `Money-weighted`
    is driven by it. This decomposes the gap: what the position you held on 1 January would have
    made untouched, and what each buy and sell added or cost against that.
    """
    import asyncio  # noqa: PLC0415

    from routers._airs_holding_timing import holding_timing  # noqa: PLC0415

    return await asyncio.to_thread(holding_timing, portfolio_id, name)


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


class ActiveShareRow(BaseModel):
    """One ISSUER, on both sides. ⚠ NOT one holding and not one listing — a book holding two share
    classes of the same company contributes ONE of these, with the weights summed. See
    `_active_share._issuer_key`."""

    name: str
    portfolio_pct: float = 0.0
    benchmark_pct: float = 0.0
    #: `portfolio_pct - benchmark_pct`. Positive = overweight; a name we do not hold is negative.
    active_pct: float = 0.0
    held: bool = False


class ActiveShareUnmatched(BaseModel):
    """A stock we hold that could not be resolved to an issuer name, so it can only be ACTIVE.

    ⚠ IT IS IN THE FIGURE, AND IT IS LISTED BECAUSE IT IS. Dropping it would renormalise the rest
    upward and quietly LOWER active share — the flattering direction — so the honest choice is to
    count it and show what could not be matched.
    """

    name: str | None = None
    isin: str | None = None
    weight_pct: float = 0.0


class ActiveShare(BaseModel):
    """`AS = ½ Σ|wᵖ − wᵇ|` over the union of both name sets — see `routers/_active_share.py`."""

    available: bool = True
    #: Why not, when `available` is false. A real answer, never an empty panel.
    reason: str | None = None
    benchmark: str

    active_share_pct: float | None = None
    #: `Σ min(wᵖ, wᵇ)`, and it is `100 − active_share_pct` by construction. Returned rather than
    #: derived on the client so the identity a reader checks holds to the digit.
    overlap_pct: float | None = None

    #: How much of the WHOLE book the compared sleeve is. ⚠ The comparison assumes the individual
    #: stocks are 100%; this is the number that stops that assumption being silent.
    stocks_pct: float | None = None
    n_holdings: int = 0
    n_in_benchmark: int = 0
    #: The book's weight in names the index does not hold at all — the SELECTION half of the bet,
    #: as opposed to sizing a name the index also has.
    off_benchmark_pct: float | None = None

    benchmark_members: int = 0
    #: ⚠ How much of the index we could price. A missing constituent does not lose its weight, it
    #: redistributes it — so an unpriceable name we do not hold makes active share read LOW.
    benchmark_covered_pct: float | None = None

    unresolved: list[ActiveShareUnmatched] = []
    rows: list[ActiveShareRow] = []


class ActiveShareHolding(BaseModel):
    isin: str | None = None
    name: str | None = None
    weight_pct: float = 0.0
    #: ⚠ THE SERVER'S OWN CLASSIFICATION, ROUND-TRIPPED. It was set by `_is_fund` when the payload
    #: was built; sending it back is what keeps ONE definition of "this is a fund, not a stock"
    #: rather than a second one here that could drift from the Stocks bucket on screen.
    is_fund: bool = False
    #: The position's EUR value — AIRS's own `current_value_eur`, not a `q·P·X` of ours.
    #:
    #: ⚠ OPTIONAL, because an ad-hoc basket has weights and no euros. Every euro figure in the
    #: exposure view is then absent rather than zero: zero would claim the position is worthless.
    #: ⚠ IGNORED BY THE OTHER SIX VIEWS, which are all scale-free — sent once on one body so the
    #: seven views cannot end up describing seven slightly different portfolios.
    value_eur: float | None = None
    #: ⚠ THE LISTING'S currency, which is the FX exposure actually borne. NOT the company's
    #: reporting currency — a different and softer claim. See `_portfolio_exposure`.
    currency: str | None = None


class ActiveShareRequest(BaseModel):
    """The holdings the Analyse modal is ALREADY showing.

    ⚠⚠ THE WEIGHTS COME FROM THE CLIENT ON PURPOSE, WHICH IS THE OPPOSITE OF THIS FILE'S USUAL
    RULE. Everywhere else a weight is computed server-side precisely so two surfaces cannot
    disagree — but this panel sits one click from the Holdings table, and its whole job is to
    describe THAT book. Re-deriving the weights here would give the risk figure a second
    denominator (start weights? design percentages? looked-through or not?) and the first question
    anybody asks of a 71% active share is which rows produced it. So it consumes the displayed
    numbers, and cannot disagree with the table by construction.

    ⚠ IT ALSO REMOVES THE SECOND HOLDINGS PIPELINE. A model portfolio and an ad-hoc basket reach
    this with the same body, so unlike Attribution there is no `portfolio_id` variant to keep in
    step — one route serves both, the same way `basket/analysis` does for the composition.
    """

    holdings: list[ActiveShareHolding] = []


@router.post("/api/airs/portfolio/active-share", response_model=ActiveShare)
async def airs_portfolio_active_share(req: ActiveShareRequest, benchmark: str = "ACWI"):
    """How much of the book's stock sleeve is NOT the benchmark.

    ⚠ ON DEMAND, NOT PART OF THE ANALYSE PAYLOAD. Answering it needs the index's constituents
    (`_asset_benchmark.members` — 1,700 rows and their caps for ACWI), and the Analyse modal is ONE
    request with no partial paint, so folding this in would put that read on the critical path of
    every open for a panel most opens never look at. Same bargain Attribution already strikes.

    ⚠ THE INDIVIDUAL STOCKS ARE TREATED AS 100% OF THE PORTFOLIO. Funds, cash and bonds are
    dropped and the rest renormalised — otherwise liquidity counts as an active bet against every
    index name at once, which is a different (and much less comparable) measure. `stocks_pct` says
    what fraction of the book that sleeve actually is.
    """
    from routers._active_share import compute_active_share  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_active_share, [h.model_dump() for h in req.holdings], benchmark)


class TrackingError(BaseModel):
    """Realised (ex-post) tracking error of the stock sleeve — see `routers/_tracking_error.py`.

    ⚠⚠ `TE = √(1/(T−1) Σ (aₜ − ā)²) · √f`, WITH ā SUBTRACTED and Bessel applied. The other
    convention (√(Σaₜ²/T)) is also called tracking error and is a different number; this codebase
    picks one and routes it through `annualized_stats`, the same function every other volatility on
    the screen goes through.

    ⚠ EX-POST, NOT EX-ANTE. There is no covariance-matrix forecast here, and the two routinely
    disagree — so every label says "realised" rather than leaving the reader to assume.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str
    #: daily | weekly | monthly. ⚠ Weekly is the DEFAULT and the honest one — see `cadence_note`.
    frequency: str = "weekly"
    #: The `f` in the formula: 252 / 52 / 12.
    periods_per_year: float | None = None
    #: `T` — how many active returns the spread was taken over.
    observations: int = 0
    years: int | None = None

    tracking_error_pct: float | None = None
    #: ⚠ THE QUANTITY TE IS THE SPREAD **OF**, returned beside it because the two are constantly
    #: confused. A book can have a large tracking error and no active return whatsoever.
    mean_active_per_period_pct: float | None = None
    active_return_ann_pct: float | None = None
    #: Active return per unit of the risk taken to earn it. Null when TE is ~0 (the denominator).
    information_ratio: float | None = None

    #: How much of the sleeve the average observation covered. Below 100% some holding had no price
    #: at one end of that step and the rest were renormalised over — never carried at zero return,
    #: which would damp the measured volatility in the flattering direction.
    avg_weight_covered_pct: float | None = None
    priced_holdings: int = 0
    total_holdings: int = 0
    #: The investable tracker the difference was taken against — a real fund with a real series,
    #: never the reconstructed index (which has no tradeable price to difference).
    benchmark_isin: str | None = None
    #: Present only on `daily`: non-synchronous closes lower the measured covariance and therefore
    #: INFLATE the spread of a difference. Carried with the number rather than left in a doc.
    cadence_note: str | None = None


@router.post("/api/airs/portfolio/tracking-error", response_model=TrackingError)
async def airs_portfolio_tracking_error(req: ActiveShareRequest, benchmark: str = "ACWI",
                                        frequency: str = "weekly", years: int = 5):
    """Volatility of the active return, annualised — the Risk panel's second view.

    ⚠ THE SAME BODY AS `active-share`, DELIBERATELY. The two views describe ONE portfolio (the
    individual stocks, renormalised to 100%), and sharing the request model is what stops them
    drifting into describing two — an active share over the stock sleeve beside a tracking error
    over the whole book would be two answers to two questions under one heading.

    ⚠ SEPARATE FROM `active-share` AS A CALL, because it costs a five-year daily price load for
    every holding plus the tracker, and most opens of the Risk panel never switch to it.
    """
    from routers._tracking_error import compute_tracking_error  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_tracking_error, [h.model_dump() for h in req.holdings], benchmark,
        frequency, years)


class CorrelationPair(BaseModel):
    a: str
    b: str
    rho: float


class RiskCorrelation(BaseModel):
    """ρ as a RISK measure — see `routers/_portfolio_correlation_risk.py`.

    ⚠⚠ NOT ATTRIBUTION, AND THE TWO MUST STAY SEPARATE PANELS. Attribution decomposes the active
    return into allocation + selection + interaction, terms that SUM to it exactly. Correlation
    appears nowhere in that decomposition and sums to nothing: it says how far the book CAN diverge,
    where attribution says where the divergence came from. A combined view would imply they
    reconcile, and they are not that kind of number.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str
    frequency: str = "weekly"
    periods_per_year: float | None = None
    observations: int = 0
    years: int | None = None

    #: ρ between the book's return series and the benchmark's. Full precision — `r_squared` is its
    #: square, and a rounded ρ beside an unrounded R² would not reconcile on screen.
    benchmark_corr: float | None = None
    #: ρ² — the share of the book's movement the index explains.
    r_squared: float | None = None
    portfolio_vol_pct: float | None = None
    benchmark_vol_pct: float | None = None
    #: The SAME figure the tracking-error view reports, from the same series.
    active_vol_pct: float | None = None
    #: ⚠ THE OTHER SIDE OF `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ`, recomputed from ρ so the identity can be
    #: SEEN to hold rather than asserted. It is what links this view to the tracking-error one.
    implied_active_vol_pct: float | None = None
    #: How far the two sides miss, in pp. Floating-point noise when all is well; anything visible
    #: means the two series stopped being the same two series.
    identity_gap_pp: float | None = None

    #: Position labels, ordered by weight descending — `matrix[i][j]` is ρ(labels[i], labels[j]).
    labels: list[str] = []
    #: ⚠ `null` FOR A PAIR WITH TOO LITTLE OVERLAP, never a faint colour. See `MIN_PAIR_OBS`.
    matrix: list[list[float | None]] = []
    #: Mean off-diagonal ρ — the one number that summarises a matrix. Unweighted on purpose: it is
    #: a question about the NAMES, not about the sizing.
    mean_pair_corr: float | None = None
    pairs_measured: int = 0
    min_pair_observations: int = 0
    least_correlated: list[CorrelationPair] = []
    most_correlated: list[CorrelationPair] = []

    priced_holdings: int = 0
    total_holdings: int = 0
    min_observations: int = 0
    cadence_note: str | None = None


@router.post("/api/airs/portfolio/risk-correlation", response_model=RiskCorrelation)
async def airs_portfolio_risk_correlation(req: ActiveShareRequest, benchmark: str = "ACWI",
                                          frequency: str = "weekly", years: int = 5):
    """Correlation to the benchmark, and between the positions — the Risk panel's third view.

    ⚠ THE SAME BODY AND THE SAME SERIES AS `tracking-error`, so `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ` holds
    between the two views rather than approximately holding. See `build_paired_series`.

    ⚠ SEPARATE FROM ATTRIBUTION BY DESIGN. That lives in its own dialog and decomposes the active
    return; this one measures dispersion. Merging them would imply a reconciliation that does not
    exist.
    """
    from routers._portfolio_correlation_risk import compute_risk_correlation  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_risk_correlation, [h.model_dump() for h in req.holdings], benchmark,
        frequency, years)


class PortfolioVolatility(BaseModel):
    """σ of the stock sleeve's OWN returns — see `routers/_portfolio_volatility.py`.

    ⚠ SAME SERIES AS THE OTHER THREE RISK VIEWS, so `volatility_pct` here is the SAME NUMBER the
    correlation view puts inside `σₐ² = σₚ² + σᵇ² − 2ρσₚσᵇ`. Two σₚ one click apart that
    disagreed would tell the reader one of them is wrong and nothing about which.

    ⚠⚠ NO CASH-FLOW CONTAMINATION, BY CONSTRUCTION RATHER THAN BY CHAIN-LINKING. This is not an
    account-value series — it is a weighted basket of instrument price returns — so a deposit or a
    withdrawal is simply not in it. That is what a time-weighted return exists to achieve. The cost
    is the other caveat: the weights are TODAY'S, carried backwards.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str
    frequency: str = "weekly"
    periods_per_year: float | None = None
    observations: int = 0
    years: int | None = None

    volatility_pct: float | None = None
    benchmark_volatility_pct: float | None = None
    #: ⚠ SORTINO'S CONVENTION: `√(mean(min(R,0)²))·√f` — divided by ALL n, against a target of 0.
    #: The semi-deviation (below-MEAN observations only, divided by how many there are) is also
    #: called downside deviation and reads higher. This one is what `sortino` below is built on.
    downside_dev_pct: float | None = None
    benchmark_downside_dev_pct: float | None = None

    return_ann_pct: float | None = None
    benchmark_return_ann_pct: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    #: ⚠ STATED, because a Sharpe without its risk-free rate is not comparable with anybody else's.
    risk_free_pct: float = 0.0

    #: ⚠ WHAT A CLIENT ACTUALLY FELT. Nobody has experienced "18% annualised volatility"; they
    #: have experienced the worst week. For a fat-tailed book the two are far apart, which is
    #: precisely when σ alone misleads.
    worst_period_pct: float | None = None
    best_period_pct: float | None = None
    negative_periods_pct: float | None = None

    priced_holdings: int = 0
    total_holdings: int = 0
    cadence_note: str | None = None


@router.post("/api/airs/portfolio/volatility", response_model=PortfolioVolatility)
async def airs_portfolio_volatility(req: ActiveShareRequest, benchmark: str = "ACWI",
                                    frequency: str = "weekly", years: int = 5):
    """Standard deviation of the sleeve's own returns, and its downside half.

    ⚠ `benchmark` IS NOT WHAT IS MEASURED HERE — volatility is a single-series statistic. It is
    carried so the index's own σ can sit beside the book's for scale, and so this view uses the
    identical series the other three do.
    """
    from routers._portfolio_volatility import compute_volatility  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_volatility, [h.model_dump() for h in req.holdings], benchmark, frequency, years)


class DrawdownEpisode(BaseModel):
    """One peak → trough → recovery.

    ⚠ AN EPISODE ENDS WHEN THE OLD PEAK IS REGAINED, not when the series turns up. A 40% fall that
    bounces 5% and then falls further is ONE drawdown; splitting on direction would report a set of
    shallow dips and no crash.
    """

    depth_pct: float
    peak_date: str | None = None
    trough_date: str | None = None
    #: ⚠ NULL WHILE STILL UNDERWATER. Inventing today's date here would report a recovery that has
    #: not happened — which is the one thing a drawdown panel must never do.
    recovery_date: str | None = None
    recovered: bool = False
    #: In PERIODS of the stated cadence, never "days" — the view names the cadence beside them.
    decline_periods: int = 0
    recovery_periods: int | None = None
    total_periods: int | None = None


class PortfolioDrawdown(BaseModel):
    """Max drawdown of the RECONSTRUCTED sleeve — see `routers/_portfolio_drawdown.py`.

    ⚠⚠ NOT THE CLIENT'S REALISED DRAWDOWN, and the two are not interchangeable. This rebuilds a
    series from the holdings as they stand TODAY: look-ahead bias (those weights were chosen with
    hindsight) and survivorship bias (names since sold are absent, and the sold ones skew towards
    the fallers). The client's own figure comes from the AIRS returns, with real trades, real costs
    and real timing. Every label here says which one it is.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str
    #: ⚠ DEFAULTS TO **DAILY**, unlike the other risk views. They compare two series whose closes
    #: are hours apart and default to weekly to remove that bias; a drawdown compares a series with
    #: itself, so the bias does not exist — and coarsening hides any dip that recovers inside the
    #: period. Monthly MDD is structurally shallower by percentage points.
    frequency: str = "daily"
    periods_per_year: float | None = None
    observations: int = 0
    years: int | None = None

    max_drawdown_pct: float | None = None
    benchmark_max_drawdown_pct: float | None = None
    #: How far below its own high water mark the series ends. "Worst ever −31%" and "down 28% right
    #: now" are very different conversations, and the second is the one being had.
    current_drawdown_pct: float | None = None
    in_drawdown: bool = False

    worst: DrawdownEpisode | None = None
    #: The deepest few, because one number hides whether it was a pattern or an event: one −30%
    #: and four −25%s have the same maximum and are not the same risk.
    episodes: list[DrawdownEpisode] = []
    episodes_total: int = 0

    #: ⚠⚠ THE SAME MDD AT ALL THREE CADENCES, MEASURED IN THE SAME REQUEST. The gap between them
    #: is the thing this view has to be honest about, and a reader cannot compare figures they must
    #: re-request one at a time. `null` for a cadence with too little overlap to measure.
    by_frequency: dict[str, float | None] = {}

    priced_holdings: int = 0
    total_holdings: int = 0


@router.post("/api/airs/portfolio/drawdown", response_model=PortfolioDrawdown)
async def airs_portfolio_drawdown(req: ActiveShareRequest, benchmark: str = "ACWI",
                                  frequency: str = "daily", years: int = 5):
    """Peak-to-trough falls of the reconstructed stock sleeve, with their dates.

    ⚠ ONE PRICE LOAD SERVES ALL THREE CADENCES — the load is the expensive part and re-bucketing
    is free, so the frequency comparison costs no extra round trips.
    """
    from routers._portfolio_drawdown import compute_drawdown  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_drawdown, [h.model_dump() for h in req.holdings], benchmark, frequency, years)


class ConcentrationRow(BaseModel):
    """One ISSUER in the top of the book — not one line. See `_active_share._issuer_key`."""

    rank: int
    name: str
    weight_pct: float
    cumulative_pct: float
    #: ⚠ THE INDEX'S WEIGHT IN THE SAME ISSUER, so a large position reads as a large BET or merely
    #: as a large company. Apple at 6% is not concentration when the index holds 5%.
    benchmark_pct: float = 0.0


class PortfolioConcentration(BaseModel):
    """`C₁₀ = Σ w₍ᵢ₎` and `HHI = Σ wᵢ²` — see `routers/_portfolio_concentration.py`.

    ⚠⚠ ON ISSUERS, NOT LINES. Alphabet A + Alphabet C is ONE position; counting two would
    understate concentration exactly at the top, where the ten largest are decided.

    ⚠⚠ BOTH DENOMINATORS ARE RETURNED because the choice changes the number: `top10_pct` is of the
    stock sleeve (comparable across books, the panel's convention) and `top10_of_book_pct` is of
    everything including cash and funds (true in absolute terms). Choosing one silently would be
    picking a side of a real question.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str

    issuers: int = 0
    benchmark_issuers: int = 0

    top1_pct: float | None = None
    top3_pct: float | None = None
    top5_pct: float | None = None
    top10_pct: float | None = None
    top20_pct: float | None = None
    top10_of_book_pct: float | None = None
    #: What fraction of the whole book the measured sleeve is — the scale between the two above.
    stocks_pct: float | None = None

    #: ⚠ ON FRACTIONS, NOT PERCENTAGES. `Σw²` over percentages is 10,000× larger (the antitrust
    #: convention) and `N_eff = 1/HHI` only inverts cleanly on fractions.
    hhi: float | None = None
    #: `1/HHI` — the "effective number of positions". An equal-weight book of N names returns
    #: exactly N; forty names dominated by five returns far fewer. ⚠ A better measure than C₁₀,
    #: which cuts at an arbitrary ten: two books with the same C₁₀ can be an even ten-name
    #: portfolio and one dominated by its top three.
    effective_positions: float | None = None

    benchmark_top10_pct: float | None = None
    benchmark_hhi: float | None = None
    benchmark_effective_positions: float | None = None

    top: list[ConcentrationRow] = []
    benchmark_covered_pct: float | None = None
    unresolved: int = 0


@router.post("/api/airs/portfolio/concentration", response_model=PortfolioConcentration)
async def airs_portfolio_concentration(req: ActiveShareRequest, benchmark: str = "ACWI"):
    """How much of the book sits in how few issuers, beside the index's own concentration.

    ⚠ SAME ISSUER FOLDING AS ACTIVE SHARE (`build_issuer_weights`), so the two views cannot
    disagree about how many positions the book has.

    ⚠ NO PRICE SERIES AT ALL — this is a weights-only measure, so it is much cheaper than the
    other risk views and needs no cadence.
    """
    from routers._portfolio_concentration import compute_concentration  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_concentration, [h.model_dump() for h in req.holdings], benchmark)


class ExposurePosition(BaseModel):
    """One ISSUER's effective position. `lines` > 1 means several holdings folded into it."""

    name: str
    weight_pct: float
    value_eur: float | None = None
    lines: int = 1
    #: ⚠ NAMED WHEN ONE ISSUER SPANS SEVERAL CURRENCIES — two listings of one company is a single
    #: position and two FX exposures, which the issuer fold would otherwise hide by design.
    currencies: list[str] = []


class CurrencyExposure(BaseModel):
    currency: str
    weight_pct: float
    value_eur: float | None = None
    issuers: int = 0


class PortfolioExposure(BaseModel):
    """Effective positions — `Eᵢ = qᵢ·Pᵢ·Xᵢ` — see `routers/_portfolio_exposure.py`.

    ⚠⚠ WE DO NOT COMPUTE THAT PRODUCT. `airs_holding` carries a quantity, but it also carries
    `current_value_eur`: AIRS's OWN valuation, already in euros, already struck on its own date.
    That is the number on the client's statement. Re-deriving it from our close and our FX rate
    would produce a second figure disagreeing with the statement on most rows, with nothing on
    screen able to say which was right. `Eᵢ` here IS that valuation, folded per issuer.

    ⚠ TRADE DATE vs SETTLEMENT DATE IS AIRS'S CONVENTION AND WE CANNOT VERIFY IT FROM HERE. The
    Vermogensoverzicht exposes no flag saying which basis it used, so a book with a very recent
    trade may differ from a trade-date view by that trade's value with nothing in our data showing
    it. Stated rather than assumed away.
    """

    available: bool = True
    reason: str | None = None
    benchmark: str

    #: False for an ad-hoc basket — weights only, no euros anywhere.
    has_values: bool = False
    issuers: int = 0
    lines: int = 0
    #: ⚠ `lines − issuers`, the fold made visible. It is the one-line answer to "why does this
    #: panel count differently from the Holdings table".
    folded_lines: int = 0

    sleeve_eur: float | None = None
    book_eur: float | None = None
    other_eur: float | None = None
    stocks_pct: float | None = None

    positions: list[ExposurePosition] = []
    currencies: list[CurrencyExposure] = []
    #: ⚠ WEIGHT WITH NO CURRENCY WE COULD ASSIGN. Folding it into EUR is the flattering default —
    #: it makes a book look more domestic than it is — so it is reported separately.
    currency_unknown_pct: float = 0.0
    unresolved: int = 0


@router.post("/api/airs/portfolio/exposure", response_model=PortfolioExposure)
async def airs_portfolio_exposure(req: ActiveShareRequest, benchmark: str = "ACWI"):
    """The euros behind the weights, per issuer, and the currency split of the sleeve.

    ⚠ SAME ISSUER FOLDING AS ACTIVE SHARE AND CONCENTRATION (`build_issuer_weights`) — built once
    and read by all three, which is what stops three panels showing three sets of weights for one
    portfolio.
    """
    from routers._portfolio_exposure import compute_exposure  # noqa: PLC0415

    return await asyncio.to_thread(
        compute_exposure, [h.model_dump() for h in req.holdings], benchmark)


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
    # ⚠ TWO DIFFERENT "EXCESS" FIGURES LIVE ON THIS SCREEN AND BOTH ARE RIGHT — the payload
    # carries both so the panel can reconcile them instead of contradicting the tile.
    #   `excess_pct`         the ATTRIBUTABLE SLEEVE's: the holdings that have a bucket at all,
    #                        renormalised once cash and funds come out (cash has no sector, so
    #                        leaving it in would score holding cash as a sector bet).
    #   `account_excess_pct` the ACCOUNT's: AIRS's own flow-aware `cumulatief_rendement` against
    #                        the same benchmark — what the Analyse tile shows.
    # Measured on AITopSelectie OFF DYN: +23.39pp here against +24.26pp there, same benchmark.
    # `unattributed_excess_pct` is the difference — cash, income on positions closed during the
    # year, and the account's flows: real return with no bucket to attribute it to. Null for
    # `source=model`, which has no account behind it.
    account_return_pct: float | None = None
    account_excess_pct: float | None = None
    unattributed_excess_pct: float | None = None
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
    # ⚠ The PRETTY name, falling back to AIRS's `Portefeuille` code — see `linkable_context`.
    _pf_names = {p["id"]: (p.get("display_name") or p["name"]) for p in (
        supabase.table("airs_model_portfolio").select("id,name,display_name").execute().data or [])}

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
    # ⚠ THE NAME WE GAVE IT, not AIRS's. `display_name` is the readable strategy name
    # ("MerkenTopSelectie Beperkt Offensief"); `code` below is AIRS's own `Portefeuille`, capped
    # at 24 characters ("BUS_MTS_BEPOFF_AFS"). The code is what you search AirSPMS for and the
    # wrong thing to pick from a list. Only 42 of 95 portfolios have a display name, so `name`
    # falls back to the code rather than going blank.
    name: str
    code: str | None = None     # AIRS's own Portefeuille, kept so the row stays findable there
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
async def airs_vermogen_refresh(force: bool = False):
    """Trigger the fleet AIRS refresh now (the portfolios page "Refresh all" button). Re-discovers
    the live portfolio list, then downloads + stores the four reports for each account that needs
    them. Runs in a daemon thread and returns immediately; poll `/api/airs/vermogen/status`.

    ⚠ INCREMENTAL. An account whose last pass got all four reports within `AIRS_FRESH_HOURS` is
    skipped — 44 accounts × 4 downloads takes minutes, and re-fetching a report AIRS has not
    republished buys nothing. Discovery itself is never skipped, so an account that is missing
    (deleted, or new in AIRS) is always scanned. `?force=true` re-scans everything.
    """
    from airs_vermogen import _STATUS, run_airs_vermogen_refresh_sync  # noqa: PLC0415

    if _STATUS.get("running"):
        return {"status": "busy", "message": "A refresh is already running"}
    threading.Thread(
        target=run_airs_vermogen_refresh_sync,
        kwargs={"triggered_by": "manual-force" if force else "manual", "force": force},
        daemon=True,
        name="airs-vermogen-manual",
    ).start()
    return {"status": "started", "force": force}


@router.post("/api/airs/vermogen/refresh/job")
async def airs_vermogen_refresh_job(force: bool = False):
    """The fleet re-scan as a CANCELLABLE JOB — the "Refresh all" button.

    ⚠ WHY THIS EXISTS BESIDE THE PLAIN POST ABOVE. That one fires a daemon thread and returns
    immediately; the caller then polls `/api/airs/vermogen/status` every 2.5s and paints its own
    banner. Three things follow from that and all three are why this page kept feeling broken:
    the work is INVISIBLE after a route change or a reload, there is NO WAY TO STOP IT once
    started, and the panel grows a second progress vocabulary that has to be kept in step with the
    toast every other button on the page already uses.

    As a job it reports into the shared toast stack, survives navigation, re-attaches via
    `attachRunningJobs`, and the toast's Cancel actually reaches the scan.

    ⚠ THE SCAN ITSELF IS UNCHANGED — `run_airs_vermogen_refresh_sync` with two optional hooks, not
    a streaming copy of it. A second implementation for the job path is exactly the drift its own
    docstring warns about, and this is the function the 05:00 scheduler tick also calls.

    ⚠ CANCEL STOPS BETWEEN ACCOUNTS, NOT INSIDE ONE, and the result is a real outcome rather than
    a failure: everything already downloaded is stored and the summary says where it stopped. An
    account's four reports are a unit — stopping midway would leave a book with two fresh reports
    and two stale ones and nothing on the row to say which.

    ⚠ `busy` IS AN ANSWER, NOT AN ERROR. `_LOCK` already serialises the scan (the scheduler holds
    it too), so a second press returns a sentence instead of painting a red toast beside real
    failures — the same rule the per-row refresh follows.
    """
    import jobs as job_registry  # noqa: PLC0415
    from jobs import JobCancelled  # noqa: PLC0415

    from airs_vermogen import run_airs_vermogen_refresh_sync  # noqa: PLC0415

    def _work(ctx) -> str:
        res = run_airs_vermogen_refresh_sync(
            triggered_by="manual-force" if force else "manual",
            force=force,
            on_step=lambda done, total, msg: ctx.progress(done, total, msg),
            # ⚠ `ctx.check()` RAISES to unwind a job; here we only need the FLAG, because the scan
            # has to run its own finalisation (write the status, release `_LOCK`) before returning.
            # `cancelled` is the registry's own view of whether Cancel was pressed.
            should_stop=lambda: ctx.cancelled,
        )
        if res.get("status") == "busy":
            return "Another AIRS refresh is already running — nothing was re-read"
        stopped = res.get("cancelled_at")
        errs = res.get("errors") or []
        # ⚠ THE DATA'S OWN DATE RIDES ALONG. The counts are about what WE fetched; the row
        # badges measure AIRS's valuation date, and without this the two read as contradictory
        # ("44 accounts" against "3 trading days old"). See `format_run_message`.
        summary = (f"{res.get('complete_accounts', 0)}/{res.get('portfolios_found', 0)} accounts, "
                   f"{res.get('holdings_rows', 0)} holdings"
                   + (f" · newest AIRS valuation {res['newest_as_of']}"
                      if res.get('newest_as_of') else ''))
        if stopped:
            # ⚠⚠ RAISED, NOT RETURNED, AND THAT IS A BUG FIX RATHER THAN A TIDY-UP. A worker that
            # RETURNS is `done` — and `done` is not decoration here, the button ACTS on it: the
            # frontend runs phase two (the minutes-long model scan) only `if (job.status ===
            # 'done')`, precisely so a Cancel does not buy the reader minutes more of exactly what
            # they asked to stop. Returning a string beginning "Cancelled" satisfied the reader and
            # not the gate, so Cancel painted a green card and then started the slow half anyway —
            # which from the outside is a Cancel button that does nothing.
            #
            # `JobCancelled`'s message becomes the summary, so the sentence below is not lost.
            raise JobCancelled(f"Cancelled before {stopped} — {summary} stored before stopping")
        if res.get("status") == "error":
            raise RuntimeError(res.get("message") or "AIRS scan stored nothing")
        return summary + (f" — {len(errs)} report(s) failed, see the console" if errs else "")

    job, reused = job_registry.start("airs.vermogen.refresh", "Refresh all portfolios", _work)
    return {"job_id": job.id, "label": "Refresh all portfolios", "force": force,
            "already_running": reused}


class AirsAccountDeleted(BaseModel):
    portefeuille: str
    deleted: dict[str, int] = {}     # per table; -1 = that table's delete errored
    total_rows: int = 0


@router.delete("/api/airs/portfolios/{portefeuille}", response_model=AirsAccountDeleted)
async def airs_portfolio_delete(portefeuille: str):
    """Delete ONE account's scraped rows — returns, holdings, mutations, model weights, its roster
    entry and its model pairing — so a refresh can be watched rebuilding them.

    ⚠ NOT THE WAY TO REMOVE AN UNWANTED ACCOUNT. The next scrape re-creates everything it can see,
    so a delete achieves nothing there and costs history; `airs_account_hidden` records that
    decision instead. This exists to prove the refresh refills a gap.

    ⚠ IT LOSES ANYTHING OLDER THAN 1 JANUARY. A scan fetches `1 Jan → today`, so `airs_performance`
    months before that are gone permanently — the UI says so before asking. CRM records and the
    hidden-account decision are deliberately NOT touched (see `_DELETABLE_TABLES`).
    """
    from airs_vermogen import delete_account  # noqa: PLC0415

    return await asyncio.to_thread(delete_account, portefeuille)


class AirsAccountName(BaseModel):
    """A nickname for one account. Empty or absent CLEARS it, restoring the fallback chain."""

    display_name: str | None = None


@router.put("/api/airs/accounts/{portefeuille}/display-name")
async def airs_account_set_display_name(portefeuille: str, body: AirsAccountName):
    """Name one AIRS account, or clear the name.

    ⚠ THE NAME BELONGS TO THE ACCOUNT, NOT TO THE MODEL IT RUNS. `display_name` on
    `airs_model_portfolio` names a strategy, and an account borrowed it through its pairing — so a
    book paired with no model could not be named at all, which is exactly backwards: those are the
    books still wearing AIRS's own code (`BUS_Ris_bepOff_Kl_AFS_Dy`) and most in need of one. Two
    accounts running one model may also deserve different names, and renaming a model must not
    silently rename every book paired with it.

    ⚠ CLEARING IS A DELETE, NOT AN EMPTY STRING. A stored "" would be a name that renders as
    nothing, indistinguishable on screen from an un-named row and invisible to the fallback chain.
    """
    def _run() -> dict:
        key = (portefeuille or "").strip()
        if not key:
            raise HTTPException(status_code=422, detail="portefeuille is required")
        name = (body.display_name or "").strip()
        if not name:
            supabase.table("airs_account_display_name").delete().eq("portefeuille", key).execute()
            return {"portefeuille": key, "display_name": None, "cleared": True}
        supabase.table("airs_account_display_name").upsert(
            {"portefeuille": key, "display_name": name,
             "updated_at": datetime.now(UTC).isoformat()},
            on_conflict="portefeuille").execute()
        return {"portefeuille": key, "display_name": name, "cleared": False}

    return await asyncio.to_thread(_run)


@router.post("/api/airs/portfolios/{portefeuille}/refresh")
async def airs_portfolio_refresh(portefeuille: str, cascade: bool = True):
    """Re-scan ONE portfolio's AIRS reports — and the books it is BUILT FROM.

    ⚠ A HOLDING CAN BE ANOTHER BOOK. Some positions are Leonteq certificates wrapping another
    strategy, and everything shown through one — the looked-through holdings, their returns, the
    attribution — is read from the WRAPPED book's own scan. Refreshing the parent alone re-reads
    the twelve lines it stores and leaves the instruments behind them as stale as they were.
    Measured: BUS_Offensief_Dyn is built on one other account, TOPS_BEOFF_BEH_DYN on NINE.

    ⚠ SO THIS IS NOT ALWAYS "a few seconds" ANY MORE — it is FIVE downloads per account in the
    chain (Rendement, Vermogensoverzicht, Mutaties, Transacties, Model). Each one's outcome comes back in `cascaded` rather than being folded into a single
    status, because a parent refreshed against a child that failed is not fresh. `cascade=false`
    refreshes only the named account.

    ⚠ AND IT REFRESHES BOTH HALVES OF THE PORTFOLIO — the AIRS book AND the model it is paired
    with — through `refresh_portfolio_fully`, like every other refresh button. It used to run the
    book alone, which is why the same portfolio could read differently depending on which page's
    Refresh you had last pressed. The book half is still what the response's top level describes
    (that is what this route's callers read); `model` carries the other half.

    Serialized against the full scan via the module lock; returns `{status: busy}` if a fleet
    refresh is in flight."""
    from routers._airs_full_refresh import refresh_portfolio_fully  # noqa: PLC0415

    full = await asyncio.to_thread(refresh_portfolio_fully, portefeuille, None, cascade=cascade)
    # ⚠ THE BOOK HALF STAYS AT THE TOP LEVEL — this is a documented response shape that scripts and
    # `AirsPortfolioUpload` read by key. Nesting it under `book` to make room for the model would
    # be a silent breaking change to callers we do not control; the model rides alongside instead.
    return {**(full.get("book") or {}), "model": full.get("model"),
            "model_status": full.get("model_status"), "full_status": full.get("status")}


@router.post("/api/airs/portfolios/{portefeuille}/refresh/job")
async def airs_portfolio_refresh_job(portefeuille: str, cascade: bool = True):
    """The same re-scan as above, as a CANCELLABLE JOB that reports progress.

    ⚠ WHY A JOB FOR A "few seconds" REFRESH. It is not a few seconds any more: with the cascade it
    is five downloads per account over a chain that reaches NINE (TOPS_BEOFF_BEH_DYN). Held open as
    one POST, the caller gets a disabled button and no line moving — indistinguishable from a hung
    one — and navigating away abandons work that carries on invisibly. As a job it reports into the
    shared toast stack, survives the route change, and re-attaches on reload (`attachRunningJobs`).

    ⚠ THE SAME `refresh_one_portfolio`, WITH A LISTENER — not a streaming copy of it. That function
    is already the one body the fleet scan and the per-row refresh share; a second version for the
    job path is exactly the drift its own docstring exists to prevent. The plain POST above stays
    for scripts and for anything that wants one blocking answer.

    ⚠⚠ IT IS CANCELLABLE BETWEEN ACCOUNTS, AND THAT REVERSES WHAT THIS DOCSTRING USED TO SAY
    (2026-08-13). It passed no `should_stop` and argued that a half-cascade leaves a parent fresh
    against stale children, so the job "reports, it does not stop". The cost of that was a Cancel
    button — on the row, in the Analyse modal and on the toast itself — that changed nothing for
    minutes while its card read "cancelling…" and then finished `done`. The argument also proves too
    much: `cascade=False` produces the identical state on purpose. So the scan now stops at an
    account boundary and NAMES the books it left stale (`cancelled_at`, `stale_books`), which is the
    honest version of the same compromise. `_LOCK` still refuses a second one.

    ⚠ AND THE JOB ENDS `cancelled`, NOT `done`. `_work` returning a string — however carefully it is
    worded — is a `done` job to the registry, so the toast would go green and the summary would be
    the only thing saying otherwise. `JobCancelled` is what makes the card amber, and it carries the
    detail as its message so the summary still names the books left stale.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._airs_full_refresh import refresh_portfolio_fully  # noqa: PLC0415

    def _work(ctx) -> str:
        full = refresh_portfolio_fully(
            portefeuille=portefeuille, cascade=cascade,
            on_step=lambda done, total, msg: ctx.progress(done, total, msg),
            # ⚠ THE FLAG, NOT `ctx.check()`. The scan has to reach its own `finally` to release
            # `_LOCK`; unwinding it with an exception from the inside would leave the AirSPMS
            # session locked against every later refresh. Same reason the fleet job gives.
            should_stop=lambda: ctx.cancelled)
        # ⚠ THE BOOK HALF STILL DRIVES THIS SUMMARY, because that is what this endpoint's caller
        # asked about and every branch below reads its keys. The MODEL half is reported as one
        # extra clause rather than folded in — a reader who pressed "Refresh" on an account row
        # should see that its model was rebuilt too, not have the two averaged into one word.
        res = full.get("book") or {}
        model_note = ("" if full.get("model_status") == "absent"
                      else " · model portfolio rebuilt" if full.get("model_status") == "ok"
                      else f" · ⚠ model portfolio NOT rebuilt ({full.get('model_status')})")
        if res.get("cancelled_at"):
            stale = res.get("stale_books") or []
            raise job_registry.JobCancelled(
                f"cancelled before {res['cancelled_at']}"
                + (f" — {len(stale)} book(s) left un-refreshed: {', '.join(stale)}"
                   if stale else " — nothing was read"))
        if res.get("status") == "busy":
            # ⚠ AN ANSWER, NOT A FAILURE. The fleet scan holds the session; this is a "try again",
            # and raising would paint it red beside the real errors.
            return f"{portefeuille} — another AIRS refresh is running; nothing was re-read"
        also = res.get("cascaded") or []
        bad = [c for c in also if c.get("status") != "ok"]
        if res.get("status") != "ok":
            raise RuntimeError(
                f"{portefeuille}: AIRS scan failed — {', '.join(res.get('errors') or []) or 'no reports returned'}")
        # ⚠ A FAILED DEPENDENCY DOWNGRADES THE WHOLE SUMMARY, exactly as the inline message did:
        # the parent's own scan succeeded, but its looked-through figures are read from a book that
        # did not, and one word saying "refreshed" would claim a freshness it does not have.
        return (f"{portefeuille} — {res.get('holdings_rows', 0)} holdings as of "
                f"{res.get('as_of') or 'today'}"
                + (f" · also refreshed {len(also)} book(s) it is built from" if also else '')
                + model_note
                + (f" — {len(bad)} FAILED, see the console" if bad else ''))

    job, reused = job_registry.start("airs.portfolio.refresh", portefeuille, _work)
    return {"job_id": job.id, "label": portefeuille, "already_running": reused}


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
    # Which of the four AIRS reports the last scan did NOT retrieve for this account
    # (`att` / `volk` / `mut` / `model`); empty when it is whole.
    #
    # ⚠ THE ROW IS MARKED, NOT WITHHELD. These accounts were briefly filtered OUT of this list
    # entirely — so a scan that reached all 44 portfolios returned 22, and nobody could see which
    # report was short or for whom. The figures here are all real; they just do not all describe
    # the same date, and that is a caveat to display, not a reason to delete the row.
    missing_reports: list[str] = []
    # When WE last read this account (`airs_account_roster.reports_at`). Paired with `as_of` —
    # the day AIRS VALUED the book — it is what lets a stale badge say whose lag it is. See
    # `_airs_accounts._fetched_at`.
    fetched_at: str | None = None


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
    # ⚠ `as_of` IS AIRS'S VALUATION DATE; THIS IS WHEN **WE** LAST READ THE BOOK, and the pair is
    # what lets the panel's ⓘ say whose lag an old date is. Without it every icon in the expanded
    # view went amber on a book we had read that same afternoon — an alarm nobody could clear, on
    # the surface with the most room to explain itself. See `_airs_accounts._fetched_at`.
    fetched_at: str | None = None
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
    # True when a human named THIS account (`airs_account_display_name`), rather than the name
    # being borrowed from a paired model or falling back to AIRS's own code.
    name_is_custom: bool = False
    description: str | None = None
    dynamic_portefeuille: str
    fixed_name: str | None = None
    fixed_portfolio_id: int | None = None
    fixed_type: str | None = None
    # ⚠ THE BOOK'S OWN DISTINCT ISINs, not the paired model's position count. It was the latter,
    # so an unpaired book showed "—" beside 22 holdings you could see on expanding it, and a paired
    # book's number described a different object. None = no snapshot stored; NOT 0.
    isins: int | None = None
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
    # Which of the four AIRS reports the last scan did NOT retrieve (`att`/`volk`/`mut`/`model`).
    #
    # ⚠ THE ROW IS MARKED, NOT WITHHELD. These accounts were briefly filtered out of the list — so
    # a scan that reached all 44 portfolios showed 22, and nobody could see which report was short
    # or for whom. Every figure here is real; they just do not all describe the same date, which is
    # a caveat to display rather than a reason to delete the row.
    missing_reports: list[str] = []
    # When WE last read this account (`airs_account_roster.reports_at`). Paired with `as_of` —
    # the day AIRS VALUED the book — it is what lets a stale badge say whose lag it is. See
    # `_airs_accounts._fetched_at`.
    fetched_at: str | None = None


@router.get("/api/airs/portfolios/overview", response_model=list[AirsPortfolioOverview])
async def airs_portfolios_overview():
    """Every AIRS book in one table: named by the Fixed portfolio it runs, valued by AIRS.

    The Fixed side has the ISINs and your nickname and AIRS values none of it; the Dynamic side
    has the money and no ISIN. Overlap between the two: zero. This is the pair, composed.
    """
    from common.read_cache import read_cache  # noqa: PLC0415

    from routers._airs_overview import list_overview_async  # noqa: PLC0415

    # ⚠ THE MEMO IS WORTH FAR MORE THAN THE CALL COUNT SUGGESTS. It removes only 3 of 17 round
    # trips here — but those three are repeats of the whole `airs_performance` history (912KB), so
    # measured locally the endpoint goes 1,109ms -> 277ms. A request is exactly the scope over
    # which "the database did not change under us" is safe; see `common/read_cache.py`.
    with read_cache("overview"):
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
    # The asset-class label — Equity | Bonds | Alternatives | Cash | Unclassified
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
    # ok | price_mismatch | stale_price | cross_listed | unpriced.
    # ⚠ `stale_price` IS NOT A SOFTER `price_mismatch`. The ratio is out of tolerance, but our close
    # is more than a week from the day AIRS valued the book — and the refresh has already tried to
    # fetch the gap, so Yahoo has nothing newer for this line. The two numbers describe different
    # days, which makes the gap TIME and not identity: calling it a mismatch says "our listing is
    # wrong" about a listing that may be perfect. Measured 2026-07-29: AMD fell 22% while our
    # newest bar sat six days back, and every stored bar matched Yahoo to the cent.
    verdict: str
    our_instrument: str | None = None
    # The date of the close on OUR side of the ratio, and how far it sits from `as_of`. A ratio
    # without its two dates cannot be argued with.
    our_price_date: str | None = None
    price_lag_days: int | None = None
    # Set when this ISIN is deliberately served by ANOTHER ISIN's instrument (an ADR priced from
    # the main company's listing). ⚠ The two do not trade at the same number — TSMC is 1 ADR = 5
    # ordinary shares — so a price difference on such a row is expected, not a finding.
    served_by: str | None = None
    # ── The model portfolio this holding IS, when it is one ────────────────────────────────
    # Some holdings are not instruments: they are other model portfolios wrapped as a Leonteq
    # certificate so they can be held like a security. They are CH ISINs Yahoo can never price
    # (`verdict='unpriced'`, correctly — there is no listing for a structured product), so they
    # sit here as dead rows whose weight leaves the coverage denominator. The link is what lets a
    # reader see through the wrapper to the strategy behind it.
    #
    # ⚠ SAME STORE AS THE MODEL-PORTFOLIO POSITIONS TABLE (`airs_model_portfolio_link`), which is
    # keyed on the HOLDING and not on (parent, holding). One certificate is the same portfolio
    # whichever account or model holds it, so a link decided on either screen is the same
    # decision — and the two screens cannot come to disagree.
    linked_portfolio_id: int | None = None
    linked_portfolio_name: str | None = None
    link_source: str | None = None         # 'manual' (a decision) | 'auto' (a guess)
    link_confidence: float | None = None   # 0-1; NULL for manual — a choice is not a guess
    link_reason: str | None = None


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
    # Per-phase milliseconds for THIS request — see `_airs_holding_isin._phase`. Expanding a row
    # is a dozen distinct steps and the slow one is not the obvious one, so the breakdown travels
    # with the payload and lands in the operator's console rather than only in a server log.
    timings_ms: dict[str, int] = {}
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


class AirsAccountTransactions(BaseModel):
    """One account's AIRS Transacties, as the SHEET — no schema imposed on it.

    ⚠ THE COLUMNS ARE DATA HERE, NOT A CONTRACT. `rapport_types=TRANS` returns an XLS (probed
    2026-07-23) and no column of it has ever been measured, so this endpoint reports the report:
    `columns` in the sheet's own order, `kinds` giving each one's pandas-inferred type, and `rows`
    keyed by column name. Naming fields against a sheet nobody has read is how `Bedrag` gets
    charted where `Bedrag eur` belonged — one word apart, and the wrong one is a plausible number
    rather than an error. See `airs_transacties` for the full reasoning and for what replaces this
    once the sheet has been seen.

    ⚠ `source` AND `note` ARE THE ANSWER'S OWN PROVENANCE. An empty `rows` means one of three very
    different things — the book did not trade, AIRS has no such report for it, or we could not ask
    — and an empty table with nothing beside it asserts the first.
    """

    portefeuille: str
    datum_van: str
    datum_tot: str
    columns: list[str] = []
    # column -> 'number' | 'date' | 'text'
    kinds: dict[str, str] = {}
    # ⚠ Values are float | str | null and NEVER NaN — a blank Excel cell is float NaN, which is not
    # JSON and which `str()` renders as the TRUTHY string "nan".
    rows: list[dict[str, float | str | None]] = []
    # When the stored snapshot was fetched. Null on a live answer — the UI says "cached" only when
    # there is a date to show for it.
    cached_at: str | None = None
    # 'cache' | 'live' | 'unavailable'
    source: str
    note: str | None = None


@router.get("/api/airs/accounts/{portefeuille}/transactions",
            response_model=AirsAccountTransactions)
async def airs_account_transactions(portefeuille: str, refresh: bool = False):
    """What this book BOUGHT and SOLD this year — the AIRS Transacties report.

    The three reports already stored say what a book holds (VOLK), what it earned (MUT) and what
    its strategy asks for (MODEL). None says what it DID, so a position that appeared mid-year, one
    sold out entirely, and a weight that drifted purely on price all look the same from outside.

    Served from the stored snapshot; `refresh=true` (or a stale window, or nothing stored) goes out
    to AIRS. A live fetch is seconds behind a headless session, which is why the panel is behind a
    click and the answer says whether it was cached.
    """
    import asyncio  # noqa: PLC0415

    from routers._airs_transacties import account_transactions  # noqa: PLC0415

    return await asyncio.to_thread(account_transactions, portefeuille, refresh)


class AirsRealisedLeg(BaseModel):
    """One instrument's realised result this year, summed over its sales.

    ⚠ `closed_out` IS DECIDED BY ABSENCE FROM THE HOLDINGS, NOT BY PRESENCE HERE. A sale is a
    REALISATION, not a closure — Synopsys was trimmed on 2026-01-22 and is still held — so a name
    can legitimately appear both here and in the positions table above.
    """

    fonds: str
    sales: int = 0
    quantity: float = 0.0
    proceeds_eur: float = 0.0
    cost_eur: float = 0.0
    # ⚠ AIRS's own `Res. YtD`, never `proceeds − cost`: the two differ by `prior_year_eur` on any
    # position carried across a year end, and they coincide on every book bought entirely this
    # year — so the account this was validated on could not have told them apart.
    realised_ytd_eur: float = 0.0
    prior_year_eur: float = 0.0
    first: str | None = None
    last: str | None = None
    closed_out: bool = False


class AirsAccountReconciliation(BaseModel):
    """The book's own YTD, lined up against what its positions — held AND sold — explain.

    ⚠ THE TWO NUMBERS ARE ALREADY ON SCREEN A FEW LINES APART AND THEY DISAGREE. Measured
    2026-08-05 over 39 accounts, **23 disagree by more than 1pp** (BUS_FTS_BEPOFF_DYN: the book
    made -4.57%, its open positions +3.27pp more than that). Both are correct answers to different
    questions, and a reader given both with no arithmetic between them cannot arbitrate.

    ⚠ EVERY COMPONENT IS A EURO AMOUNT. The two percentages are measured on different opening
    capitals, so they do not subtract into anything meaningful — `gap_pp` is reported for
    orientation and is deliberately named in POINTS, never divided into.
    """

    portefeuille: str
    as_of: str | None = None
    # The ATT period the book side covers, and how many monthly rows it spans.
    periode: str | None = None
    months: int | None = None

    # ── The book's own year, from AIRS. Authoritative; never recomputed here.
    book_return_pct: float | None = None
    # ⚠ `beleggingsresultaat`, NOT end − begin: the difference is deposits, and a book that took
    # EUR 1m mid-year would report the deposit as profit.
    book_result_eur: float | None = None
    book_start_eur: float | None = None
    book_end_eur: float | None = None
    deposits_eur: float = 0.0
    withdrawals_eur: float = 0.0
    costs_eur: float = 0.0
    book_reconciles: bool | None = None

    # ── What the positions still held explain, on the positions table's own basis.
    open_return_pct: float | None = None
    open_result_eur: float | None = None
    open_start_eur: float | None = None
    open_end_eur: float | None = None
    open_priced: int = 0
    open_unpriced: int = 0

    # ── Measured, so it leaves the residual: income paid by funds no longer held.
    sold_income_eur: float = 0.0
    sold_funds: list[str] = []

    # ── What the sales realised this year, from the cached Transacties sheet.
    # ⚠ None is NOT zero. No sheet cached means the realised result is UNKNOWN; publishing 0 would
    # hand the open positions' figure to the reader as the year's total.
    realised_ytd_eur: float | None = None
    realised_names: int = 0
    realised_note: str | None = None
    realised: list[AirsRealisedLeg] = []
    buys_eur: float | None = None
    buy_count: int | None = None

    # ⚠ THE NET OF TWO OPPOSITE EFFECTS, and labelled as such rather than as "closed positions":
    # a position sold outright leaves opening value in the book with no row to carry it, while
    # AIRS restating `Beginwaarde` to the CURRENT quantity inflates the rows for anything bought
    # into during the year. On AITopSelectie the net is NEGATIVE.
    start_gap_eur: float | None = None

    # ── The year, built from the positions: held + realised + income from names no longer held.
    total_result_eur: float | None = None
    # ⚠⚠ ONLY ON A FLOW-FREE BOOK. `result ÷ opening capital` reproduces `cumulatief_rendement`
    # exactly (measured: 38.729379% against AIRS's 38.729375%) when nothing was paid in or out,
    # and is undefined the moment something was — AzTopSelectie opened at ZERO and took EUR 1m.
    # `return_basis` says which case the reader is in: 'opening_capital' | 'flows' | 'unavailable'.
    total_return_pct: float | None = None
    return_basis: str | None = None
    # ⚠ ASSERTED, NEVER ASSUMED — a total never set against the book's own is an assertion, not a
    # reconciliation. Measured residual EUR 0.04 on a EUR 387,293.75 year.
    residual_vs_book_eur: float | None = None
    # ⚠ None is UNKNOWN, not False. The held leg is the VOLK holdings snapshot and the result is
    # the ATT report — two downloads, routinely a day apart — and one day of market movement on a
    # EUR 1.4m book reads as tens of thousands of "unexplained" residual. See `dates_aligned`.
    reconciles: bool | None = None
    holdings_as_of: str | None = None
    book_as_of: str | None = None
    dates_aligned: bool | None = None
    residual_reason: str | None = None

    # The gap BEFORE the sales are counted, kept so the reader can see how much they closed.
    # ⚠ NOT distributed across the components above.
    unexplained_eur: float | None = None
    gap_pp: float | None = None
    # Cached Transacties rows. None = never fetched.
    transaction_rows: int | None = None
    # Transaction types the parser does not interpret, with counts (measured: {'D': 1}, a
    # quantity-only row carrying no money). Counted rather than assumed harmless.
    unknown_transaction_types: dict[str, int] = {}


@router.get("/api/airs/accounts/{portefeuille}/return-reconciliation",
            response_model=AirsAccountReconciliation)
async def airs_account_return_reconciliation(portefeuille: str):
    """Why this book's own YTD is not the YTD its open positions add up to.

    Reads both sides from the loaders the other panels already use — `_year_perf` for the book,
    `account_holdings` for the positions — so the reconciliation cannot quietly disagree with
    either of the figures it is reconciling.
    """
    import asyncio  # noqa: PLC0415

    from routers._airs_return_reconciliation import account_return_reconciliation  # noqa: PLC0415

    return await asyncio.to_thread(account_return_reconciliation, portefeuille)


@router.get("/api/airs/accounts/{portefeuille}/linkable", response_model=LinkableContext)
async def airs_account_linkable_portfolios(portefeuille: str):
    """What an ACCOUNT's holdings may be linked to — the same dropdown the model-portfolio
    positions table uses, and the same gates.

    ⚠ "SELF" FOR AN ACCOUNT IS THE MODEL IT RUNS. `linkable_context` excludes the owner so a
    portfolio cannot be its own holding; an account is not a model, so its analogue is the model
    it is paired with on `/account-model-links`. A certificate of the account's own strategy is
    exactly the wrapper cycle the gate exists to stop. An unpaired account excludes nothing,
    which is right: we do not know its strategy, so we cannot say which link would be circular.

    ONE call for the whole table — per row it would be a request per holding.
    """
    from routers._airs_holding_isin import _account_owner_model_id  # noqa: PLC0415
    from routers._airs_portfolio_links import linkable_context  # noqa: PLC0415

    owner = await asyncio.to_thread(_account_owner_model_id, portefeuille)
    return await asyncio.to_thread(linkable_context, supabase, owner)


@router.put("/api/airs/accounts/{portefeuille}/link")
async def airs_set_account_holding_link(portefeuille: str, body: SetLinkRequest):
    """Point one of an ACCOUNT's holdings at the model portfolio it IS (or, with a null target,
    record that it is not one).

    ⚠ THE SAME ROW THE MODEL-PORTFOLIO SCREEN WRITES. `airs_model_portfolio_link` is keyed on the
    holding, not on (parent, holding) — one certificate is the same portfolio wherever it is held
    — so this is not a second store for the same fact, and the two screens cannot disagree.
    """
    from routers._airs_holding_isin import _account_owner_model_id  # noqa: PLC0415

    owner = await asyncio.to_thread(_account_owner_model_id, portefeuille)
    return await asyncio.to_thread(_save_link, owner, body)


@router.delete("/api/airs/accounts/{portefeuille}/link")
async def airs_clear_account_holding_link(portefeuille: str, isin: str | None = None,
                                          fonds: str = ""):
    """Forget the human decision for this holding and fall back to the automatic guess. NOT the
    same as linking it to nothing — that is a decision too, and is stored as a null.

    `portefeuille` names which table the request came from; the row it clears is the same one the
    model-portfolio screen writes, because the link is keyed on the holding.
    """
    return await airs_clear_portfolio_link(0, isin=isin, fonds=fonds)


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


class AllocationBandGrid(BaseModel):
    """The whole policy, always complete: every profile × every invested class.

    `variants` and `buckets` ship with it so the editor renders the grid the SERVER knows about
    rather than a copy of it — add a fifth risk profile to `VARIANTS` and the editor grows a column
    without a frontend change, which is the only way the two cannot drift.
    """

    variants: list[str]
    buckets: list[str]
    cells: list[AllocationBand]


@router.get("/api/airs/allocation-bands", response_model=AllocationBandGrid)
async def airs_allocation_bands():
    """The allocation policy — all sixteen cells, nulls where nothing is set."""
    from routers._airs_allocation_bands import POLICY_BUCKETS, load_bands  # noqa: PLC0415
    from routers._airs_portfolio_variant import VARIANTS  # noqa: PLC0415

    cells = await asyncio.to_thread(load_bands)
    return {"variants": list(VARIANTS), "buckets": list(POLICY_BUCKETS), "cells": cells}


@router.put("/api/airs/allocation-bands", response_model=AllocationBandGrid)
async def airs_set_allocation_bands(body: list[AllocationBand]):
    """Apply these cells to the policy. Admin-only (the API gate refuses a non-admin write).

    ⚠ PARTIAL BY DESIGN — send only the cells you changed. A cell that IS sent and is empty means
    "clear this row"; a cell that is not sent means nothing at all. Sending the full grid from a
    stale view therefore deletes everything that changed since it loaded, which is not theoretical:
    it wiped 15 of 16 seeded rows on 2026-08-04, silently.

    ⚠ VALIDATED IN FULL BEFORE ANYTHING IS WRITTEN. A save is ONE intent, so a bad cell rejects the
    whole submission with a sentence naming it — landing the first eight and refusing the ninth
    would leave a policy half-updated while the reader believes all of it took.

    Returns the WHOLE grid as stored, so the editor renders what the database now holds — including
    any cell somebody else changed while it was open — rather than what it hoped it sent.
    """
    from routers._airs_allocation_bands import POLICY_BUCKETS, load_bands, save_bands  # noqa: PLC0415
    from routers._airs_portfolio_variant import VARIANTS  # noqa: PLC0415

    def _run() -> dict:
        try:
            save_bands([c.model_dump() for c in body])
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        return {"variants": list(VARIANTS), "buckets": list(POLICY_BUCKETS), "cells": load_bands()}

    return await asyncio.to_thread(_run)


def _fundamentals_scope(isins: list[str]) -> tuple[list[int], int, int, int]:
    """`(company_ids, holdings, no_fundamentals, no_company)` for a set of held ISINs.

    Shared by the two entry points — a model portfolio names its positions, an account-derived
    basket names its holdings, and from here they are the same question.

    ⚠ RESOLVE THE ALIAS BEFORE THE LOOKUP. `company` is keyed on the RAW ISIN, so an issuer we hold
    under one ISIN and ingested under another is invisible to a direct match — the ADR-vs-home-line
    split (`company_override` kind `alias`). Measured: without this, AITopSelectie reads 19 of 20
    and the "missing" one is Taiwan Semiconductor, whose alias we already had.

    ⚠⚠ THE REMAINDER IS TWO DIFFERENT ABSENCES AND THE CALLER MUST BE ABLE TO TELL THEM APART. An
    ETF, a bond or a cash line HAS no company fundamentals by definition; a missing `company` row
    for an operating company is a real gap worth fixing. Measured on BUS_Neutraal_FX: 24 of 40
    reachable, of which eleven are funds/bonds and only five are gaps. One number for both makes a
    correct answer look broken.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415

    from routers._fundamental_coverage import classify_holding  # noqa: PLC0415

    canon = canonical_map(isins)
    wanted = sorted({canon.get(i, i) for i in isins})

    by_isin: dict[str, int] = {}
    for i in range(0, len(wanted), IN_CHUNK_SIZE):
        for c in (supabase.table("company").select("company_id,isin")
                  .in_("isin", wanted[i:i + IN_CHUNK_SIZE]).execute().data or []):
            by_isin[(c.get("isin") or "").strip().upper()] = c["company_id"]

    missing = [i for i in wanted if i not in by_isin]
    grid: dict[str, dict] = {}
    for i in range(0, len(missing), IN_CHUNK_SIZE):
        for g in (supabase.table("asset_grid").select("isin,asset_class,leonteq_product_type")
                  .in_("isin", missing[i:i + IN_CHUNK_SIZE]).execute().data or []):
            grid[(g.get("isin") or "").strip().upper()] = g
    verdicts = [classify_holding(i, grid.get(i), has_company=False, subscribed=None)
                for i in missing]
    no_fundamentals = sum(1 for v in verdicts if v in ("not_equity", "fund", "cash"))
    return (sorted(set(by_isin.values())), len(wanted),
            no_fundamentals, len(missing) - no_fundamentals)


class PortfolioFundamentalsJob(BaseModel):
    """The job handle, plus the two counts that keep the button honest.

    ⚠ `holdings` AND `reachable` ARE BOTH RETURNED BECAUSE THEY DIFFER. The gap is holdings whose
    ISIN has no `company` row — an ADR held under a different ISIN from the one we ingested, a
    structured product, an in-house fund. Returning only the number fetched would let the UI imply
    it covered the portfolio.
    """

    job_id: str
    label: str
    holdings: int
    reachable: int
    # ⚠ THE UNREACHABLE REMAINDER IS TWO DIFFERENT THINGS AND THE UI MUST NOT MERGE THEM.
    # Measured: AITopSelectie is 19 of 20 (Taiwan Semiconductor, held under its US ADR ISIN — a
    # real GAP, fixable), while BUS_Neutraal_FX is 24 of 40 and every one of the missing 16 is an
    # ETF, fund, bond or certificate — instruments that HAVE no company fundamentals by definition.
    # "24 of 40" with one explanation reads as broken when it is correct.
    no_fundamentals: int
    no_company: int


@router.post("/api/airs/model-portfolios/{portfolio_id}/fundamentals/ingest/job",
             response_model=PortfolioFundamentalsJob)
async def ingest_portfolio_fundamentals_job(portfolio_id: int, force: bool = True,
                                            only_due: bool = True, feeds: str = "statements",
                                            limit: int = 0, prices: bool = False):
    """Refresh GuruFocus fundamentals for every company this model portfolio holds.

    The portfolio-scoped twin of the benchmark fill. ⚠ IT IS THE SAME FILL, not a copy — see
    `routers/_fundamental_fill.py`. Only the selector differs: an index names its constituents,
    a portfolio names its holdings.

    ⚠⚠ THE ISIN -> company BRIDGE IS PARTIAL, AND THE COUNT MUST SAY SO. A model holds instruments
    by ISIN; GuruFocus fundamentals hang off `company`, joined on `company.isin`. Measured on
    AITopSelectie OFF FX: 19 of 20 holdings resolve, and the missing one is Taiwan Semiconductor,
    held via its US ADR ISIN (US8740391003) while the company world carries the Taiwan line
    (TW0002330008). "Refreshed 19 holdings" is true; implying the portfolio is covered is not, so
    `holdings` and `reachable` are both returned and the caller shows `n of m`.

    ⚠ CERTIFICATES ARE NOT LOOKED THROUGH. A Leonteq AMC that IS another model contributes no
    company of its own; refresh that model from its own row. Expanding here would make one press
    fan out across portfolios without saying so.

    ⚠ `force=True` BY DEFAULT, AND IT HAS TO BE. Without it `needs()` sees the sentinel row and
    runs nothing, and even selected, `is_cache_fresh` replays the Storage blob for months — the
    two caches that make the fundamentals grid's per-row Fetch a no-op for a company that already
    has data. This button exists precisely to get data we do not yet have.

    ⚠ `only_due=True` IS WHAT MAKES IT CHEAP. The detector (`ingest.earnings.due`) drops the
    holdings whose next fiscal period cannot plausibly have been filed yet, so a press costs one
    API call per company that might actually have something — and zero when none do.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._fundamental_fill import fill_company_ids  # noqa: PLC0415

    p = (supabase.table("airs_model_portfolio").select("id,name,positions_datum")
         .eq("id", portfolio_id).limit(1).execute().data or [])
    if not p:
        raise HTTPException(status_code=404, detail=f"No model portfolio {portfolio_id}.")
    name = p[0].get("name") or f"portfolio {portfolio_id}"

    pos = (supabase.table("airs_model_portfolio_position").select("isin,datum")
           .eq("portfolio_id", portfolio_id).execute().data or [])
    if p[0].get("positions_datum"):
        pos = [r for r in pos if r.get("datum") == p[0]["positions_datum"]]
    isins = sorted({(r.get("isin") or "").strip().upper() for r in pos if r.get("isin")})
    ids, holdings, no_fundamentals, no_company = _fundamentals_scope(isins)

    def _work(ctx) -> str:
        # ⚠ `prices` IS OFF BY DEFAULT so every existing caller is unchanged. The Fundamental modal
        # asks for it because its own tabs price things — Quick Valuation shows today's share price
        # and charts the multiple off the daily closes, neither of which is one of the three
        # GuruFocus fundamentals feeds. See `_refresh_prices`.
        return fill_company_ids(ctx, name, ids, feeds=feeds, force=force, limit=limit,
                                only_due=only_due, prices=prices)

    job, reused = job_registry.start("fundamentals.portfolio", name, _work)
    return {"job_id": job.id, "label": name, "holdings": holdings, "reachable": len(ids),
            "no_fundamentals": no_fundamentals, "no_company": no_company,
            "already_running": reused}


@router.post("/api/airs/basket/fundamentals/ingest/job",
             response_model=PortfolioFundamentalsJob)
async def ingest_basket_fundamentals_job(req: BasketRequest, force: bool = True,
                                         only_due: bool = True, feeds: str = "statements",
                                         limit: int = 0, prices: bool = False):
    """The same fill, for a basket of holdings rather than a stored model portfolio.

    ⚠⚠ IT EXISTS BECAUSE MOST BOOKS ON /management-dashboard HAVE NO FIXED MODEL. `openModal` in
    `PortfolioOverviewPanel` only carries a `fixed_portfolio_id` when the account is PAIRED with
    one; otherwise it resolves the account's own ISINs into a basket and opens the same Analyse
    view. Scoping the refresh to a model portfolio id therefore hid the button on exactly the rows
    a user is most likely to be looking at — an account is the unit of work, the model is the
    optional extra.

    Mirrors `POST /api/airs/basket/analysis`, which exists for the identical reason: one view
    serving a stored portfolio and an ad-hoc set alike.
    """
    import jobs as job_registry  # noqa: PLC0415

    from routers._fundamental_fill import fill_company_ids  # noqa: PLC0415

    name = req.label or "basket"
    isins = sorted({(h.isin or "").strip().upper() for h in (req.holdings or []) if h.isin})
    ids, holdings, no_fundamentals, no_company = _fundamentals_scope(isins)

    def _work(ctx) -> str:
        # ⚠ `prices` FORWARDED, and it was not. This route DECLARED the parameter and then dropped
        # it on the floor, so the Quick Valuation refresh silently skipped the price leg on every
        # un-paired book — which, per the ⚠⚠ above, is most books on /management-dashboard. The
        # model-portfolio twin had the opposite half of the same mistake: it passed `prices=prices`
        # without declaring it, so that path raised `NameError` instead. One feature, two routes,
        # neither working, and in opposite directions — a loud 500 on the rarer path and a silent
        # no-op on the common one.
        return fill_company_ids(ctx, name, ids, feeds=feeds, force=force, limit=limit,
                                only_due=only_due, prices=prices)

    job, reused = job_registry.start("fundamentals.basket", name, _work)
    return {"job_id": job.id, "label": name, "holdings": holdings, "reachable": len(ids),
            "no_fundamentals": no_fundamentals, "no_company": no_company,
            "already_running": reused}

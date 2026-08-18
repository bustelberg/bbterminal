"""ONE function that fully refreshes ONE portfolio. Every refresh button calls it.

⚠⚠ WHY THIS EXISTS: A PORTFOLIO IS TWO OBJECTS AND EACH BUTTON REFRESHED ONE OF THEM.

    AIRS keeps a portfolio as a pair — the FIXED model (weights, ISINs, an effective date; AIRS
    values none of it) and the DYNAMIC book (the real positions and the money; no ISINs). Two
    halves, two scrapes, two entirely separate refresh paths:

        book   `airs_vermogen.refresh_one_portfolio`      Rendement + Vermogensoverzicht, cascaded
        model  `_airs_portfolio_refresh.refresh_portfolio` composition -> instruments -> FX ->
                                                           prices -> recompute

    and which one a press ran depended on WHICH PAGE THE BUTTON WAS ON. /management-dashboard's
    row refreshed the book. /portfolios' row refreshed the model. The Analyse modal offers one
    "Refresh" and inherited whichever panel opened it, so the same-looking button on the same-
    looking modal did different work depending on where the reader came from.

    ⚠ AND NEITHER HALF'S DOCSTRING WAS WRONG, WHICH IS WHY IT SURVIVED. Each one described its own
    scope accurately; `refresh_portfolio`'s header even says in capitals that "REFRESH FROM AIRS"
    cannot fix a wrong return on its own. Nothing was misdocumented — there was simply no object
    called "a portfolio refresh" for either to be half of.

    The reported symptom was the row and the Analyse modal disagreeing, and the instinct is to go
    looking for a cache. There was no cache. There were two definitions of "refreshed".

⚠ THE AIRS LEGS ARE SERIALIZED AND THE REST IS NOT — see `airs_vermogen._acquire_session`.
    AirSPMS is ONE authenticated session that cannot be driven by two threads, so the report
    downloads and the composition read queue on a lock. Everything else in a full refresh — the
    OpenFIGI resolve, the ECB/Yahoo FX backfill, the price fetch per holding, the recompute — talks
    to other people entirely, and that is where the minutes are. Holding the lock across the whole
    function would have been simpler and would have made a concurrent fan-out pointless.

⚠ IT IS SAFE TO CALL CONCURRENTLY, WHICH IS THE POINT. `refresh_many` fans out over exactly this
    function; there is no separate bulk path to drift from the single one.

⚠ A PORTFOLIO MAY BE HALF A PAIR, AND THAT IS NOT AN ERROR. 18 of 51 accounts have no model row
    at all, and a model may exist with no account running it. Each half is refreshed if it exists
    and REPORTED AS ABSENT if it does not — never silently skipped, because "we refreshed it" over
    a half that was never attempted is the exact claim this module was built to stop.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

_log = logging.getLogger(__name__)

# How long a leg waits for the AirSPMS session before giving up. Generous, because the thing it
# waits for is another portfolio's report downloads (seconds each, a handful per account) and the
# alternative is abandoning this portfolio half-refreshed. Not unbounded: a wedged session must
# surface as a failed leg with a reason, not as a job that never ends.
SESSION_WAIT_SECONDS = 15 * 60

# ⚠ THE FAN-OUT WIDTH, AND IT IS NOT ABOUT CPU. Every worker spends most of its life waiting on
# somebody else's HTTP, and the one genuinely serial resource (the AIRS session) is already a lock,
# so this bounds how hard we lean on Yahoo — which answers an overloaded caller with an EMPTY list
# rather than a 429, the failure mode that once moved Alphabet onto a Vienna listing. Four is
# deliberately modest for that reason and not because more threads would not "work".
DEFAULT_CONCURRENCY = 4


def _pair(portefeuille: str | None, portfolio_id: int | None) -> tuple[str | None, int | None]:
    """The other half's handle, given either one.

    ⚠ THROUGH `list_account_links`, WHICH IS WHERE PAIRING ALREADY LIVES — a stored human decision
    when there is one, its name-stem guess otherwise. A second pairing rule here would be a second
    answer to "which model is this book running", and the two would disagree on exactly the books
    a human had to intervene on.
    """
    from routers._airs_account_links import list_account_links  # noqa: PLC0415

    accounts = list_account_links()["accounts"]
    if portefeuille:
        hit = next((a for a in accounts
                    if (a.get("portefeuille") or "").strip().lower() == portefeuille.strip().lower()),
                   None)
        return portefeuille, (hit or {}).get("model_portfolio_id")
    # ⚠ FIRST MATCH, AND IT IS ORDERED — `list_account_links` sorts by account name, so a model
    # claimed by two books resolves to the same one on every call rather than to whichever row the
    # database happened to return first. Two books running one model is a pairing mistake to fix in
    # the UI, not something to resolve differently on each press.
    hit = next((a for a in accounts if a.get("model_portfolio_id") == portfolio_id), None)
    return (hit or {}).get("portefeuille"), portfolio_id


def refresh_portfolio_fully(
    portefeuille: str | None = None,
    portfolio_id: int | None = None,
    *,
    cascade: bool = True,
    halves: tuple[str, ...] = ("book", "model"),
    on_step: Callable[[int, int, str], None] | None = None,
    on_event: Callable[..., None] | None = None,
    should_stop: Callable[[], bool] | None = None,
    wait: float | None = SESSION_WAIT_SECONDS,
) -> dict:
    """Both halves of one portfolio, brought current. Give it EITHER handle.

    Returns `{portefeuille, portfolio_id, book, model, book_status, model_status, status}` —
    each half's own full result under its own key, plus a one-word verdict per half so a caller
    does not have to know two result shapes to find out whether it worked.

    ⚠ THE MODEL HALF RUNS EVEN IF THE BOOK HALF FAILED, and vice versa. They read different
    sources and one being down says nothing about the other; stopping after a failed book scan
    would make a Yahoo outage look like an AIRS outage and leave the half that WAS available
    stale for no reason. The verdict carries both.

    ⚠ `should_stop` IS CHECKED BETWEEN HALVES, never inside one. Same rule the cascade follows: an
    account's reports are downloaded and stored as a unit, and so is a composition-and-reprice.

    ⚠⚠ `halves` IS A SCOPE, NOT A SWITCH, AND IT EXISTS FOR ONE CALLER: the 05:00 tick, which
    prices every model portfolio and must NOT scrape the accounts at that hour. Two ⚠⚠ notes on
    `airs_vermogen_refresh` record why — a forcing account scrape that runs before AIRS has valued
    the books stores YESTERDAY's valuation, and because it fires once nothing re-reads it until
    tomorrow, so the holdings read a full day behind while looking perfectly current. The MODEL
    half has no such hazard: a composition is a dated set of weights, and its other four steps talk
    to OpenFIGI, the ECB and Yahoo rather than to AIRS.

    ⚠ IT IS NOT A BOOLEAN. `book=False` at a call site says nothing about what it turns off; a
    tuple naming the halves reads as the scope it is. A half left out is `skipped`, never `absent`
    — "we chose not to" and "there was none" are different facts and the caller can tell them
    apart.

    ⚠ TWO HOOKS, BECAUSE THE TWO CALLERS ASK DIFFERENT QUESTIONS. A JOB wants a BAR — one
    `(done, total, message)` across both halves, which is why the denominator is computed here
    rather than left to each half (each owned the bar before, so a full refresh ran 0->100% twice
    and read as the job restarting). An SSE STREAM wants the FRAMES: `on_event` relays each half's
    own `(kind, **fields)` untouched, so the model's five `phase` frames still arrive as `phase`
    and the console still bolds them. Flattening everything onto `on_step` was tried first and
    silently cost exactly that distinction — a formatting loss with no error anywhere.
    """
    portefeuille, portfolio_id = _pair(portefeuille, portfolio_id)
    if not portefeuille and portfolio_id is None:
        return {"status": "error", "portefeuille": None, "portfolio_id": None,
                "book_status": "absent", "model_status": "absent",
                "message": "no such portfolio — neither an AIRS account nor a model portfolio"}

    label = portefeuille or f"portfolio {portfolio_id}"
    # ⚠ ONE DENOMINATOR ACROSS BOTH HALVES. Each half used to own the bar, so a full refresh went
    # 0->100% and then back to 0->100% — which reads as the job restarting, and is why the two
    # halves cannot simply be called one after the other and left to narrate themselves.
    book_steps = 1 if (portefeuille and "book" in halves) else 0
    model_steps = 5 if (portfolio_id is not None and "model" in halves) else 0
    total = book_steps + model_steps
    done = {"n": 0}

    def _say(msg: str, kind: str = "progress", **fields) -> None:
        if on_step:
            on_step(done["n"], total, msg)
        if on_event:
            on_event(kind, message=msg, **fields)

    out: dict = {"portefeuille": portefeuille, "portfolio_id": portfolio_id,
                 "book": None, "model": None,
                 "book_status": "absent", "model_status": "absent"}

    if portefeuille and "book" not in halves:
        out["book_status"] = "skipped"
    if portfolio_id is not None and "model" not in halves:
        out["model_status"] = "skipped"

    # ── HALF 1: the AIRS book (Rendement + Vermogensoverzicht, and the books behind it).
    if portefeuille and "book" in halves:
        if should_stop is not None and should_stop():
            out["status"] = "cancelled"
            out["cancelled_at"] = label
            return out
        _say(f"{label} — 1/2 the AIRS book", "phase", phase="book")
        from airs_vermogen import refresh_one_portfolio  # noqa: PLC0415

        try:
            book = refresh_one_portfolio(
                portefeuille, cascade,
                # ⚠ THE INNER BAR IS RELAYED, NOT ADOPTED. Its own (done, total) counts ACCOUNTS in
                # a cascade — up to nine — and pasting that onto this bar would make the whole job
                # read 3/9 when it is one of six steps. The message is the useful part.
                on_step=(lambda _d, _t, m: _say(m)) if (on_step or on_event) else None,
                should_stop=should_stop,
                wait=wait)
            out["book"] = book
            out["book_status"] = book.get("status") or "error"
        except Exception as e:  # noqa: BLE001 — one half must not lose the other
            _log.warning("[full-refresh] %s: book half failed — %s: %s",
                         label, type(e).__name__, e)
            out["book"] = {"status": "error", "errors": [f"{type(e).__name__}: {e}"]}
            out["book_status"] = "error"
        done["n"] += book_steps
        _say(f"{label} — book: {out['book_status']}")

    # ── HALF 2: the model portfolio (composition -> instruments -> FX -> prices -> recompute).
    if portfolio_id is not None and "model" in halves:
        if should_stop is not None and should_stop():
            # ⚠ NOT "cancelled" OUTRIGHT — the book half above is downloaded and STORED, and a word
            # that throws that away is the same mistake the cascade's own cancel path avoids.
            out["status"] = "cancelled"
            out["cancelled_at"] = f"model {portfolio_id}"
            out["model_status"] = "skipped"
            return out
        _say(f"{label} — 2/2 the model portfolio", "phase", phase="model")
        from routers._airs_portfolio_refresh import refresh_portfolio  # noqa: PLC0415

        def _emit(kind: str, **kw) -> None:
            # `refresh_portfolio` narrates five phases and every holding inside them. Its phase
            # lines already carry "n/5", so the bar advances on those and the rest is narration.
            if kind == "phase":
                done["n"] = min(total, done["n"] + 1)
            if on_step and kw.get("message"):
                on_step(done["n"], total, str(kw["message"]))
            # ⚠ FORWARDED WITH ITS OWN KIND AND ITS OWN FIELDS. This half's frames are what the
            # /portfolios console renders — `phase` in bold, `progress` plain — and re-emitting
            # them all as one kind loses that with nothing to show it went missing.
            if on_event:
                on_event(kind, **kw)

        try:
            out["model"] = refresh_portfolio(portfolio_id, _emit, wait)
            out["model_status"] = "ok"
        except Exception as e:  # noqa: BLE001 — see above
            _log.warning("[full-refresh] %s: model half failed — %s: %s",
                         label, type(e).__name__, e)
            out["model"] = {"error": f"{type(e).__name__}: {e}"}
            out["model_status"] = "error"

    done["n"] = total
    # ⚠ THE WORST HALF DECIDES, and "ok" requires that every half that EXISTS succeeded. A verdict
    # that reads `ok` because the half that ran was fine — while the other errored — is precisely
    # the "we refreshed it" this module exists to stop being said about half a portfolio.
    # ⚠ A SKIPPED HALF IS NOT A FAILED ONE, and neither is an absent one. `ok` still requires
    # every half that actually RAN to have succeeded — the point of the verdict is that "we
    # refreshed it" can never be said over a half that errored.
    states = [s for s in (out["book_status"], out["model_status"])
              if s not in ("absent", "skipped")]
    out["status"] = ("error" if "error" in states
                     else "busy" if "busy" in states
                     else "cancelled" if "cancelled" in states
                     else "ok" if states else "error")
    _say(f"{label} — {out['status']}")
    return out


def refresh_many(
    portefeuilles: list[str],
    *,
    cascade: bool = False,
    halves: tuple[str, ...] = ("book", "model"),
    concurrency: int = DEFAULT_CONCURRENCY,
    on_result: Callable[[str, dict], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> list[dict]:
    """N portfolios, fully refreshed, several at a time — `refresh_portfolio_fully` and nothing else.

    ⚠ THE ONLY THING THIS ADDS IS A THREAD POOL. There is no bulk implementation to drift from the
    single one, which is the mistake `scan_one`'s own docstring records ("Refresh-all is now
    literally refresh-one, N times") — made once already, on the layer below this.

    ⚠ `cascade=False` BY DEFAULT HERE AND TRUE FOR A SINGLE PRESS, AND THE ASYMMETRY IS THE POINT.
    The cascade exists so that refreshing ONE book also re-reads the books behind its certificates
    — which a sweep is going to reach on their own turn anyway. Left on, TOPS_BEOFF_BEH_DYN alone
    would pull nine accounts that are all in the list, at four downloads each.

    ⚠ A FAILING PORTFOLIO IS A RESULT, NOT AN EXCEPTION. One book that will not scan must not
    abandon the other forty-four; the outcome carries its own `status` and the caller counts them.
    """
    results: list[dict] = []
    if not portefeuilles:
        return results

    def _one(name: str) -> dict:
        if should_stop is not None and should_stop():
            return {"portefeuille": name, "status": "cancelled", "book_status": "skipped",
                    "model_status": "skipped"}
        try:
            return refresh_portfolio_fully(portefeuille=name, cascade=cascade, halves=halves,
                                           should_stop=should_stop)
        except Exception as e:  # noqa: BLE001 — see the docstring
            _log.warning("[full-refresh] %s threw — %s: %s", name, type(e).__name__, e)
            return {"portefeuille": name, "status": "error",
                    "message": f"{type(e).__name__}: {e}"}

    with ThreadPoolExecutor(max_workers=max(1, concurrency),
                            thread_name_prefix="airs-full-refresh") as pool:
        for res in pool.map(_one, portefeuilles):
            results.append(res)
            if on_result:
                on_result(res.get("portefeuille") or "?", res)
    return results

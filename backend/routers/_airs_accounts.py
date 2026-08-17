"""The AIRS ACCOUNTS — what the books actually made, on AIRS's own numbers.

WHY THIS EXISTS BESIDE THE MODEL VIEW, RATHER THAN REPLACING IT
    A model portfolio (`*_FX`) is a COMPOSITION — weights, no holdings — so AIRS has nothing to
    value and no Vermogensoverzicht exists for one. Measured: of 58 models with a composition and
    39 accounts with AIRS values, the overlap is ZERO. They answer different questions:

        the model    "would this strategy work?"      -> priced from yfinance; nothing else can
                                                          value a set of weights
        the account  "what did this book make?"       -> AIRS knows, authoritatively

    The gap between them is implementation drift, timing and fees, and it is worth seeing.

WHY AIRS AND NOT YFINANCE, HERE
    Measured 2026-07-16 on AMD: AIRS's implied prices agree with our yfinance series to +0.5% at
    the open and -0.8% at the close, and AIRS is THREE DAYS FRESHER (2026-07-16 vs 2026-07-13).
    But the real prize is coverage: `TOPS_OFF_BEH_DYN` holds Leonteq AMC certificates that Yahoo
    has no listing for — the zero-bar guard refuses them, correctly — and AIRS values 7 of 7 where
    the yfinance path prices 0 of 9. AIRS is the custodian's system; it does not need a listing.

⚠⚠ THE PORTFOLIO RETURN IS `cumulatief_rendement`, AND NEVER `eindvermogen / beginvermogen`.

        `rendement`  == eindvermogen/beginvermogen - 1   -- exact, in 38 of 38 accounts
        `cumulatief_rendement`                            -- AIRS's own, flow-aware

        AITopSelectie OFF DYN     ratio  -5.85%   actual  +46.12%   gap +51.97pp
        BUS_BM_AAN_ww_EUR_2026_d         +0.40%           +14.29%       +13.90pp
        BUS_FTS_BEPOFF_DYN               +2.43%            -5.08%        -7.51pp

    31 of 38 disagree by more than a point. Summing the holdings' values and dividing
    (`sum(current)/sum(start)`) is the SAME wrong number wearing different arithmetic — it was the
    obvious way to build this view, and it would have reported -5.85% on a book that made +46%.

    ⚠ THE ORIGINAL DIAGNOSIS ABOVE WAS WRONG, THOUGH THE RULE IT PRODUCED IS RIGHT (2026-07-17).
    The gap was read here as flows — "the value ratio is a return only when nothing was
    deposited or withdrawn". Measured against a real download: AITopSelectie OFF DYN has
    `stortingen` = 0 and `onttrekkingen` = 0 for every month of 2026, and its two figures still
    differ by 50pp. Flows were not the cause and could not have been.

    The cause is that ONE ATT ROW IS ONE MONTH. `rendement` -5.85% was never a botched YTD; it
    was that month's return, which is a fact about a different window. Both are AIRS's own and
    both are correct — of different periods. See `_year_perf`, which is where the year is now
    assembled, and which the whole of this module's money now flows through.

⚠ THE HOLDINGS DO NOT SUM TO THE PORTFOLIO RETURN, AND THAT IS CORRECT.
    They are different quantities, not a reconciliation that failed:
      - each holding's figure is a PRICE return (AIRS restates `Beginwaarde lopend jaar` to the
        current quantity — measured on 32 of 36 quantity changes; the 4 that do not are KLA's
        10:1 split, where the VALUE correctly does not move);
      - the portfolio's figure is flow-aware AND includes `opbrengsten` (income), which no price
        return contains.
    The /portfolios MODEL view has the opposite property — its holdings weight exactly to its
    total — so a reader arriving from there will expect these to tie. They must be told.
"""
from __future__ import annotations

import asyncio
from datetime import date

# ⚠ IMPORTED, NOT RE-LISTED. The set of reports an account needs is the set the refresh fetches;
# two copies would drift the moment a fifth report is added, and the drift would show up as
# accounts silently missing from the page.
from airs_vermogen import REPORTS
from common.pg import load_rows_via_copy
from deps import supabase
from routers._airs_ref import model_weights_for as ref_model_weights_for, mutaties_for as ref_mutaties_for

_SNAP_COLS = ("as_of_date,holding_name,quantity,currency,weight,start_value_eur,"
              "current_value_eur,ytd_return_eur,ytd_return_pct,ytd_return_local_pct,"
              "cost_basis_local,current_price_local,airs_weight,fund_result_eur,"
              "fx_result_eur,airs_result_pct")

# AIRS reports a portfolio's own return; we never recompute it. These are the columns it uses.
_PERF_COLS = ("portefeuille,periode,beginvermogen,stortingen,onttrekkingen,koersresultaat,"
              "opbrengsten,kosten,mutatie_opgelopen_rente,"
              "beleggingsresultaat,eindvermogen,rendement,cumulatief_rendement")


# The money columns are PER PERIOD and therefore summed across the year; everything else is
# read off one end of the chain. Kept as a list so the aggregation cannot drift from the
# select above.
_PER_PERIOD_SUMS = ("stortingen", "onttrekkingen", "koersresultaat", "opbrengsten",
                    "kosten", "mutatie_opgelopen_rente", "beleggingsresultaat")


def _paged(build, *, page: int = 1000) -> list[dict]:
    """Every row the query matches — not the first serverful.

    ⚠⚠ `.limit(20000)` IS NOT PROTECTION, AND READING IT AS PROTECTION IS HOW THIS BIT US. The
    bound that applies is the SERVER'S `db-max-rows`: **1,000 on Supabase cloud, 10,000 locally**.
    PostgREST truncates to it and says nothing — no error, no header, no short-read signal.

    Measured 2026-08-03, and it is the whole reason this helper exists: `_year_perf` read
    `airs_performance` with `.order("periode").limit(20000)`. The table holds 1,334 rows, so
    locally it all came back and every figure was right. In production the read stopped at 1,000
    — and because the order is ASCENDING, the rows it dropped were the NEWEST ones. The portfolios
    page then showed `AITopSelectie OFF DYN` at **+55.20%**, which is June's `cumulatief_rendement`,
    while July's −11.96% sat in the table unread. Local: +36.64%. Same code, same query, two
    answers, and nothing anywhere said the read was short.

    ⚠ AND REFRESHING MADE IT WORSE. `airs_performance` is append-only — every daily run writes
    another row for each month in progress — so each refresh pushed the newest rows further past
    the cap. The one action that looks like a fix was feeding the bug.

    Advances by what CAME BACK and stops on an empty page, so it is correct under any cap (a
    short page is only "the last page" while the server's limit is at least `page`). The caller
    must order on a key unique enough that a page boundary landing inside a tie cannot serve a
    row twice or skip it.

    `build()` returns a FRESH query each call — postgrest builders are not re-executable.
    """
    out: list[dict] = []
    off = 0
    while True:
        rows = build().range(off, off + page - 1).execute().data or []
        if not rows:
            break
        out += rows
        off += len(rows)
    return out


def _year_perf() -> dict[str, dict]:
    """Each account's YEAR, aggregated from AIRS's own monthly rows.

    ⚠⚠ ONE ATT ROW IS ONE MONTH, NOT ONE PORTFOLIO — and reading the freshest row as
    "the year" is the bug this function exists to prevent. Measured on AITopSelectie
    OFF DYN, whose sheet has seven rows:

        periode      begin        eind      rendement  cumulatief
        2026-01-31   1,000,000    1,044,066   +4.41%     +4.41%
        ...
        2026-07-16   1,551,994    1,422,088   -8.37%    +42.21%

    `beginvermogen` is THAT MONTH's opening (July's 1,551,994 — not the year's
    1,000,000); `rendement` is THAT MONTH's return; `cumulatief_rendement` is the year.
    Both verified exactly: eind/begin-1 == rendement, and compounding all seven
    rendement == cumulatief (42.2088). So the freshest row served July's price result
    of **-130,063** as the year's, beside a +42.21% YTD — a number of the wrong sign,
    three times too small, on a screen claiming to describe the same period. The year's
    price result is **+420,225**.

    ⚠ THE ROWS ARE NOT ALL DISTINCT PERIODS, SO THEY CANNOT SIMPLY BE SUMMED. The daily
    refresh re-downloads Jan-1..today, and the sheet's final row is a PARTIAL month, so
    every run writes another row for the month in progress. BUS_Offensief_Dyn holds 20
    rows for 7 months: seven of them are June (all with `beginvermogen` 1,211,625.02 —
    May's close) and eight are July (all 1,252,235.80). Summing the lot counts June
    seven times. A period is identified by its opening capital; the freshest row per
    MONTH is that month's answer, and the rest are earlier looks at it.

    Returns one dict per account: the money columns summed over those monthly rows, the
    year's opening from the FIRST, and `cumulatief_rendement` from the LAST (never
    recomputed — it is AIRS's own and it is flow-aware).
    """
    # ⚠ PAGED, NOT `.limit(20000)` — see `_paged`. This exact read served June's YTD in
    # production while July sat unread, because the cap that binds is the server's (1,000) and
    # ascending order puts the newest rows last. The sort key is `(periode, portefeuille,
    # fetched_at)`: `periode` alone ties across all ~44 accounts, and a page boundary inside a
    # tie can serve a row twice or never.
    rows = _paged(lambda: supabase.table("airs_performance").select(_PERF_COLS)
                  .order("periode").order("portefeuille").order("fetched_at"))
    by_acct: dict[str, list[dict]] = {}
    for r in rows:
        by_acct.setdefault(r["portefeuille"], []).append(r)

    out: dict[str, dict] = {}
    for name, rs in by_acct.items():
        # One year only: mixing years would sum across a `cumulatief_rendement` that
        # restarts each January. The report window is Jan-1..today, so this is the year
        # in hand rather than a filter on today's date (which a stale table would fail).
        year = max(str(r["periode"])[:4] for r in rs)
        rs = [r for r in rs if str(r["periode"]).startswith(year)]
        # Freshest row per month — see the double-count note above.
        per_month: dict[str, dict] = {}
        for r in sorted(rs, key=lambda r: str(r["periode"])):
            per_month[str(r["periode"])[:7]] = r          # later periode overwrites earlier
        months = [per_month[k] for k in sorted(per_month)]
        if not months:
            continue
        first, last = months[0], months[-1]
        agg = {k: sum((r.get(k) or 0) for r in months) for k in _PER_PERIOD_SUMS}
        agg.update({
            "portefeuille": name,
            "periode": last["periode"],
            "months": len(months),
            "beginvermogen": first.get("beginvermogen"),   # the YEAR's opening
            "eindvermogen": last.get("eindvermogen"),
            "cumulatief_rendement": last.get("cumulatief_rendement"),
            "rendement_latest_month": last.get("rendement"),
        })
        # AIRS's own consistency check, asserted rather than assumed:
        #     eind - begin - stortingen + onttrekkingen == sum(beleggingsresultaat)
        # Measured residual -0.00 on AITopSelectie (422,087.64). A month we failed to
        # store breaks it, and a silently short year is exactly the failure that looks
        # like a number. Surfaced, never corrected into place.
        begin, end = agg["beginvermogen"], agg["eindvermogen"]
        if begin is not None and end is not None:
            implied = end - begin - agg["stortingen"] + agg["onttrekkingen"]
            agg["residual_eur"] = round(implied - agg["beleggingsresultaat"], 2)
            agg["reconciles"] = abs(agg["residual_eur"]) < 1.0
        else:
            agg["residual_eur"], agg["reconciles"] = None, None
        out[name] = agg
    return out


def _direct_result(portefeuille: str, holding_names: set[str]):
    """This account's dividend income, per instrument, from the stored Mutaties journal.

    Returns `(attached_by_holding_name, sold_totals)` — the second being income from positions no
    longer held, already rolled up.

    ⚠ THE ROLL-UP HAPPENS HERE, NOT IN `account_holdings`, AND THAT IS DELIBERATE. That function
    is guarded by a test forbidding the token `sum(` in its source, because summing the holdings
    into a portfolio RETURN is the headline bug this module exists to prevent (see the module
    docstring: it would have reported -5.85% on a book that made +46%). Adding euros of income is
    a different and legitimate act, but the guard cannot tell them apart from source text — and a
    crude guard on a real trap is worth more than a precise one nobody trusts, so the arithmetic
    moves rather than the rule.

    ⚠ AGGREGATED ON READ, NEVER STORED AS A TOTAL. The journal lines are the source; a stored
    per-holding sum is a second source of truth that drifts from the rows it counts, and it could
    not answer "which payments, on what dates" — the first thing anyone asks when a dividend figure
    looks wrong.

    ⚠ JOINED BY NAME, EXACTLY. This sheet carries no ISIN. Both `fonds` and `holding_name` are AIRS
    strings truncated at the same 50 characters, so an exact match is sound — and nothing fuzzy
    belongs here (see `_airs_holding_isin` for the price of fuzzy matching this join).
    """
    from airs_mutaties import Mutatie, attach, direct_result  # noqa: PLC0415

    # Paged for the same reason as `_year_perf` — `.limit(5000)` never overrode the server's
    # 1,000. One account's mutations are under that today; the table only grows.
    rows = ref_mutaties_for(portefeuille)   # one shared read — see `_airs_ref`
    empty: dict = {"gross": None, "tax": None, "funds": None}
    if not rows:
        return {}, empty
    muts = [Mutatie(
        grootboek=r["grootboek"], fonds=r["fonds"], omschrijving=r.get("omschrijving") or "",
        boekdatum=date.fromisoformat(str(r["boekdatum"])) if r.get("boekdatum") else None,
        amount_eur=float(r["amount_eur"]),
    ) for r in rows]
    attached, orphans = attach(direct_result(muts), holding_names)
    if not orphans:
        return attached, empty
    return attached, {
        "gross": round(sum(d.gross_eur for d in orphans), 2),
        "tax": round(sum(d.tax_eur for d in orphans), 2),
        "funds": [d.fonds for d in orphans],
    }


def _model_weights(portefeuille: str) -> dict[str, dict]:
    """This book's OWN model weights, keyed by holding name (`airs_model_weight`).

    ⚠ NO PAIRING. These come from the dynamic portfolio's own MODEL report, so there is no
    fixed portfolio to match it to and no guess to get wrong — which was the failure mode with the
    worst blast radius here, since the risk variants of a strategy hold the same instruments and a
    mis-pairing therefore looks entirely normal on every other column.

    The cash-line rename is already applied at parse time (`airs_model.NAME_ALIASES`), so this is
    a straight dictionary lookup on the holding's own name.
    """
    return ref_model_weights_for(portefeuille)   # one shared read — see `_airs_ref`


def parse_holding_counts_csv(raw: str) -> tuple[dict[str, int], dict[str, str]]:
    """`portefeuille,as_of_date,count` CSV -> (counts, newest dates). Pure, so the parsing is
    testable without a database.

    ⚠ PARSED AS CSV, NEVER `line.split(",")`. AIRS portfolio names are free text with spaces
    ("WTS test 1 FX", "VTopSelectie OFF DY") and nothing stops one containing a comma — at which
    point a naive split shifts every field on that row and the account silently gets the wrong
    count. Postgres quotes such a field; `csv.reader` unquotes it.
    """
    import csv  # noqa: PLC0415
    import io as _io  # noqa: PLC0415

    counts: dict[str, int] = {}
    newest: dict[str, str] = {}
    isins: dict[str, int] = {}
    for row in csv.reader(_io.StringIO(raw)):
        if len(row) != 4:
            continue
        name, day, n, n_isin = row
        newest[name] = day
        counts[name] = int(n)
        isins[name] = int(n_isin)
    return counts, newest, isins


def _holding_counts() -> tuple[dict[str, int], dict[str, str], dict[str, int]]:
    """(holdings per account, that account's snapshot date) — off the freshest snapshot only.

    ⚠ AGGREGATED IN POSTGRES, BECAUSE READING THE TABLE DOES NOT SCALE AND FAILS SILENTLY. This
    used to `select(...).limit(20000)` over ALL of `airs_holding` and reduce it in Python.
    `airs_holding` keeps one snapshot per account PER DATE and grows on every scan — measured
    2026-07-30 it was already at 9,817 rows across 18 snapshot dates for 39 accounts. The moment it
    crosses 20,000, PostgREST returns the first page and says nothing: accounts whose newest rows
    fall outside it get a count of 0 or a stale date, which reads on the page as a book that holds
    nothing rather than as a truncated read. It is the same silent-cap failure the price loaders
    already carry warnings about, on a table nobody was watching grow.

    One row per account comes back instead of one per holding — ~44 rows rather than ~10,000 — and
    the answer no longer depends on how much history has accumulated.
    """
    from common.pg import _run_copy  # noqa: PLC0415

    buf = _run_copy(
        "COPY (SELECT DISTINCT ON (portefeuille) portefeuille, as_of_date::text, cnt, isins "
        "FROM (SELECT portefeuille, as_of_date, count(*) AS cnt, "
        "             count(DISTINCT isin) FILTER "
        "               (WHERE isin IS NOT NULL AND btrim(isin) <> '') AS isins "
        "        FROM airs_holding GROUP BY portefeuille, as_of_date) g "
        "ORDER BY portefeuille, as_of_date DESC) TO STDOUT WITH CSV",
        (),
    )
    if buf is not None:
        return parse_holding_counts_csv(buf.getvalue().decode())
    return _holding_counts_paged()


def _holding_counts_paged() -> tuple[dict[str, int], dict[str, str], dict[str, int]]:
    """The COPY-less fallback: the same reduction, but PAGED rather than capped.

    ⚠ `.range()` IN A LOOP, NOT A BIGGER `.limit()`. Raising the cap only moves the cliff; paging
    until a short page arrives has no cliff at all. Slower and correct beats fast and quietly wrong
    — this runs wherever `SUPABASE_DB_URL` is unset.
    """
    rows: list[dict] = []
    off = 0
    while True:
        page = (supabase.table("airs_holding").select("portefeuille,as_of_date,isin")
                .range(off, off + 999).execute().data or [])
        rows += page
        if len(page) < 1000:
            break
        off += 1000

    newest: dict[str, str] = {}
    for r in rows:
        d = str(r["as_of_date"])
        if d > newest.get(r["portefeuille"], ""):
            newest[r["portefeuille"]] = d
    counts: dict[str, int] = {}
    seen_isins: dict[str, set[str]] = {}
    for r in rows:
        if str(r["as_of_date"]) == newest.get(r["portefeuille"]):
            counts[r["portefeuille"]] = counts.get(r["portefeuille"], 0) + 1
            iv = (r.get("isin") or "").strip()
            if iv:
                seen_isins.setdefault(r["portefeuille"], set()).add(iv)
    return counts, newest, {k: len(v) for k, v in seen_isins.items()}



def _hidden_accounts() -> set[str]:
    """Accounts a human has removed from the list (`airs_account_hidden`), lower-cased.

    ⚠ AN EMPTY SET ON FAILURE, NEVER AN EXCEPTION. This gates a read of the whole portfolios
    page; a missing table or a transient error must show one row too many, not zero rows.
    """
    try:
        rows = (supabase.table("airs_account_hidden").select("portefeuille")
                .limit(500).execute().data or [])
    except Exception:  # noqa: BLE001 — see above
        return set()
    return {(r["portefeuille"] or "").strip().lower() for r in rows if r.get("portefeuille")}



def _live_accounts() -> set[str] | None:
    """The accounts AIRS listed on the most recent discovery, lower-cased — or None.

    ⚠ `None` MEANS "DO NOT FILTER", AND IS NOT THE SAME AS AN EMPTY SET. Before the first
    discovery has run (a fresh database, or right after this table was added) the roster is empty.
    Treating that as "no account exists" would blank the entire portfolios page; treating it as
    "we do not know yet" shows what we have, which is the honest state. An empty set would mean
    AIRS genuinely returned nothing, and `_record_roster` refuses to write that.
    """
    try:
        rows = (supabase.table("airs_account_roster").select("portefeuille,last_seen_at")
                .order("last_seen_at", desc=True).limit(2000).execute().data or [])
    except Exception:  # noqa: BLE001 — a missing table must not blank the page
        return None
    if not rows:
        return None
    newest = rows[0]["last_seen_at"]
    return {(r["portefeuille"] or "").strip().lower()
            for r in rows if r.get("last_seen_at") == newest and r.get("portefeuille")}


def _fetched_at() -> dict[str, str]:
    """Per account (lower-cased), when WE last successfully scanned it — `reports_at`.

    ⚠⚠ IT IS A DIFFERENT FACT FROM `as_of`, AND THE ROW WAS SHOWING ONLY ONE OF THEM. `as_of` is
    the date AIRS VALUED the book; this is the moment we last READ it. The freshness badge was
    computed from `as_of` alone and told the reader to press Refresh — which cannot help when the
    gap is AIRS's rather than ours.

    Measured 2026-08-17, immediately after a full "Refresh all": 31 accounts were re-scanned, and
    the newest valuation AIRS returned for ANY of them was 2026-08-15. Twenty came back dated
    2026-08-11 or 2026-08-12 — three to four trading days old — from a scan that had just run
    successfully. 32 of 40 rows wore the amber warning, and not one of them could be cleared by the
    action the warning named.

    With both dates the row can say which side is behind: a recent `reports_at` with an old `as_of`
    is AIRS's valuation batch (nothing to do), an old `reports_at` is ours (refresh).
    """
    try:
        rows = (supabase.table("airs_account_roster").select("portefeuille,reports_at")
                .order("reports_at", desc=True).limit(2000).execute().data or [])
    except Exception:  # noqa: BLE001 — a missing column must not blank the page
        return {}
    return {(r["portefeuille"] or "").strip().lower(): r["reports_at"]
            for r in rows if r.get("portefeuille") and r.get("reports_at")}


def _missing_reports() -> dict[str, list[str]]:
    """Per account (lower-cased), which of the four reports the last refresh did NOT retrieve.

    ⚠ THIS USED TO BE A FILTER AND IT WAS THE WRONG SHAPE. Accounts missing a report were withheld
    from the list entirely, so a scan that reached all 44 portfolios displayed 22 — the work was
    done and invisible, and the operator had no way to see WHICH report was short or for whom.
    Marking a row costs nothing and keeps the finding; hiding it threw the finding away along with
    the row.

    An account with nothing missing simply has no entry. An account nobody has measured has no
    entry either — absence of evidence is not evidence of a gap, same rule as before.
    """
    try:
        rows = (supabase.table("airs_account_roster").select("portefeuille,reports_ok,reports_at")
                .order("reports_at", desc=True).limit(2000).execute().data or [])
    except Exception:  # noqa: BLE001 — a missing column must not blank the page
        return {}
    dated = [r for r in rows if r.get("reports_at")]
    if not dated:
        return {}
    newest = dated[0]["reports_at"]
    out: dict[str, list[str]] = {}
    for r in dated:
        if r.get("reports_at") != newest or not r.get("portefeuille"):
            continue
        got = set(r.get("reports_ok") or [])
        gap = [code for code in REPORTS if code not in got]
        if gap:
            out[(r["portefeuille"] or "").strip().lower()] = gap
    return out


def _complete_accounts() -> set[str] | None:
    """Accounts whose last refresh retrieved ALL FOUR reports, lower-cased — or None.

    ⚠ NO LONGER USED AS A LIST FILTER — see `_missing_reports`. Kept because it is the honest
    expression of "is this account whole", which the tests pin and which any future caller
    (an alert, a health check) should reuse rather than re-derive.

    ⚠ AN ACCOUNT MISSING ONE REPORT IS NOT A SLIGHTLY-WORSE ROW, IT IS A MIXTURE OF DATES.
    Measured 2026-07-29: Rendement 44/44 but Vermogensoverzicht 31/44, so thirteen accounts
    rendered this week's return beside last week's holdings, with nothing on screen saying so.
    Every figure was real; only their combination was fiction.

    ⚠ `None` MEANS "DO NOT FILTER" — same contract as `_live_accounts`, same reason. Before any
    refresh has recorded an outcome (a fresh database, or the deploy that added the columns) every
    account looks incomplete, and asserting that would blank the page over a measurement never
    taken. An empty SET is different: it means a refresh ran and nothing came back whole.

    ⚠ AND ONLY THE NEWEST VERDICT COUNTS. Rows carry the batch stamp their refresh wrote, so an
    account skipped by a later run keeps an older `reports_at` and is correctly not counted as
    complete-as-of-now — rather than coasting on a verdict from a week ago.
    """
    try:
        rows = (supabase.table("airs_account_roster").select("portefeuille,reports_ok,reports_at")
                .order("reports_at", desc=True).limit(2000).execute().data or [])
    except Exception:  # noqa: BLE001 — a missing column must not blank the page
        return None
    dated = [r for r in rows if r.get("reports_at")]
    if not dated:
        return None
    newest = dated[0]["reports_at"]
    need = set(REPORTS)
    return {(r["portefeuille"] or "").strip().lower()
            for r in dated
            if r.get("reports_at") == newest and r.get("portefeuille")
            and need.issubset(set(r.get("reports_ok") or []))}


def list_accounts() -> list[dict]:
    """Every AIRS account with a reported return, freshest first.

    Every money figure here is the YEAR's — summed across AIRS's monthly rows by
    `_year_perf`, never the freshest row's (which is one month; see that docstring).
    """
    perf = _year_perf()
    counts, newest, isin_counts = _holding_counts()
    hidden = _hidden_accounts()
    live = _live_accounts()
    missing = _missing_reports()
    fetched = _fetched_at()
    out: list[dict] = []
    for name, r in perf.items():
        # ⚠ Filtered HERE, at the one place the list is built, so every surface that reads it
        # agrees. Hiding in the UI instead would leave the account in the API, in the
        # account-model link picker and in anything else that enumerates accounts.
        key = (name or "").strip().lower()
        if key in hidden:
            continue
        # ⚠ AND THE SCRAPE DECIDES WHAT EXISTS. `perf` comes from `airs_performance`, which is
        # append-only, so an account AIRS deactivated stays here for ever with a frozen snapshot
        # (measured: 44 live, 50 listed — TOPS_AZTS_L, TOPS_MOTS_L and WTS test 1-4). The
        # performance table answers "what did it make", which stays true after the book is gone;
        # only the discovery pass answers "does AIRS still list it".
        if live is not None and key not in live:
            continue
        begin, end = r.get("beginvermogen"), r.get("eindvermogen")
        out.append({
            "portefeuille": name,
            # ⚠ MARKED, NOT HIDDEN. A row assembled from a fresh Rendement and a week-old
            # Vermogensoverzicht mixes dates, and the reader has to be told — but withholding the
            # row told them nothing at all and threw away the scan's work. Empty list = whole;
            # absent from `missing` also = whole (or never measured), which is the same display.
            "missing_reports": missing.get(key, []),
            # When WE last read this account — see `_fetched_at`. Paired with `as_of` it is what
            # lets the row say whether the gap is ours or AIRS's.
            "fetched_at": fetched.get(key),
            "periode": str(r["periode"]) if r.get("periode") else None,
            "as_of": newest.get(name),
            "begin_value_eur": begin,
            "end_value_eur": end,
            # ⚠ AIRS'S OWN YEAR RETURN — the compounding of every month's `rendement`, and
            # flow-aware. Never `end/begin - 1`.
            "ytd_pct": r.get("cumulatief_rendement"),
            # ⚠ THE FRESHEST ROW'S `rendement` IS THE LATEST MONTH'S RETURN — NOT a rival YTD.
            # It was served as `value_ratio_pct` ("the naive value ratio... the wrong one"),
            # which mis-stated what it is: -8.37% is not a wrong answer for the year, it is the
            # right answer for July. Named for the window it actually measures.
            "latest_month_pct": r.get("rendement_latest_month"),
            "months": r.get("months"),
            # AIRS's own identity, asserted in `_year_perf` and carried so a short year shows
            # up as a discrepancy rather than as a confident total.
            "residual_eur": r.get("residual_eur"),
            "reconciles": r.get("reconciles"),
            # AIRS's own split of the result: price vs income. `koersresultaat` is the "price
            # gains" a reader is usually after; `opbrengsten` is dividends and coupons, which no
            # price return contains.
            "price_result_eur": r.get("koersresultaat"),
            "income_eur": r.get("opbrengsten"),
            # `beleggingsresultaat` — the investment result. NOT price+income: AIRS also
            # subtracts `kosten` and adds `mutatie_opgelopen_rente`, so the three-column
            # sum a reader tries in their head only ties where both are 0 (which is most,
            # but not all, portfolios). Both terms ride along so the gap is answerable.
            "investment_result_eur": r.get("beleggingsresultaat"),
            "costs_eur": r.get("kosten"),
            "accrued_interest_change_eur": r.get("mutatie_opgelopen_rente"),
            # ⚠ THE FLOWS. This is why `ytd_pct` and `value_ratio_pct` differ; a reader who
            # sees a 52pp gap between two returns on one row deserves the cause on it too.
            "deposits_eur": r.get("stortingen"),
            "withdrawals_eur": r.get("onttrekkingen"),
            "holdings": counts.get(name),          # None = we hold no snapshot for it
            # ⚠ THE BOOK'S OWN ISINs, NOT THE PAIRED MODEL'S POSITION COUNT. The column used to
            # read the model — so a book with no model showed "—" beside 22 holdings you could see
            # the moment you expanded it, and the number it DID show for a paired book described a
            # different object. The Vermogensoverzicht has carried `ISIN-code` since 2026-07-23;
            # distinct non-null ISINs on the newest snapshot is what a reader means by "ISINs".
            "isins": isin_counts.get(name),
        })
    out.sort(key=lambda x: (x["portefeuille"] or "").lower())
    return out


def account_holdings(portefeuille: str) -> dict:
    """One account's freshest snapshot: every position, with AIRS's own EUR values.
    (helper below is used by this function; see `_direct_result`.)

    Each `ytd_pct` is a PRICE return — `Beginwaarde lopend jaar` is restated to the current
    quantity, so it is not contaminated by a purchase. It will NOT sum to the account's return:
    that one is flow-aware and includes income. See the module docstring.
    """
    # ⚠ THE NEWEST SNAPSHOT IS ASKED FOR, NOT FILTERED OUT OF EVERY SNAPSHOT. This read all of an
    # account's history under `.limit(2000)` and took `max(as_of_date)` from whatever came back —
    # but `airs_holding` keeps one snapshot per account PER DATE and grows on every scan (10,084
    # rows across all accounts, 704 for the busiest, and only climbing). The server caps a
    # response at 1,000 in production and truncates SILENTLY, and this query had no `order` at
    # all, so the rows it kept were arbitrary: `max()` over them would name an OLD snapshot and
    # the panel would show last month's positions as today's, with nothing saying so. Same family
    # as `_year_perf`'s June-instead-of-July (see `_paged`).
    #
    # Two reads instead: the newest date, then that date's rows. ~40 rows rather than 704, and
    # the answer stops depending on how much history has accumulated.
    newest = (supabase.table("airs_holding").select("as_of_date")
              .eq("portefeuille", portefeuille)
              .order("as_of_date", desc=True).limit(1).execute().data or [])
    if not newest:
        return {"portefeuille": portefeuille, "as_of": None, "rows": []}
    as_of = str(newest[0]["as_of_date"])
    # ⚠ COPY REMOVES THE PAGING *AND* ITS EMPTY PROBE PAGE. The pager below is correct and must
    # stay as the fallback, but it costs `ceil(rows/1000) + 1` round trips — the +1 being the
    # empty page that proves the previous one was the last (measured: a 24-row snapshot issued a
    # second request at `offset=24` purely to come back empty). A COPY has no row cap, so one
    # query returns the snapshot and there is nothing to prove.
    # ⚠ `as_of_date` STAYS A SERVER-SIDE FILTER. `airs_holding` keeps 28 historical snapshots per
    # book, so fetching the book and picking the date in Python reads **788 rows instead of 42**.
    snap = load_rows_via_copy("airs_holding", _SNAP_COLS, "portefeuille", [portefeuille],
                              where={"as_of_date": as_of})
    if snap is None:
        snap = _paged(lambda: supabase.table("airs_holding").select(_SNAP_COLS)
                      .eq("portefeuille", portefeuille).eq("as_of_date", as_of)
                      .order("holding_name"))
    if not snap:
        return {"portefeuille": portefeuille, "as_of": None, "rows": []}
    snap.sort(key=lambda r: -(r.get("current_value_eur") or 0))
    income, sold_income = _direct_result(portefeuille, {r["holding_name"] for r in snap})
    model = _model_weights(portefeuille)
    # The YEAR's, to match the holdings beneath it: each holding's figure runs from
    # `Beginwaarde lopend jaar`, so pairing them with July's price result would set a
    # year of holdings against a month of portfolio.
    perf = _year_perf().get(portefeuille) or {}
    return {
        "portefeuille": portefeuille,
        "as_of": as_of,
        # Repeated here so the panel can state, on the same screen as the positions, that these
        # do not add up to it — and why.
        "ytd_pct": perf.get("cumulatief_rendement"),
        "price_result_eur": perf.get("koersresultaat"),
        "income_eur": perf.get("opbrengsten"),
        # ⚠ Income the holdings table CANNOT show. A position sold during the year paid real
        # dividends and has no row left to carry them — measured, 3 of 27 funds and EUR 1,010 of
        # BUS_Neutraal_Dyn's EUR 12,031. Summing the Direct result column and calling it the
        # book's income would understate it with nothing on screen to say why.
        "dividend_sold_eur": sold_income["gross"],
        "dividend_sold_tax_eur": sold_income["tax"],
        "dividend_sold_funds": sold_income["funds"],
        "rows": [{
            "holding_name": r["holding_name"],
            "quantity": r.get("quantity"),
            "currency": r.get("currency"),
            "weight": r.get("weight"),
            "start_value_eur": r.get("start_value_eur"),
            "current_value_eur": r.get("current_value_eur"),
            "ytd_return_eur": r.get("ytd_return_eur"),
            # ⚠ None where `Beginwaarde` is 0 — a position not held at the year's open (or a cash
            # line). Its YTD is UNDEFINED, not 0%: dividing by zero would be infinite and calling
            # it flat would be a claim. `parse_airs_excel` already refuses it; this preserves the
            # refusal rather than coalescing it to a number.
            "ytd_return_pct": r.get("ytd_return_pct"),
            "ytd_return_local_pct": r.get("ytd_return_local_pct"),
            # AIRS's OWN figures, passed through as reported. `airs_result_pct` is its
            # `Resultaat in %` and is NOT in the same unit as `ytd_return_pct` above (a
            # fraction) — a consumer that renders them in one column will be 100× out on
            # one of them. `fund_result`/`fx_result` split the result into performance and
            # FX, which is the one thing here we cannot derive ourselves.
            "cost_basis_local": r.get("cost_basis_local"),
            "current_price_local": r.get("current_price_local"),
            "airs_weight": r.get("airs_weight"),
            "fund_result_eur": r.get("fund_result_eur"),
            "fx_result_eur": r.get("fx_result_eur"),
            "airs_result_pct": r.get("airs_result_pct"),
            # The DIRECT result: what this instrument actually paid the book, from the Mutaties
            # journal. ⚠ `dividend_eur` is GROSS and `dividend_tax_eur` is NEGATIVE (as AIRS books
            # it), so net is their sum — they are two columns because a US name losing 15% and a
            # Dutch one losing nothing is a fact about the holding, not rounding.
            # ⚠ None, never 0.0, when the journal has no line for it: "paid nothing" and "we have
            # not scanned this book's journal" are different claims and only one is safe to make.
            "dividend_eur": (d.gross_eur if (d := income.get(r["holding_name"])) else None),
            "dividend_tax_eur": (d.tax_eur if (d := income.get(r["holding_name"])) else None),
            "dividend_payments": (d.payments if (d := income.get(r["holding_name"])) else None),
            # The book own model weight, from ITS OWN MODEL report (no fixed-portfolio pairing).
            # None = the model does not name this holding, which is drift worth seeing, not a 0%.
            "model_pct": (m.get("model_pct") if (m := model.get(r["holding_name"])) else None),
            "model_drift_pct": (m.get("drift_pct") if (m := model.get(r["holding_name"])) else None),
            "model_actual_pct": (m.get("actual_pct") if (m := model.get(r["holding_name"])) else None),
        } for r in snap],
    }


async def list_accounts_async() -> list[dict]:
    return await asyncio.to_thread(list_accounts)


async def account_holdings_async(portefeuille: str) -> dict:
    return await asyncio.to_thread(account_holdings, portefeuille)

"""Every data-quality question worth asking about ONE AIRS book, in one pass.

⚠⚠ IT ASKS THE QUESTIONS THIS CODEBASE HAS ALREADY BEEN BITTEN BY, not a generic null-check. Each
section below corresponds to an incident recorded in CLAUDE.md — a holding that silently left its
portfolio because an FX row was missing, a price series frozen because the vendor does not cover
the exchange, a GBp quote read as pounds. A holding can be wrong in all of these ways while every
figure on screen looks entirely ordinary, which is what makes a sweep worth more than a glance.

⚠ READ-ONLY. It reports; it changes nothing.

Usage (from backend/):
    uv run python scripts/audit_book_data_quality.py BUS_Offensief_Dyn
    uv run python scripts/audit_book_data_quality.py BUS_Offensief_Dyn --verbose
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402
from common.pg import load_rows_via_copy  # noqa: E402
from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: E402

#: Minor-unit quotes: the number is 1/100 of the currency the FX table knows.
#: ⚠ `GBp` IS PENCE AND `fx_rate` ONLY HOLDS `GBP`. Read as pounds a £46.75 share prices at £4,675;
#: refused outright, a 5,930-bar holding vanishes from every portfolio that owns it.
SUBUNITS = {"GBP": "GBp", "ZAR": "ZAc", "ILS": "ILA"}

#: A close this far above or below the previous one is a corporate action, not a market move.
#: ⚠ OUR CLOSES ARE NOT SPLIT-ADJUSTED and cannot self-heal — ingest only fetches dates NEWER than
#: our stored max, so a vendor's retroactive rewrite is never re-read.
SPLIT_JUMP = 1.9

#: Trading days behind the freshest close we hold anywhere before a series counts as stale.
STALE_DAYS = 5

#: The euro did not exist before this date, so `fx_rate` cannot and will never hold a rate for it.
#: ⚠ WITHOUT THIS THE CHECK CRIED WOLF ON EVERY US HOLDING. The first run flagged eight of them —
#: "fx USD starts 1999-01-04 but the price series starts 1986" — which is not a gap in our data,
#: it is the currency's own birthday. What the check is FOR is a currency added late (TWD arrived
#: with 20 rows from 2026-05-27 while its real history runs to 2014), and that is what a start
#: date well AFTER the euro's own reveals.
EURO_EPOCH = "1999-01-05"

#: Median daily traded value over market cap. ⚠ THE DOCUMENTED WRONG-LISTING DETECTOR — never an
#: exchange map (Vienna is not Prague) and never "ISIN country != listing country" (that flags
#: every deliberate ADR). Below this the line is a shadow of the real one.
THIN_LISTING = 1e-5

#: Holding names that legitimately carry no ISIN. ⚠ CASH IS NOT A GAP, and the Dutch matters:
#: the first run reported `Effectenrekening` — the securities cash account — as a missing
#: identifier, which is the audit inventing work.
CASH_WORDS = ("liquid", "cash", "effectenrekening", "rekening", "kas")


def paged(table: str, select: str, build, order: str) -> list[dict]:
    """⚠ PAGED. PostgREST truncates at 1,000 rows on cloud; an unpaged audit would read the first
    page and pronounce the rest healthy."""
    out: list[dict] = []
    off = 0
    while True:
        rows = build(deps.supabase.table(table).select(select)).order(order) \
            .range(off, off + 999).execute().data or []
        if not rows:
            return out
        out += rows
        off += len(rows)


def _to_eur(value: float, ccy: str, rates: dict[str, float]) -> float | None:
    """A local price in EUR, or None when the rate is unknown.

    ⚠ THE MINOR UNIT IS SCALED ON THE **RATE**, never by forgetting the divisor: `eur = pence /
    (100 x gbp_rate)`. Drop the 100 and £46.75 prices at £4,675.
    """
    c = (ccy or "").strip()
    if not c:
        return None
    if c == "EUR":
        return value
    scale = 1.0
    for major, minor in SUBUNITS.items():
        if c == minor:
            c, scale = major, 100.0
    r = rates.get(c)
    if not r:
        return None
    return value / scale / r


class Report:
    """Findings, grouped by severity so the summary can lead with what matters."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []

    def add(self, level: str, holding: str, msg: str) -> None:
        self.rows.append((level, holding, msg))

    def section(self, title: str, level: str) -> None:
        hits = [r for r in self.rows if r[0] == level]
        if not hits:
            return
        print(f"\n{title} ({len(hits)})")
        print("-" * 78)
        for _, h, m in hits:
            print(f"  {h[:34]:<34} {m}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("book", help="the AIRS portefeuille name, e.g. BUS_Offensief_Dyn")
    ap.add_argument("--verbose", action="store_true", help="list every holding, not just findings")
    args = ap.parse_args()

    sb = deps.supabase
    rep = Report()

    # ── the book ────────────────────────────────────────────────────────────
    holdings = paged("airs_holding", "*", lambda q: q.eq("portefeuille", args.book), "id")
    if not holdings:
        print(f"No holdings stored for {args.book!r}.")
        return 1
    as_of = max(h["as_of_date"] for h in holdings if h["as_of_date"])
    live = [h for h in holdings if h["as_of_date"] == as_of]
    total_val = sum(h["current_value_eur"] or 0 for h in live)

    print(f"BOOK   {args.book}")
    print(f"as of  {as_of}   ·   {len(live)} positions   ·   EUR {total_val:,.0f}")
    fetched = max((h["retrieved_at"] or "") for h in live)
    print(f"read   {fetched[:19] if fetched else 'unknown'}")

    # ⚠ THE ROSTER IS THE ONLY PLACE THAT KNOWS A SCRAPE FAILED. A book whose `volk` report never
    # arrived still has yesterday's holdings, and they look exactly like today's.
    ros = (sb.table("airs_account_roster").select("*")
           .eq("portefeuille", args.book).execute().data or [])
    if ros:
        ok = ros[0].get("reports_ok") or []
        missing = {"att", "model", "mut", "trans", "volk"} - set(ok)
        if missing:
            rep.add("ERROR", "(book)", f"AIRS reports missing: {', '.join(sorted(missing))}")

    # ⚠ AIRS'S OWN VALUATION DATE, not ours. A book valued days ago is not a book we read late.
    lag = (date.today() - date.fromisoformat(as_of)).days
    if lag > 4:
        rep.add("WARN", "(book)", f"AIRS last valued this book {lag} days ago ({as_of})")

    wsum = sum(h["weight"] or 0 for h in live)
    if abs(wsum - 1.0) > 0.005:
        rep.add("ERROR", "(book)", f"weights sum to {wsum:.4f}, not 1.0")

    # ── reference data ──────────────────────────────────────────────────────
    isins = sorted({h["isin"] for h in live if h.get("isin")})
    grid = {g["isin"]: g for g in paged(
        "asset_grid", "isin,analysis_id,yahoo_symbol,name,exchange,currency,status,bars,"
                      "first_date,price_to,med_adv_eur,sector,msci_region,domicile_country,"
                      "listing_country,market_cap_eur,wrapper,openfigi_type,is_leveraged,"
                      "delisted_at,out_of_scope_at,illiquid_at,company_id,gf_ticker,gf_exchange",
        lambda q: q.in_("isin", isins), "isin") if g.get("isin")}

    # The freshest close anywhere — staleness is measured against the market, never the calendar,
    # so "today's close is not published yet" and a vendor outage do not read as a stale holding.
    #
    # ⚠ ASKED OF `asset_analysis`, NOT `asset_price`. The obvious query — `asset_price` ordered by
    # `target_date desc limit 1` — TIMES OUT (57014): that table holds ~10M rows and its primary
    # key is `(analysis_id, target_date)`, so there is no index that answers a date-only sort and
    # Postgres has to scan the lot. `asset_analysis` carries a `price_to` per instrument, ~8k rows,
    # and is the same question one level up.
    newest = (sb.table("asset_analysis").select("price_to")
              .not_.is_("price_to", "null")
              .order("price_to", desc=True).limit(1).execute().data or [])
    market_to = str(newest[0]["price_to"]) if newest else None

    aids = sorted({g["analysis_id"] for g in grid.values() if g.get("analysis_id")})
    bars = defaultdict(list)
    bulk = load_rows_via_copy("asset_price", "analysis_id,target_date,close", "analysis_id", aids)
    for r in (bulk or []):
        if r.get("close") is not None:
            bars[r["analysis_id"]].append((str(r["target_date"]), float(r["close"])))
    for v in bars.values():
        v.sort()

    fx = defaultdict(list)
    fx_latest: dict[str, float] = {}
    fx_seen: dict[str, str] = {}
    for r in paged("fx_rate", "currency_code,rate_date,rate", lambda q: q, "rate_date"):
        c, d = r["currency_code"], str(r["rate_date"])
        fx[c].append(d)
        if d >= fx_seen.get(c, ""):
            fx_seen[c] = d
            fx_latest[c] = float(r["rate"])

    seen_name: dict[str, str] = {(h.get("isin") or ""): (h.get("holding_name") or "?").strip()
                                 for h in live}

    # ⚠⚠ TWO ISINs ON ONE PRICE SERIES IS TWO PRODUCTS DRAWN AS ONE. Found on the first run: ELEVEN
    # Leonteq certificates share `EONR.DE`, an EUR overnight-return index that has moved +3.85% in
    # two years — while AIRS prices two of them 13% apart and reports -1.7% and +11.7% YTD. The
    # euro amounts on screen come from AIRS and are right; every figure WE derive from a series
    # (volatility, beta, correlation, momentum, the model reprice) is drawn from a cash index.
    shared: dict[int, list[str]] = defaultdict(list)
    for iso, gg in grid.items():
        if gg.get("analysis_id"):
            shared[gg["analysis_id"]].append(iso)
    for aid, group in shared.items():
        if len(group) > 1:
            names = [n for n in (seen_name.get(i) for i in group) if n]
            rep.add("ERROR", (names[0] if names else group[0])[:34],
                    f"{len(group)} holdings share one price series (analysis {aid}): "
                    + ", ".join(group))

    # ── per holding ─────────────────────────────────────────────────────────
    seen_isin: dict[str, str] = {}
    priced = unpriced = 0
    for h in sorted(live, key=lambda r: -(r["current_value_eur"] or 0)):
        nm = (h.get("holding_name") or "?").strip()
        isin = (h.get("isin") or "").strip()
        w = (h.get("weight") or 0) * 100

        if not isin:
            # ⚠ CASH HAS NO ISIN AND THAT IS CORRECT — not every missing identifier is a gap.
            if not any(w in nm.lower() for w in CASH_WORDS):
                rep.add("WARN", nm, f"no ISIN ({w:.1f}% of the book)")
            continue
        if isin in seen_isin:
            rep.add("ERROR", nm, f"shares ISIN {isin} with {seen_isin[isin]!r}")
        seen_isin[isin] = nm

        g = grid.get(isin)
        if not g:
            rep.add("ERROR", nm, f"{isin} is not an instrument in the grid ({w:.1f}%)")
            unpriced += 1
            continue
        if g.get("status") != "ok":
            rep.add("ERROR", nm, f"grid status={g.get('status')!r} — {g.get('reason') or 'no reason'}")

        for marker, what in (("delisted_at", "delisted"), ("out_of_scope_at", "out of scope"),
                             ("illiquid_at", "marked illiquid")):
            if g.get(marker):
                rep.add("WARN", nm, f"{what} since {str(g[marker])[:10]}")

        # ── currency and FX ────────────────────────────────────────────────
        ccy = (g.get("currency") or "").strip()
        major = None
        for maj, minor in SUBUNITS.items():
            if ccy == minor:
                major = maj
        need = major or ccy
        if need and need != "EUR":
            if need not in fx:
                # ⚠ THE WORST FAILURE IN THIS FILE. A missing rate does not blank a cell — the
                # holding leaves the portfolio and the return renormalises over the rest.
                rep.add("ERROR", nm, f"no fx_rate rows for {need} — this holding cannot be priced")
            else:
                first = (bars.get(g["analysis_id"]) or [("9999", 0)])[0][0]
                start = fx[need][0] if fx[need] else None
                # ⚠ MEASURED AGAINST THE EURO'S OWN START, not against our price history — see
                # `EURO_EPOCH`. A rate that begins with the currency is complete; one that begins
                # years later is the real defect, and it silently converts a whole stretch of
                # history at one back-filled number.
                if start and start > EURO_EPOCH and start > first:
                    rep.add("ERROR" if start > "2020-01-01" else "WARN", nm,
                            f"fx {need} only goes back to {start} — everything before that "
                            f"converts at one back-filled rate (series starts {first})")
        if major:
            rep.add("INFO", nm, f"quoted in {ccy} (minor unit of {major}) — divide by 100")

        # ── the price series ───────────────────────────────────────────────
        series = bars.get(g.get("analysis_id")) or []
        if not series:
            rep.add("ERROR", nm, f"no price bars at all ({w:.1f}% of the book)")
            unpriced += 1
            continue
        priced += 1
        last = series[-1][0]
        if market_to and last < market_to:
            gap = (date.fromisoformat(market_to) - date.fromisoformat(last)).days
            if gap > STALE_DAYS:
                rep.add("ERROR" if gap > 15 else "WARN", nm,
                        f"last close {last}, {gap} days behind the market ({market_to})")

        # ⚠ AN UNADJUSTED SPLIT IS A RETURN, NOT A GAP. It hits a cap-weighted figure twice in the
        # same direction, because the start weight is backed out through the same broken price.
        #
        # ⚠⚠ THE NEWEST JUMP, NOT THE FIRST — this loop ran forwards and `break`ed, so a 1997
        # oddity HID a 2026 one. The splits that matter are the recent ones (KLA, CrowdStrike and
        # DuPont were all rewritten by the vendor in 2026), and those are exactly the ones an
        # oldest-first scan reports last, which is to say never.
        jumps = []
        for i in range(1, len(series)):
            a, b = series[i - 1][1], series[i][1]
            if a > 0 and b > 0 and (b / a > SPLIT_JUMP or a / b > SPLIT_JUMP):
                jumps.append((series[i][0], a, b))
        if jumps:
            d, a, b = jumps[-1]
            extra = f" (+{len(jumps) - 1} older)" if len(jumps) > 1 else ""
            # ⚠ ONLY RECENT ONES ARE A WARNING. A 1997 step on a 30-year series is history nobody
            # is pricing off; the same step last year is in every return on the screen.
            recent = d >= (date.today() - timedelta(days=730)).isoformat()
            rep.add("WARN" if recent else "INFO", nm,
                    f"close jumps {a:g} -> {b:g} on {d} (x{b / a:.2f}) "
                    f"— possible unadjusted split{extra}")

        # ── is this the real listing, or a shadow of it? ───────────────────
        adv, cap = g.get("med_adv_eur"), g.get("market_cap_eur")
        if adv and cap and cap > 0 and (adv / cap) < THIN_LISTING:
            rep.add("WARN", nm,
                    f"{g.get('yahoo_symbol')} on {g.get('exchange')} trades EUR {adv:,.0f}/day "
                    f"against a EUR {cap / 1e9:,.0f}bn company ({adv / cap:.1e}) "
                    f"— a shadow listing, not the home line")

        # ── does AIRS agree with our last close? ───────────────────────────
        # ⚠⚠ THE ONE GENUINELY INDEPENDENT CHECK IN THIS FILE. Everything else asks whether our own
        # data is self-consistent; this asks whether it matches a source that never saw it. On the
        # first run it separated 25 holdings agreeing to the cent from two that did not.
        # ⚠ BOTH SIDES CONVERTED TO EUR FIRST. The first version compared a USD close against an
        # EUR valuation and "found" a 17% error in ASML, which was the exchange rate.
        theirs, their_ccy = h.get("current_price_local"), (h.get("currency") or "").strip()
        if theirs and series:
            ours_eur = _to_eur(series[-1][1], ccy, fx_latest)
            theirs_eur = _to_eur(float(theirs), their_ccy, fx_latest)
            if ours_eur and theirs_eur and theirs_eur > 0:
                off = ours_eur / theirs_eur - 1
                if abs(off) > 0.03:
                    rep.add("ERROR" if abs(off) > 0.08 else "WARN", nm,
                            f"our close EUR {ours_eur:,.2f} vs AIRS EUR {theirs_eur:,.2f} "
                            f"({off * 100:+.1f}%) — we may be pricing a different instrument")

        # ── classification ─────────────────────────────────────────────────
        if not g.get("sector") and not g.get("wrapper"):
            rep.add("WARN", nm, "no sector — it will fall out of the sector charts")
        if not g.get("msci_region"):
            rep.add("WARN", nm, "no MSCI region")
        elif not g.get("domicile_country"):
            # ⚠ REGION FALLS BACK TO THE LISTING COUNTRY when domicile is unknown — that is how the
            # S&P once read 7.2% European, off US megacaps priced on thin German lines.
            rep.add("INFO", nm,
                    f"region {g['msci_region']} derived from the listing "
                    f"({g.get('listing_country')}), not a known domicile")

        # ── the vendor's fundamental side ──────────────────────────────────
        gf_ex = g.get("gf_exchange")
        if g.get("company_id") and gf_ex and not is_gf_subscribed_exchange(gf_ex):
            rep.add("WARN", nm,
                    f"GuruFocus company on {gf_ex}, outside the subscription — "
                    f"fundamentals are not fetched (and older stored ones may be wrong)")

        if args.verbose:
            print(f"  {w:5.1f}%  {nm[:30]:<30} {g.get('yahoo_symbol') or '-':<11} "
                  f"{ccy:<4} {len(series):>5} bars  {series[0][0]}..{last}")

    # ── AIRS's own split, where it exists ──────────────────────────────────
    with_split = [h for h in live if h.get("fund_result_eur") is not None]
    if live and not with_split:
        rep.add("INFO", "(book)", "AIRS published no Koers/Valuta split for this snapshot")

    # ── output ─────────────────────────────────────────────────────────────
    print(f"\npriced {priced} of {len(live)} positions"
          + (f", {unpriced} unpriceable" if unpriced else ""))
    rep.section("ERRORS — a figure on screen is wrong or a holding is missing", "ERROR")
    rep.section("WARNINGS — worth a look", "WARN")
    rep.section("NOTES — correct, but easy to misread", "INFO")

    counts = Counter(r[0] for r in rep.rows)
    print(f"\n{counts.get('ERROR', 0)} errors · {counts.get('WARN', 0)} warnings "
          f"· {counts.get('INFO', 0)} notes")
    return 1 if counts.get("ERROR") else 0


if __name__ == "__main__":
    raise SystemExit(main())

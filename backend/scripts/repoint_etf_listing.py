"""Move an ETF row off a THIN cross-listing onto the most liquid listing OF THE SAME ISIN.

WHY `repoint_primary_listing.py` CANNOT DO THIS
    That script detects on median-daily-traded-value / MARKET CAP, and filters to
    `asset_class == 'equity'`. An ETF has no market cap, so the ratio is undefined and the
    row is skipped: it reports zero candidates over a grid full of thin ETFs. Detection here
    is the plain traded value instead — for a fund there is no denominator to normalise by,
    and none is needed: EUR 4,787/day is not a real market for anything.

WHY IT MUST NOT RE-RESOLVE BY NAME  ← the whole point of this file
    `resolve()` searches Yahoo BY NAME and gates identity with `same_company(...)`, which
    strips corporate forms. For an operating company that is exactly right. For a FUND it is
    dangerous, because a fund's share classes have near-identical names and DIFFERENT ISINs:

        IE00BNDS1P30   V3GF.MI   Vanguard ESG Global All Cap UCITS ETF
        IE00BNDS1Q47   V3GE.DE   Vanguard ESG Global All Cap UCITS ETF

    Both are in our grid. They differ by accumulating/distributing (and currency), which no
    name comparison survives — `same_company` would happily swap one for the other. That is
    not a thin-listing annoyance, it is a WRONG PRICE SERIES: an accumulating class rolls
    dividends into the price, a distributing class does not, so the two compound differently.

    So candidates come from OpenFIGI's listings OF THIS ISIN (`lookup_isin` -> the venues
    that one share class actually trades on). Being ISIN-anchored, the candidate set cannot
    contain a sibling share class — the safety is structural, not a name heuristic.

    The name check that remains is a guard on the other failure: `build_candidates` CONSTRUCTS
    a Yahoo symbol (`ticker + venue suffix`), and tickers are reused across venues, so a
    constructed symbol can land on an unrelated instrument. That is what `figi_name` gates.

GATES (a row is only rewritten when all hold)
    * the winner is a DIFFERENT symbol than the incumbent,
    * it actually has price bars (a zero-bar "resolution" is not a resolution — the GODE.DE
      incident: ten structured products all "resolved" to one empty series, status ok),
    * it is at least `--min-gain`x more liquid than the incumbent.
    Dry run by default; every candidate's traded value is printed, win or lose, because both
    mis-resolutions we've hit this month were only visible in the per-row output.

    cd backend && uv run python scripts/repoint_etf_listing.py                 # dry run, all thin ETFs
    cd backend && uv run python scripts/repoint_etf_listing.py --isin IE00BZ0PKT83
    cd backend && uv run python scripts/repoint_etf_listing.py --apply
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  — loads SUPABASE_* / OPENFIGI_API_KEY before anything else
from asset_pipeline import openfigi, store  # noqa: E402
from asset_pipeline.fast_resolve import _score_retry, build_candidates  # noqa: E402
from asset_pipeline.resolve import resolve_analysis_instrument, same_company  # noqa: E402

# Below this a listing is not a market — it is a quote. (The thinnest we found: an iShares
# multifactor ETF at EUR 4,787/day, against the same fund's primary line.)
THIN_ADV_EUR = 250_000

# How many of the ISIN's venues to probe. `fast_resolve` caps at 5, which is right for an
# equity (home listing first, done). A UCITS ETF genuinely lists on 6-10 venues, and the
# liquid one is not reliably the home-country one, so probe wider.
CAND_LIMIT = 12

PAUSE_S = 1.0  # Yahoo answers with an EMPTY list under load, not a 429. Don't hammer it.

_FIGI_COLS = ("openfigi_figi", "openfigi_name", "openfigi_ticker", "openfigi_exch",
              "openfigi_type")


def _same_fund(candidate_name: str | None, anchor: str | None) -> bool:
    """Is this candidate the same FUND as the row we're repointing?

    ⚠ DO NOT ANCHOR THIS ON OpenFIGI'S NAME. OpenFIGI abbreviates a fund past the point any
    fuzzy matcher can recover — measured against Yahoo's name for the very same listing:

        iShares STOXX World Equity Multifactor UCITS ETF USD (Acc)
        ISH STO WOR EQT MU UC ET-USD                                 -> 55.8   REJECT
        Vanguard ESG Global Corporate Bond UCITS ETF EUR Hedged Inc
        VANG ESG GC ETF EUR H DIST                                   -> 59.5   REJECT

    `same_company` strips corporate forms; it cannot bridge vowel-crushing. Anchored on
    OpenFIGI, this gate rejected the INCUMBENT against its own name on 4 of 9 rows — every
    candidate fell through, and the script "kept" the thin listing it exists to replace.
    (One row, VanEck, scored 88 and passed. That is luck, not a working gate.)

    The anchor is the incumbent's YAHOO name instead: same source, same naming convention,
    so the other venues of one ISIN come back near-identical — while a ticker that collides
    with an unrelated instrument on some venue does not. With no anchor at all we cannot
    check, and an unchecked swap is exactly what we refuse to do.
    """
    if not anchor or not candidate_name:
        return False
    return same_company(candidate_name, anchor)


def _consensus_anchor(names: list[str]) -> str | None:
    """The anchor for a row that has NO incumbent — a `queued` ETF, never resolved, so there is
    no Yahoo name to compare a candidate against.

    Neither of the other names will do, and both failures are documented above: OpenFIGI
    abbreviates a fund past what any matcher can recover ("ISH STO WOR EQT MU UC ET-USD"), and
    Leonteq's is the same vowel-crushing ("ISHR EDGE MSCI WRLD MOMENTUM"). Anchoring on either
    rejects every candidate, including the right one.

    But the candidates all come from OpenFIGI's listings OF THIS ONE ISIN, and a fund's venues
    report near-identical names *to Yahoo* — same source, same convention. So the anchor is
    their AGREEMENT: whichever name the most candidates concur with is the fund, and a
    constructed ticker that collided with an unrelated instrument on some venue is the outlier
    that agrees with nobody.

    Requires a real majority of 2+. One lone candidate is not a consensus — it is an unchecked
    guess, and an unchecked swap is the one thing this file exists to refuse.
    """
    best, best_n = None, 0
    for i, n in enumerate(names):
        agree = 1 + sum(1 for j, m in enumerate(names) if j != i and same_company(n, m))
        if agree > best_n:
            best, best_n = n, agree
    return best if best_n >= 2 else None


def _thin_etfs(sb, isin: str | None) -> list[dict]:
    cols = ("isin,name,analysis_symbol,med_adv_eur,asset_class,status,wrapper,"
            + ",".join(_FIGI_COLS))
    rows, off = [], 0
    while True:
        q = sb.table("asset_grid").select(cols)
        if isin:
            # An EXPLICIT ISIN is judged whatever its status. A `queued` row has never been
            # resolved at all — no symbol, no prices, invisible everywhere — and it is exactly
            # the row that most needs an ISIN-anchored resolve, because the by-name path
            # (`resolve()`) is the one that swaps a fund for its sibling share class.
            # IE00BP3QZ825 (iShares Edge MSCI World Momentum) sat queued: OpenFIGI lists its
            # LSE line as IWFM, and nothing was reaching it.
            q = q.eq("isin", isin)
        else:
            q = q.eq("status", "ok").eq("asset_class", "etf")
        batch = q.range(off, off + 999).execute().data or []
        rows += batch
        if len(batch) < 1000:
            break
        off += 1000

    out = []
    for r in rows:
        # A wrapper (a Bitcoin ETP analysed as BTC-USD) is priced off its underlying — its
        # own listing's traded value says nothing about the series we actually use.
        if r.get("wrapper"):
            continue
        # In a SWEEP, a row with no incumbent has no thinness to measure and is skipped. Named
        # explicitly, it is the whole point.
        if not isin and not r.get("analysis_symbol"):
            continue
        adv = float(r.get("med_adv_eur") or 0)
        if isin or (0 < adv < THIN_ADV_EUR):
            out.append(r)
    out.sort(key=lambda r: float(r.get("med_adv_eur") or 0))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="persist the fixes (default: dry run)")
    ap.add_argument("--isin", help="one ISIN, bypassing the thinness filter")
    ap.add_argument("--min-gain", type=float, default=2.0,
                    help="require the new listing to be this many times more liquid")
    ap.add_argument("--limit", type=int, default=0, help="stop after N rows (0 = all)")
    a = ap.parse_args()

    sb = deps.supabase
    cands = _thin_etfs(sb, a.isin)
    if a.limit:
        cands = cands[: a.limit]

    print(f"{len(cands)} thin ETF row(s){' — APPLYING' if a.apply else ' — DRY RUN'}\n",
          flush=True)
    fixed = kept = failed = 0

    for r in cands:
        isin, old = r["isin"], r.get("analysis_symbol")     # `old` is None for a QUEUED row
        old_adv = float(r.get("med_adv_eur") or 0)
        # Yahoo's own name for the incumbent listing — the only anchor that survives a fund
        # name. See `_same_fund` for why OpenFIGI's cannot be used here. A queued row has no
        # incumbent and therefore no such name; its anchor is derived below, by consensus.
        anchor = r.get("name")
        fig = {k: r.get(k) for k in _FIGI_COLS}

        label = old or f"(queued: {r.get('status')})"
        print(f"  {isin}  {label:<11} EUR {old_adv:>11,.0f}/day   {(r.get('name') or '')[:40]}",
              flush=True)

        # Every venue THIS share class trades on. ISIN-anchored: a sibling share class
        # (a different ISIN) can never appear here.
        #
        # `yahoo_isin=True` adds Yahoo's own resolution of the ISIN to the pool. The other
        # candidates are CONSTRUCTED (`ticker + venue suffix`), which assumes OpenFIGI and
        # Yahoo agree on the ticker — on German venues they frequently do not, and the liquid
        # listing then cannot be reached at all: DE000A0F5UH1 is `SDGPEX` to OpenFIGI and
        # `ISPA` to Yahoo, so we built the non-existent SDGPEX.DE and never saw Xetra. It is
        # only ever an extra candidate — Yahoo's ISIN pick is liquidity-blind (it answers
        # Alphabet with 1GOOGL.MI) and is ranked and name-gated below like any other.
        figi_rows = openfigi.lookup_isin(isin)
        symbols = build_candidates(isin, figi_rows, None, limit=CAND_LIMIT, yahoo_isin=True)
        if not symbols:
            kept += 1
            print("      keep — OpenFIGI lists no venue we can map to a Yahoo symbol\n",
                  flush=True)
            continue

        # PROBE FIRST, GATE SECOND. A queued row's anchor is the agreement among these very
        # candidates, so it cannot be known until they have all been scored.
        probed: list[dict] = []
        for sym in symbols:
            sc = _score_retry(sym)
            adv = float((sc or {}).get("med_adv_eur") or 0)
            if not sc or adv <= 0:
                print(f"      {sym:<12} —  no price series", flush=True)
                continue
            probed.append(sc)
            time.sleep(PAUSE_S)

        if anchor is None:
            anchor = _consensus_anchor([str(s.get("name") or "") for s in probed])
            if anchor is None:
                kept += 1
                print("      keep — no incumbent to anchor on, and this ISIN's venues do not "
                      "agree on a name. Unverifiable; NOT judged.\n", flush=True)
                continue
            print(f"      anchor (consensus of this ISIN's venues): {anchor[:48]!r}", flush=True)

        scored: list[dict] = []
        for sc in probed:
            sym, adv = sc["symbol"], float(sc.get("med_adv_eur") or 0)
            # A constructed symbol can collide with an unrelated instrument on that venue,
            # so the candidate's name still has to be checked — but NOT against OpenFIGI's.
            if not _same_fund(sc.get("name"), anchor):
                print(f"      {sym:<12} EUR {adv:>11,.0f}/day  REJECT — different name "
                      f"({(sc.get('name') or '')[:34]!r})", flush=True)
                continue
            mark = "  <- incumbent" if sym == old else ""
            print(f"      {sym:<12} EUR {adv:>11,.0f}/day{mark}", flush=True)
            scored.append(sc)

        if not scored:
            kept += 1
            print("      keep — no candidate validated\n", flush=True)
            continue

        # THE INCUMBENT MUST SURVIVE ITS OWN COMPARISON. If it doesn't, we are not looking at
        # a complete picture of this ISIN's venues, and every "keep" below would be a false
        # negative wearing a clean bill of health. Two distinct causes, and they are not the
        # same bug — say which. (A queued row has no incumbent, so there is nothing to survive:
        # the consensus above IS its check, and it is a stricter one.)
        if old and old not in symbols:
            #  (a) Neither OpenFIGI's listings for this ISIN nor Yahoo's own resolution of it
            #      produced the symbol we already hold. The candidate set is INCOMPLETE, so
            #      "most liquid" is unknowable — exactly the empty-candidate-set trap that put
            #      Alphabet on Vienna.
            kept += 1
            print(f"      keep — !! {old} is not among this ISIN's listings (OpenFIGI + "
                  f"Yahoo), so the candidate set is incomplete. NOT judged.\n", flush=True)
            continue
        if old and not any(s["symbol"] == old for s in scored):
            #  (b) It WAS a candidate and its own name gate threw it out. The gate is broken.
            kept += 1
            print(f"      keep — !! the incumbent {old} failed its own name gate. The gate is "
                  f"misconfigured; this row was NOT judged.\n", flush=True)
            continue

        best = max(scored, key=lambda s: float(s.get("med_adv_eur") or 0))
        new, new_adv = best["symbol"], float(best.get("med_adv_eur") or 0)

        if old and new == old:
            kept += 1
            print("      keep — the incumbent IS the most liquid listing of this ISIN\n",
                  flush=True)
            continue
        if old and new_adv < old_adv * a.min_gain:
            kept += 1
            print(f"      keep — {new} is not {a.min_gain}x more liquid "
                  f"({new_adv:,.0f} vs {old_adv:,.0f})\n", flush=True)
            continue

        if old:
            gain = (new_adv / old_adv) if old_adv else float("inf")
            print(f"      FIX  {old} -> {new}   ({gain:,.1f}x more liquid)\n", flush=True)
        else:
            print(f"      RESOLVE  (queued) -> {new}   EUR {new_adv:,.0f}/day — the most liquid "
                  f"listing of this ISIN\n", flush=True)
        fixed += 1

        if a.apply:
            best["eligible"] = True
            ai = resolve_analysis_instrument(best, r.get("asset_class") or "etf")
            res = {
                "input": isin, "id_type": "isin",
                "asset_class": ai["analysis_asset_class"], "wrapper": ai["wrapper"],
                "is_leveraged": ai["is_leveraged"], "candidates": [best],
                "execution": ai["execution"], "analysis": ai["analysis"],
                "chosen": ai["analysis"], "underlying": None,
                "reason": (f"{'Repointed' if old else 'Resolved'} to {new} — most liquid "
                           f"listing of this ISIN (OpenFIGI-anchored)."),
                "analysis_note": ai["analysis_note"], "sector": ai["analysis_asset_class"],
                "candles": None, "ibkr": None,
            }
            ids = store.upsert_asset(res, figi=fig)
            rows = store.store_series(ids["analysis_id"], ai["analysis"]["symbol"],
                                      ai["analysis"].get("first_ts"))
            # A resolution with no price series is not a resolution (the GODE.DE incident).
            # Here it would be worse: we'd have swapped a working thin row for an empty one.
            if not rows:
                failed += 1
                fixed -= 1
                print(f"      !! {new} stored 0 bars — NOT a usable listing. "
                      f"Re-point by hand.\n", flush=True)

    print(f"\n  fixed={fixed}  kept={kept}  failed={failed}")
    if a.apply and fixed:
        try:
            store.set_default_executions()
            print("  refreshed is_default flags")
        except Exception as e:  # noqa: BLE001
            print(f"  set_default_executions failed: {e}")
    if not a.apply and fixed:
        print("\n  dry run — re-run with --apply to persist")
    return 0


if __name__ == "__main__":
    sys.exit(main())

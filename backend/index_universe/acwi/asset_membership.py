"""ACWI membership authored straight from the iShares file, resolved to ASSETS.

⚠⚠ IT EXISTS BECAUSE THE COMPANY WORLD IS NARROWER THAN THE INDEX. `universe_asset_membership` is a
view over `universe_membership -> company.isin -> asset_execution.isin`, so a constituent is only a
member if GuruFocus sells it to us — and `FEASIBLE_GF_EXCHANGES` has no Toronto, no ASX, no LSE, no
Johannesburg. Constellation Software sat in `asset_execution` as `CA21037X1006 / CSU.TO`, priced by
yfinance, and was not in the index it is demonstrably in. Measured: 132 constituents in that state.

⚠⚠ TICKER + EXCHANGE, NEVER NAME — see `yahoo_map`. The export has no ISIN column, and a name join
on this data matched Berkshire B to Berkshire A, US Newmont to the Australian CDI line, and Mizuho
to Magellan Financial Group. A venue `yahoo_map` cannot place yields no row at all.

⚠⚠ AND THE FILE'S VENUE IS OFTEN NOT THE VENUE WE HOLD — the Shopify case, reported 2026-09-01.
iShares books Shopify on the **Toronto** Stock Exchange, so the symbol is `SHOP.TO`; we hold the
same company as `SHOP` on NasdaqGS under `CA82509L1076`. The exact-symbol join therefore missed a
constituent we price perfectly well. Measured on the 15-Apr-2026 file, 111 rows are in that state.

⚠⚠ THE OBVIOUS FIX — MATCH THE BARE TICKER ON ANY VENUE — IS CATASTROPHIC, AND THE 111 ROWS ARE
THEIR OWN PROOF. `TGT` (Target, United States) finds `TGT.DU` = **11 88 0 Solutions AG**; `NG.`
(National Grid) finds `NG` = **NovaGold Resources**; `RMS` (Hermès) finds `RMS.AX` = **Ramelius
Resources**; `OR` (L'Oréal) finds `OR` = **OR Royalties**; `T` (Telus) finds **AT&T**; `BA.` (BAE
Systems) finds **Boeing**. That is the WisdomTree Coffee -> Luckin Coffee failure again, on a
column that would look entirely ordinary afterwards.

⚠⚠ SO THE FALLBACK IS GATED ON THE ISIN'S OWN COUNTRY AGAINST THE COUNTRY THE FILE NAMES, which is
the rule `scripts/close_company_bridge.py` already proved for exactly this "same ticker, different
venue" question. Shopify: the file says Canada and `CA82509L1076` says Canada, so it is the same
issuer. Every collision above is refused because the country differs. It also picks the RIGHT LINE
where several exist: `RIO TINTO PLC` is filed under United Kingdom and we hold three RIO rows
(GB, AU, US) — the gate takes `GB0007188757`, the London ordinary, not the Australian company or
the US ADR. Same for BP, ASML and Gold Fields against their US ADRs.

⚠ IT ERRS SAFE. A country the `country` table cannot name resolves to None and the row is refused,
so an unmapped country costs a member rather than inventing one. Constituents whose ISIN is
registered somewhere other than where they list — Ferrari (filed Italy, `NL…`), Ferrovial (Spain,
`NL…`), Tenaris (Italy, `LU…`) — stay out for the same reason; a false refusal costs one member
this run, a false accept puts a different company in the index for ever.

⚠ MEASURED on the 15-Apr-2026 file: **1,541 -> 1,724 members**, of which 63 come from this rule and
the rest from `yahoo_map`'s venue and ticker-spelling fixes landing in the same run. All 63 were
read by eye against their stored name and ISIN. `venue_unknown` 139 -> 45; `not_held` 479 -> 498,
which is a rise on purpose — rows that used to be miscounted as an unplaceable venue are now
correctly counted as an instrument we simply do not hold.

⚠ IT WRITES ONLY WHAT IT RESOLVED, AND DELETES WHAT IT NO LONGER RESOLVES. The file is the
authority on what is in ACWI today, so a constituent that leaves it must leave this table — a
membership table that only ever grows is a list of everything that was ever in the index.
"""
from __future__ import annotations

import logging

import deps
from asset_pipeline.geo import normalize_country
from asset_pipeline.resolve import same_company
from index_universe.acwi.holdings import load_acwi_holdings
from index_universe.acwi.yahoo_map import yahoo_symbol

log = logging.getLogger(__name__)

#: The universe this file authors. ⚠ BY LABEL, like every other universe resolution in this app —
#: ids differ between environments and a hardcoded one silently writes into the wrong universe.
ACWI_LABEL = "ACWI"

SOURCE = "ishares_acwi"


def _universe_id(label: str) -> int | None:
    rows = (deps.supabase.table("universe").select("universe_id")
            .eq("label", label).limit(1).execute().data or [])
    return rows[0]["universe_id"] if rows else None


def _country_by_code() -> dict[str, str]:
    """`{'CA': 'Canada'}` — the ISIN prefix's country, for the interlisting gate.

    ⚠ THE SAME `country` TABLE THE REGION LOGIC USES, not a second copy. A prefix this table cannot
    name yields None, which the gate reads as "refuse", never as "assume it matches".
    """
    rows = (deps.supabase.table("country").select("country_code,country_name")
            .execute().data or [])
    return {r["country_code"]: r["country_name"] for r in rows if r.get("country_code")}


def _assets_by_symbol_from(rows: list[dict]) -> dict[str, int]:
    """`{YAHOO_SYMBOL: analysis_id}` over already-read execution rows.

    ⚠ SEVERAL EXECUTIONS CAN SHARE ONE SYMBOL and it does not matter: measured, all 62 such symbols
    resolve to a SINGLE `analysis_id` (one company, a local line and a US ADR). Membership is a
    property of the company, so the last writer wins and wins with the same value.
    """
    out: dict[str, int] = {}
    for r in rows:
        sym = (r.get("yahoo_symbol") or "").strip().upper()
        if sym and r.get("analysis_id") is not None:
            out[sym] = r["analysis_id"]
    return out


def _executions() -> list[dict]:
    """Every healthy execution row, paged.

    ⚠ PAGED. PostgREST truncates silently at 1,000 rows on cloud and this reads ~8,200 — an unpaged
    version would resolve the first thousand symbols and drop the index's whole tail without an
    error anywhere.

    ⚠ ONE READ FOR BOTH INDEXES — `_assets_by_symbol_from` and `_by_base_ticker` are built from the
    same list; reading the table twice would double an 8,200-row scan for nothing.
    """
    out: list[dict] = []
    off = 0
    while True:
        rows = (deps.supabase.table("asset_execution")
                .select("isin,yahoo_symbol,analysis_id,name")
                .eq("status", "ok").order("isin")
                .range(off, off + 999).execute().data or [])
        if not rows:
            return out
        out += rows
        off += len(rows)


def _base(symbol: str) -> str:
    """`'DELTA-R.BK'` -> `'DELTA'` — the symbol stripped of its venue AND its class marker.

    ⚠⚠ IT MUST STRIP THE HYPHEN TOO, and that is not cosmetic. `yahoo_map` renders a class marker
    as a hyphen on every venue, so the Thai NVDR `DELTA.R` becomes `DELTA-R.BK` while the ordinary
    we hold is `DELTA.BK`. Splitting on the dot alone leaves `DELTA-R`, which matches nothing, and
    all twelve Thai constituents would silently stop resolving — a regression caused entirely by
    fixing the spelling of a DIFFERENT venue.

    ⚠ IT WIDENS THE CANDIDATE LIST ON PURPOSE, and the gate is what makes that safe: `BBD` now
    collects both Bombardier classes and Banco Bradesco. The country test drops Bradesco, the two
    Bombardier lines are then ambiguous, and `_interlisted` refuses rather than picking one.
    """
    return symbol.split(".")[0].split("-")[0]


def _by_base_ticker(rows: list[dict]) -> dict[str, list[dict]]:
    """`{'SHOP': [<the NasdaqGS row>, …]}` — the symbol with its venue and class marker removed.

    ⚠ A CANDIDATE LIST, NOT AN ANSWER. A bare ticker is reused across the world (`T` is Telus in
    Toronto and AT&T in New York), so this only narrows the search; `_interlisted` decides.
    """
    out: dict[str, list[dict]] = {}
    for r in rows:
        sym = (r.get("yahoo_symbol") or "").strip().upper()
        if sym and r.get("analysis_id") is not None:
            out.setdefault(_base(sym), []).append(r)
    return out


def _interlisted(cands: list[dict], location: str, name: str,
                 codes: dict[str, str]) -> dict | None:
    """The one execution row that is the SAME ISSUER as this file row, or None.

    ⚠ THE COUNTRY GATE IS THE TEST; the name is only a tie-breaker. Requiring the name to agree as
    well would cost seven correct members measured on the live file: GSK is stored as `GSK plc`
    against the file's `GLAXOSMITHKLINE` (the company renamed), five Thai NVDR lines carry
    `… NON-VOTING DR PCL` against the ordinary's name, and First Abu Dhabi Bank's stored name is
    simply wrong (`First Trust Multi Cap Value Al` over a correct `AEN000101016`). The country gate
    already refuses every collision in that set, so the name earns its place only where the gate
    leaves more than one candidate standing.

    ⚠ AMBIGUITY IS REFUSED, NOT GUESSED. Two rows from one country passing the name check means the
    ticker is reused by related instruments there, and picking either is a coin toss.
    """
    want = normalize_country(location)
    if not want:
        return None
    same = [c for c in cands
            if normalize_country(codes.get((c.get("isin") or "  ")[:2].upper())) == want]
    if len(same) > 1:
        same = [c for c in same if same_company(name, c.get("name") or "")]
    return same[0] if len(same) == 1 else None


def resolve() -> tuple[list[dict], dict]:
    """`(rows, stats)` — the file resolved to `index_file_membership` rows. Reads, never writes."""
    holdings, as_of = load_acwi_holdings()
    equities = [h for h in holdings if (h.get("Asset Class") or "").strip() == "Equity"]
    executions = _executions()
    by_symbol = _assets_by_symbol_from(executions)
    by_base = _by_base_ticker(executions)
    codes = _country_by_code()

    rows: dict[int, dict] = {}
    unplaced: list[str] = []
    unheld = 0
    interlisted = 0
    for h in equities:
        sym = yahoo_symbol(h.get("Ticker", ""), h.get("Exchange", ""), h.get("Location", ""))
        if not sym:
            unplaced.append(h.get("Exchange") or "?")
            continue
        aid = by_symbol.get(sym.upper())
        held_sym = sym.upper() if aid is not None else None
        if aid is None:
            # ⚠ THE EXACT SYMBOL FIRST, ALWAYS. This runs only when the file's own venue yields
            # nothing, so a constituent we hold where iShares says it is can never be diverted to
            # another listing by the looser rule below.
            hit = _interlisted(by_base.get(_base(sym.upper()), []),
                               h.get("Location") or "", h.get("Name") or "", codes)
            if hit is not None:
                aid = hit["analysis_id"]
                held_sym = (hit.get("yahoo_symbol") or "").strip().upper()
                interlisted += 1
        if aid is None:
            unheld += 1
            continue
        # ⚠ FIRST WIN, NOT LAST. iShares lists some names twice (`CSU` and the placeholder
        # `2299955D` for Constellation Software); the real ticker resolves and the placeholder does
        # not, but where both would, the first row is the one with the tradable ticker.
        #
        # ⚠⚠ `yahoo_symbol` IS THE ONE WE MATCHED, NOT THE ONE WE BUILT. On the interlisting path
        # those differ by definition — Shopify is `SHOP.TO` in the file and `SHOP` in our store —
        # and writing the constructed symbol would put a string in the table that matches no row we
        # hold, which is exactly the debugging trail a reader follows when a member looks wrong.
        rows.setdefault(aid, {
            "analysis_id": aid, "source": SOURCE,
            "ticker": h.get("Ticker"), "exchange": h.get("Exchange"),
            "yahoo_symbol": held_sym, "source_as_of": as_of,
        })
    stats = {
        "as_of": as_of, "equities": len(equities), "resolved": len(rows),
        "venue_unknown": len(unplaced), "not_held": unheld,
        # ⚠ REPORTED SEPARATELY because it is the looser rule's whole output. A run whose
        # interlisted count jumps is a run to look at; folded into `resolved` it would be invisible.
        "interlisted": interlisted,
    }
    return list(rows.values()), stats


def sync(label: str = ACWI_LABEL) -> dict:
    """Rewrite `index_file_membership` for `label` from the committed file.

    ⚠ DELETE-THEN-INSERT, SCOPED TO THIS UNIVERSE. The file is the authority on today's index, so a
    constituent that has left it must leave the table; an upsert alone would accumulate every name
    that has ever been in ACWI and quietly widen the benchmark for ever.

    ⚠ IT REFUSES TO EMPTY THE TABLE. If the file resolves to nothing — a parser change, a moved
    data file — the safe outcome is the previous membership, not an index with no members and a
    benchmark that computes off zero constituents without erroring.
    """
    uid = _universe_id(label)
    if uid is None:
        return {"status": "error", "detail": f"no universe labelled {label!r}"}

    rows, stats = resolve()
    if not rows:
        log.warning("[acwi] the file resolved to NO assets — keeping the existing membership")
        return {"status": "error", "detail": "resolved 0 assets; membership left unchanged", **stats}

    deps.supabase.table("index_file_membership").delete().eq("universe_id", uid).execute()
    payload = [{**r, "universe_id": uid} for r in rows]
    for i in range(0, len(payload), 200):        # `IN_CHUNK_SIZE` — the Cloudflare 502 guard
        deps.supabase.table("index_file_membership").insert(payload[i:i + 200]).execute()

    log.warning("[acwi] index_file_membership: %d asset(s) from the %s file "
                "(%d equities, %d venue unknown, %d not held, %d matched on another venue)",
                len(payload), stats["as_of"], stats["equities"],
                stats["venue_unknown"], stats["not_held"], stats["interlisted"])
    return {"status": "ok", "universe_id": uid, "written": len(payload), **stats}

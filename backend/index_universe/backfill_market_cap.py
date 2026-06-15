"""Backfill `company.market_cap_eur` + `market_cap_date` (and correct
`company_name`) from GuruFocus.

GuruFocus's stock summary returns the current market cap at
`summary.company_data.mktcap`, in MILLIONS of the stock's native (exchange)
currency. For each company we:

  1. fetch the summary (one call per company — same endpoint/ladder as the
     ISIN backfill),
  2. read `mktcap` (native, millions),
  3. convert to an absolute EUR figure at the latest FX rate for the company's
     currency (ECB rates are "units per 1 EUR", so EUR = native / rate; EUR
     itself → rate 1),
  4. store `market_cap_eur` (absolute EUR) + `market_cap_date` = today.

It's a point-in-time snapshot (GuruFocus's cap is "now") — re-run to refresh.
Companies GuruFocus can't price (out-of-scope regions → 403) or whose currency
has no FX rate are left NULL.

Usage:
    uv run python -m index_universe.backfill_market_cap                 # full run
    uv run python -m index_universe.backfill_market_cap --only-missing  # skip ones already set
    uv run python -m index_universe.backfill_market_cap --limit 50      # cap GF calls (testing)
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass, field

from supabase import Client

from index_universe.backfill_isin import _gf_symbol  # native: bare US / EXCH:TICK

_log = logging.getLogger(__name__)


@dataclass
class MarketCapResult:
    companies_scanned: int = 0
    skipped_have_value: int = 0
    gurufocus_calls: int = 0
    set_count: int = 0
    no_mktcap: int = 0       # call made, no usable mktcap came back
    no_fx_rate: int = 0      # had a mktcap but the currency has no FX rate → can't convert
    renamed: int = 0         # company_name corrected from GuruFocus
    renames: list[str] = field(default_factory=list)  # "cid: old → new" samples
    errors: list[str] = field(default_factory=list)


def _name_from_company_data(cd: dict) -> str | None:
    """The company's display name out of GuruFocus `summary.company_data`.
    GuruFocus uses `company` for the full name; we also try a couple of
    fallback keys so a minor API-shape change doesn't silently no-op."""
    for k in ("company", "company_name", "name"):
        v = cd.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _name_key(s: str | None) -> str:
    """Alphanumeric-only, lowercased form for comparing names WITHOUT churning
    on punctuation/spacing ("Apple Inc" == "Apple Inc."). A genuine mismatch
    (the wrong company entirely, e.g. "TSMC" vs "Forside Co Ltd") still differs."""
    import re  # noqa: PLC0415
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def gf_company_name_for(ticker: str, exchange_code: str | None) -> dict:
    """Fetch the company name GuruFocus reports for a (ticker, exchange).
    Returns `{name, found, symbol, log}`. Used by the per-row "name from
    GuruFocus" action so a mislabeled listing can be corrected to what the
    GuruFocus link actually shows."""
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    symbol = _gf_symbol(ticker or "", exchange_code)
    if not symbol:
        return {"name": None, "found": False, "symbol": None, "log": "no GuruFocus symbol for this (ticker, exchange)"}
    res = _api_request(_build_api_url(f"stock/{symbol}/summary"))
    if res.data is None:
        return {"name": None, "found": False, "symbol": symbol, "log": res.log}
    cd = ((res.data.get("summary") or {}).get("company_data") or {}) if isinstance(res.data, dict) else {}
    name = _name_from_company_data(cd)
    return {"name": name, "found": bool(name), "symbol": symbol, "log": res.log}


def _load_companies(supabase: Client) -> list[dict]:
    """Every company with name + ticker + exchange code + currency + current mktcap."""
    out: list[dict] = []
    offset = 0
    page = 1000
    for _ in range(50):
        resp = (
            supabase.table("company")
            .select("company_id, company_name, gurufocus_ticker, market_cap_eur, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code, currency_code)")
            .order("company_id")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _fx_rates_per_eur(supabase: Client) -> dict[str, float]:
    """`{currency: units-per-EUR}` from the latest fx_rate rows. EUR → 1.0."""
    from fx_rates import fetch_latest_from_db  # noqa: PLC0415
    rows = fetch_latest_from_db(supabase) or []
    rates: dict[str, float] = {"EUR": 1.0}
    for r in rows:
        cur = r.get("currency")
        rate = r.get("rate")
        if cur and rate:
            rates[cur.upper()] = float(rate)
    return rates


def _gurufocus_summary(symbol: str) -> tuple[float | None, str | None, str]:
    """Fetch a GuruFocus summary for `symbol` → `(mktcap, name, log)`.
    `mktcap` is `summary.company_data.mktcap` (native, millions); `name` is the
    company name GuruFocus reports for that listing (used to correct mislabeled
    rows). One call serves both."""
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    res = _api_request(_build_api_url(f"stock/{symbol}/summary"))
    if res.data is None:
        return None, None, res.log
    cd = ((res.data.get("summary") or {}).get("company_data") or {}) if isinstance(res.data, dict) else {}
    name = _name_from_company_data(cd)
    raw = cd.get("mktcap")
    try:
        v = float(raw)
        mktcap = v if v > 0 else None
    except (TypeError, ValueError):
        mktcap = None
    return mktcap, name, res.log


def backfill_market_cap(
    supabase: Client,
    *,
    only_missing: bool = False,
    limit: int | None = None,
    on_progress=None,
) -> MarketCapResult:
    def emit(msg: str, **fields) -> None:
        _log.info(msg)
        if on_progress:
            # Structured payload so the caller can drive a live progress bar
            # (processed/total/set) rather than parsing the message string.
            on_progress({"message": msg, **fields})

    result = MarketCapResult()
    companies = _load_companies(supabase)
    result.companies_scanned = len(companies)
    fx = _fx_rates_per_eur(supabase)
    today = _dt.date.today().isoformat()

    targets: list[dict] = []
    for c in companies:
        if only_missing and c.get("market_cap_eur") is not None:
            result.skipped_have_value += 1
            continue
        targets.append(c)
    if limit is not None:
        targets = targets[:limit]

    total = len(targets)
    emit(f"{len(companies)} companies; resolving market cap for {total} "
         f"(~{round(total * 1.5 / 60)} min at the 1.5s rate limit)…",
         processed=0, total=total, set=0)

    for i, c in enumerate(targets, 1):
        # Emit once per company (not every 50) so the caller sees real-time
        # progress. Done at the top so the count advances even for companies
        # we skip below (no symbol / no mktcap / no FX rate).
        emit(
            f"  …{i}/{total} ({result.set_count} set, {result.no_mktcap} no-mktcap, "
            f"{result.no_fx_rate} no-fx)",
            processed=i, total=total, set=result.set_count,
        )
        cid = int(c["company_id"])
        exch = (c.get("gurufocus_exchange") or {}) or {}
        symbol = _gf_symbol(c.get("gurufocus_ticker") or "", exch.get("exchange_code"))
        if not symbol:
            continue
        result.gurufocus_calls += 1
        try:
            mktcap_native_m, gf_name, _msg = _gurufocus_summary(symbol)
        except Exception as e:
            result.errors.append(f"cid={cid} ({symbol}) GF error: {type(e).__name__}: {e}")
            continue

        # Accumulate every field this company's summary lets us correct, then
        # write once. The name fix is independent of market cap — a company GF
        # can't price still gets its name corrected.
        updates: dict = {}

        # Name correction: GuruFocus is authoritative for what a (ticker,
        # exchange) listing actually IS, so a genuinely-different name (the
        # wrong company entirely, e.g. "TSMC" on a listing GF calls "Forside Co
        # Ltd") gets overwritten. Punctuation/spacing-only diffs are ignored.
        if gf_name and _name_key(gf_name) != _name_key(c.get("company_name")):
            updates["company_name"] = gf_name
            if len(result.renames) < 50:
                result.renames.append(f"cid={cid}: {c.get('company_name')!r} → {gf_name!r}")

        if mktcap_native_m is None:
            result.no_mktcap += 1
        else:
            cur = (exch.get("currency_code") or "EUR").upper()
            rate = fx.get(cur)
            if not rate:
                result.no_fx_rate += 1
            else:
                # mktcap is in millions of native currency → absolute EUR.
                native_abs = mktcap_native_m * 1_000_000.0
                updates["market_cap_native"] = native_abs
                updates["market_cap_currency"] = cur
                updates["market_cap_fx_rate"] = rate
                updates["market_cap_eur"] = native_abs / rate
                updates["market_cap_date"] = today

        if not updates:
            continue
        try:
            supabase.table("company").update(updates).eq("company_id", cid).execute()
            if "market_cap_eur" in updates:
                result.set_count += 1
            if "company_name" in updates:
                result.renamed += 1
        except Exception as e:
            result.errors.append(f"cid={cid} update failed: {type(e).__name__}: {e}")

    emit(f"Done. set={result.set_count} renamed={result.renamed} "
         f"no-mktcap={result.no_mktcap} no-fx={result.no_fx_rate} "
         f"errors={len(result.errors)}",
         processed=total, total=total, set=result.set_count)
    return result


def format_summary(r: MarketCapResult) -> str:
    lines = [
        f"Companies scanned:   {r.companies_scanned}",
        f"  Skipped (had val): {r.skipped_have_value}",
        f"  GuruFocus calls:   {r.gurufocus_calls}",
        f"  Set:               {r.set_count}",
        f"  Renamed:           {r.renamed}",
        f"  No mktcap:         {r.no_mktcap}",
        f"  No FX rate:        {r.no_fx_rate}",
        f"  Errors:            {len(r.errors)}",
    ]
    for rn in r.renames[:10]:
        lines.append(f"    {rn}")
    for e in r.errors[:10]:
        lines.append(f"    {e}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    only_missing = "--only-missing" in sys.argv
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])
    res = backfill_market_cap(
        supabase, only_missing=only_missing, limit=limit,
        on_progress=lambda d: print(d["message"]),
    )
    print("\n" + format_summary(res))

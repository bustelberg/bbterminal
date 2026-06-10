"""Backfill `company.market_cap_eur` + `market_cap_date` from GuruFocus.

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
    errors: list[str] = field(default_factory=list)


def _load_companies(supabase: Client) -> list[dict]:
    """Every company with ticker + exchange code + currency + current mktcap."""
    out: list[dict] = []
    offset = 0
    page = 1000
    for _ in range(50):
        resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, market_cap_eur, "
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


def _gurufocus_mktcap(symbol: str) -> tuple[float | None, str]:
    """Fetch `summary.company_data.mktcap` (native, millions) for a symbol."""
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    res = _api_request(_build_api_url(f"stock/{symbol}/summary"))
    if res.data is None:
        return None, res.log
    cd = ((res.data.get("summary") or {}).get("company_data") or {}) if isinstance(res.data, dict) else {}
    raw = cd.get("mktcap")
    try:
        v = float(raw)
        return (v if v > 0 else None), res.log
    except (TypeError, ValueError):
        return None, res.log


def backfill_market_cap(
    supabase: Client,
    *,
    only_missing: bool = False,
    limit: int | None = None,
    on_progress=None,
) -> MarketCapResult:
    def emit(msg: str) -> None:
        _log.info(msg)
        if on_progress:
            on_progress(msg)

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

    emit(f"{len(companies)} companies; resolving market cap for {len(targets)} "
         f"(~{round(len(targets) * 1.5 / 60)} min at the 1.5s rate limit)…")

    for i, c in enumerate(targets, 1):
        cid = int(c["company_id"])
        exch = (c.get("gurufocus_exchange") or {}) or {}
        symbol = _gf_symbol(c.get("gurufocus_ticker") or "", exch.get("exchange_code"))
        if not symbol:
            continue
        result.gurufocus_calls += 1
        try:
            mktcap_native_m, _msg = _gurufocus_mktcap(symbol)
        except Exception as e:
            result.errors.append(f"cid={cid} ({symbol}) GF error: {type(e).__name__}: {e}")
            continue
        if mktcap_native_m is None:
            result.no_mktcap += 1
            continue
        cur = (exch.get("currency_code") or "EUR").upper()
        rate = fx.get(cur)
        if not rate:
            result.no_fx_rate += 1
            continue
        # mktcap is in millions of native currency → absolute EUR.
        eur = (mktcap_native_m * 1_000_000.0) / rate
        try:
            supabase.table("company").update(
                {"market_cap_eur": eur, "market_cap_date": today}
            ).eq("company_id", cid).execute()
            result.set_count += 1
        except Exception as e:
            result.errors.append(f"cid={cid} update failed: {type(e).__name__}: {e}")
        if i % 50 == 0 or i == len(targets):
            emit(f"  …{i}/{len(targets)} processed "
                 f"({result.set_count} set, {result.no_mktcap} no-mktcap, {result.no_fx_rate} no-fx)")

    emit(f"Done. set={result.set_count} no-mktcap={result.no_mktcap} "
         f"no-fx={result.no_fx_rate} errors={len(result.errors)}")
    return result


def format_summary(r: MarketCapResult) -> str:
    lines = [
        f"Companies scanned:   {r.companies_scanned}",
        f"  Skipped (had val): {r.skipped_have_value}",
        f"  GuruFocus calls:   {r.gurufocus_calls}",
        f"  Set:               {r.set_count}",
        f"  No mktcap:         {r.no_mktcap}",
        f"  No FX rate:        {r.no_fx_rate}",
        f"  Errors:            {len(r.errors)}",
    ]
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
    res = backfill_market_cap(supabase, only_missing=only_missing, limit=limit, on_progress=print)
    print("\n" + format_summary(res))

"""Universe and per-company currency lookups.

`load_universe` produces the base DataFrame the backtester iterates
over. When a universe label + target_month is provided, it joins through
`universe_membership` so the returned `sector` column comes from the
snapshot (not the company table). Otherwise sector is None on every
row, and the caller is expected to supply sectors via `monthly_eligible`."""
from __future__ import annotations

import pandas as pd
from supabase import Client

from deps import IN_CHUNK_SIZE, chunked
from ._helpers import _query_with_retry


def load_company_asset_map(
    supabase: Client,
    company_ids: list[int],
) -> dict[int, tuple[int, str | None]]:
    """Map a GuruFocus company id to its Yahoo analysis id and quote currency.

    The momentum universe is still defined by ``company``/``universe_membership``;
    Yahoo bars live in the separate asset domain. ``asset_grid`` is the reviewed
    ISIN bridge between them. Rows without a resolved Yahoo instrument are left
    out rather than silently falling back to GuruFocus prices.
    """
    out: dict[int, tuple[int, str | None]] = {}
    for chunk in chunked(sorted(set(company_ids)), IN_CHUNK_SIZE):
        rows = (
            supabase.table("asset_grid")
            .select("company_id,analysis_id,currency,status")
            .in_("company_id", chunk)
            .eq("status", "ok")
            .execute()
            .data or []
        )
        for row in rows:
            cid, aid = row.get("company_id"), row.get("analysis_id")
            if cid is None or aid is None:
                continue
            # More than one execution can temporarily point at one company;
            # keep the first stable mapping rather than duplicating its series.
            out.setdefault(int(cid), (int(aid), row.get("currency")))
    return out


def load_universe(
    supabase: Client,
    *,
    universe_label: str | None = None,
    target_month: str | None = None,
) -> pd.DataFrame:
    """Load companies for backtesting.

    If universe_label and target_month are given, loads from universe_membership
    for that specific universe/month (with sector from the membership row).
    Otherwise loads all companies joined with their exchange info.

    Returns DataFrame with columns:
        company_id, company_name, gurufocus_ticker, gurufocus_exchange, sector, country
    """
    rows: list[dict] = []
    page_size = 1000
    offset = 0

    # Load companies with exchange info. Skip rows the pipeline has
    # marked `delisted_at` or `out_of_scope_at` — neither has fetchable
    # price/volume data and surfacing them would just clutter the
    # universe + the audit gap list.
    while True:
        resp = _query_with_retry(
            lambda o=offset: (
                supabase.table("company")
                .select("company_id, company_name, gurufocus_ticker, exchange_id, gurufocus_exchange:gurufocus_exchange(exchange_code, country:country(country_name))")
                .is_("delisted_at", "null")
                .is_("out_of_scope_at", "null")
                .range(o, o + page_size - 1)
                .execute()
            ),
            description="load_universe",
        )
        if not resp.data:
            break
        rows.extend(resp.data)
        if len(resp.data) < page_size:
            break
        offset += page_size

    if not rows:
        return pd.DataFrame(
            columns=["company_id", "company_name", "gurufocus_ticker", "gurufocus_exchange", "sector", "country"]
        )

    # Flatten the nested exchange/country join
    flat_rows = []
    for r in rows:
        exchange_info = r.get("gurufocus_exchange") or {}
        country_info = exchange_info.get("country") or {}
        flat_rows.append({
            "company_id": r["company_id"],
            "company_name": r["company_name"],
            "gurufocus_ticker": r["gurufocus_ticker"],
            "gurufocus_exchange": exchange_info.get("exchange_code"),
            "country": country_info.get("country_name"),
        })

    df = pd.DataFrame(flat_rows)

    # If a universe is specified, join with universe_membership for sector.
    # Fixed-basket model: always use the universe's LATEST stored month (any
    # `target_month` argument is ignored — universes are frozen snapshots now).
    if universe_label:
        # Get universe_id
        u_resp = supabase.table("universe").select("universe_id").eq("label", universe_label).limit(1).execute()
        if u_resp.data:
            universe_id = u_resp.data[0]["universe_id"]
            lm = (
                supabase.table("universe_membership")
                .select("target_month")
                .eq("universe_id", universe_id)
                .order("target_month", desc=True)
                .limit(1)
                .execute()
            )
            target_month = lm.data[0]["target_month"] if lm.data else target_month
        if u_resp.data and target_month:
            # Load membership rows
            m_rows: list[dict] = []
            m_offset = 0
            while True:
                m_resp = _query_with_retry(
                    lambda o=m_offset: (
                        supabase.table("universe_membership")
                        .select("company_id, sector, universe_ticker")
                        .eq("universe_id", universe_id)
                        .eq("target_month", target_month)
                        .range(o, o + page_size - 1)
                        .execute()
                    ),
                    description="load_universe_membership",
                )
                if not m_resp.data:
                    break
                m_rows.extend(m_resp.data)
                if len(m_resp.data) < page_size:
                    break
                m_offset += page_size

            if m_rows:
                membership_df = pd.DataFrame(m_rows)
                df = df.merge(membership_df[["company_id", "sector"]], on="company_id", how="inner")
                df = df.dropna(subset=["sector"]).reset_index(drop=True)
                return df

    # Fallback: no sector info available, return without sector filtering
    df["sector"] = None
    return df


def load_company_currency(
    supabase: Client,
    company_ids: list[int],
) -> dict[int, str | None]:
    """Load the trading currency for each company via its exchange.

    Returns {company_id: currency_code}. Companies with no exchange resolve
    to None and won't be FX-converted.
    """
    if not company_ids:
        return {}

    return {company_id: currency for company_id, (_aid, currency) in
            load_company_asset_map(supabase, company_ids).items()}

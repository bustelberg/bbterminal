"""Persist the AIRS model portfolios (+ their positions) so /portfolios is a DB read.

The scrape is expensive — a paginated HTML walk plus one XLS per portfolio — and it used to
run on every page visit. Now the scan WRITES here and the page READS from here; "Rescan" is
the explicit refresh.

Storing the positions is free: the holdings count already downloads each portfolio's XLS to
count it. And the positions are the valuable half — `isin` is the exact join into
`asset_execution` that the AIRS *holdings* sheet never gave us (it carries only a fund name).
"""
from __future__ import annotations

from datetime import datetime, timezone

from deps import IN_CHUNK_SIZE, supabase


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_portfolios(rows: list[dict]) -> None:
    """Upsert the list. Only the columns the LIST page knows about — it says nothing about
    positions, so it must not touch `positions_*` (that would wipe a count we already have)."""
    if not rows:
        return
    payload = [{
        "id": r["id"],
        "name": r["name"],
        "truncated": bool(r.get("truncated")),
        "omschrijving": r.get("omschrijving") or None,
        "portfolio_type": r.get("fixed") or None,
        "fixed_datum": r.get("fixed_datum") or None,
        "scanned_at": _now(),
    } for r in rows]
    for i in range(0, len(payload), IN_CHUNK_SIZE):
        supabase.table("airs_model_portfolio").upsert(
            payload[i:i + IN_CHUNK_SIZE], on_conflict="id").execute()


def save_positions(portfolio_id: int, datum: str | None, rows: list[dict],
                   dates: list[str] | None = None) -> None:
    """Replace this portfolio's stored composition with what AIRS just gave us.

    ONE snapshot per portfolio — the newest that actually has rows. We deliberately do not
    accumulate history here: AIRS still serves any past date on demand (the date picker hits
    it live), and the stored copy exists to make the page instant, not to be an archive.

    So it is delete-ALL-then-insert, not an upsert. A position that VANISHED from the model
    has to actually disappear; an upsert would leave it behind for ever, and a stale holding
    that looks current is worse than no holding at all.

    `positions_scanned_at` is what separates "counted, and it holds nothing" (a real, empty
    fixed model — exactly one portfolio is like this) from "never counted". It is stamped
    even when `rows` is empty, because an empty answer IS an answer.
    """
    (supabase.table("airs_model_portfolio_position")
     .delete().eq("portfolio_id", portfolio_id).execute())

    payload = []
    for r in rows:
        # A NaN ISIN str()s to the literal "nan", which is TRUTHY — the trap
        # `_parse_positions_xls` exists to stop. Belt and braces: never store one.
        isin = r.get("ISINCode")
        isin = str(isin).strip() if isin else None
        if isin and isin.lower() == "nan":
            isin = None
        payload.append({
            "portfolio_id": portfolio_id,
            "datum": datum or "",
            "isin": isin or None,
            "fonds": (str(r["Fonds"]).strip() if r.get("Fonds") else None),
            "percentage": (float(r["Percentage"]) if r.get("Percentage") is not None else None),
            "valuta": (str(r["valuta"]).strip() if r.get("valuta") else None),
            "categorie": (str(r["Beleggingscategorie"]).strip()
                          if r.get("Beleggingscategorie") else None),
            "sector": (str(r["Beleggingssector"]).strip()
                       if r.get("Beleggingssector") else None),
            "regio": (str(r["regio"]).strip() if r.get("regio") else None),
        })

    for i in range(0, len(payload), IN_CHUNK_SIZE):
        supabase.table("airs_model_portfolio_position").insert(
            payload[i:i + IN_CHUNK_SIZE]).execute()

    # `positions_datum` is set ONLY when AIRS actually gave us a composition. Zero rows means
    # every candidate date came back empty — i.e. the dropdown offered nothing but its
    # "today" placeholder — so there is NO dated composition on record. Leaving the datum
    # NULL is what lets the view say `no_snapshot` instead of claiming "holds 0 instruments",
    # which is a fact we did not learn. (Measured on BUS_DUTD_DEF_AFS + EuropaTopSelect OFF FX.)
    (supabase.table("airs_model_portfolio").update({
        "positions_datum": (datum or None) if rows else None,
        # The dates AirSPMS offers. Kept so the cached response can still populate the date
        # picker — otherwise it would collapse to the one snapshot we hold and quietly imply
        # no others exist.
        "positions_dates": dates or None,
        "positions_scanned_at": _now(),
        "positions_error": None,
    }).eq("id", portfolio_id).execute())


def save_positions_error(portfolio_id: int, error: str) -> None:
    """We asked and it broke. Record THAT — do not stamp `positions_scanned_at`, or the row
    would read as "counted: 0 holdings", which is a fact we did not learn."""
    (supabase.table("airs_model_portfolio").update({"positions_error": error[:500]})
     .eq("id", portfolio_id).execute())


def load_portfolios() -> list[dict]:
    """The stored grid — `holdings` is DERIVED by the view from the positions, so it can
    never disagree with them."""
    return (supabase.table("airs_model_portfolio_grid")
            .select("*").order("name").execute().data or [])


def load_positions(portfolio_id: int) -> dict | None:
    """The cached composition, in the SAME shape the live fetch returns — or None if we have
    never stored one (so the caller can fall back to AIRS).

    Shaped like `airs_scanner.fetch_portfolio_positions_sync`'s raw dict (AIRS's own column
    names), so `_shape_positions` can consume either without caring where it came from. One
    code path for the response means the cached answer cannot drift from the live one.

    A portfolio that we scanned and found to have NO dated composition returns a real, empty
    answer — not None. "We looked and there is nothing" is a cached fact; re-scraping AIRS to
    rediscover it every time would be the whole bug this cache exists to fix.
    """
    p = (supabase.table("airs_model_portfolio")
         .select("id,name,positions_datum,positions_dates,positions_scanned_at")
         .eq("id", portfolio_id).limit(1).execute().data or [])
    if not p or not p[0].get("positions_scanned_at"):
        return None                                   # never scanned — nothing to serve
    row = p[0]

    stored = (supabase.table("airs_model_portfolio_position")
              .select("*").eq("portfolio_id", portfolio_id).execute().data or [])
    return {
        "portfolio": row["name"],
        "portfolio_id": row["id"],
        "datum": row.get("positions_datum"),
        "dates": row.get("positions_dates") or [],
        "cached_at": row.get("positions_scanned_at"),
        # Back to AIRS's column names — see the docstring.
        "rows": [{
            "Fonds": r.get("fonds"),
            "ISINCode": r.get("isin"),
            "Percentage": r.get("percentage"),
            "valuta": r.get("valuta"),
            "Beleggingscategorie": r.get("categorie"),
            "Beleggingssector": r.get("sector"),
            "regio": r.get("regio"),
        } for r in stored],
    }

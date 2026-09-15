"""The one table: a portfolio, by the name you gave it, on AIRS's own numbers.

WHAT IT JOINS, AND WHY EACH SIDE IS THERE
    A portfolio exists in AIRS as TWO rows that share nothing but a strategy:

        the Fixed one (`_FX`/`_AFS`)   weights + ISINCode + your nickname — AIRS values none of it
        the Dynamic one (`_DYN`)       the real book: quantities, EUR values, returns — NO ISIN

    Measured: of 58 Fixed portfolios with a composition and 31 AIRS-valued Dynamic ones, the
    overlap is ZERO. Neither is the portfolio; the pair is. This composes them into one row so a
    reader does not have to hold two tables in their head and a naming convention between them.

    NAME comes from the Fixed side (`display_name` — the nickname; all 28 linked pairs have one).
    EVERY NUMBER comes from the Dynamic side, because AIRS is the system of record for what a
    book made and we are not.

⚠ THE PAIRING IS MOSTLY UNCONFIRMED, AND THE ROW SAYS SO. 27 of the 28 links are `guess` — an
    exact stem match nobody has approved (`_airs_account_links`). A guess is not a small doubt
    here: the risk variants of a strategy hold the SAME instruments (BUS_FTS_Bepoff/DEF/NEU_AFS
    share 27 of 27 ISINs), so a mis-pairing shows a book's real money under another strategy's
    name and nothing on the row would look wrong. `link_source` rides along for that reason.

⚠ AN UNLINKED DYNAMIC PORTFOLIO IS NOT DROPPED. It has AIRS numbers and no nickname — 23 of the
    51, the benchmarks among them. Hiding them would make this table quietly disagree with the
    Dynamic table it summarises; they appear with the AIRS name and no ISINs behind them.
"""
from __future__ import annotations

import asyncio
import re

from deps import supabase


_MANAGEMENT_GROUPS = {
    "busdefensiefdyn": "bustelberg",
    "busneutraaldyn": "bustelberg",
    "busbepoffensiefdyn": "bustelberg",
    "busoffensiefdyn": "bustelberg",
    "topsdefbehdyn": "toppenberg",
    "topsbeoffbehdyn": "toppenberg",
    "topsoffbehdyn": "toppenberg",
}
_SINGLE_RISK_PROFILE_ACCOUNTS = {
    "aitopselectieoffdyn",
    "aztopselectiedyn",
    "ditopselectieoffdyn",
    "europatopselectoffdyn",
    "startopselectieoffdyn",
    "vtopselectieoffdy",
    "busrisbepoffklafsdy",
    "dealmakerstopseloffdyn",
    "tolpoortenselectoffdyn",
}
_RISK_PROFILE_SUFFIX = re.compile(r"\s+(?:beperkt\s+offensief|offensief|neutraal|defensief)\s*$", re.I)


def _management_group(account_name: str | None) -> str:
    """The dashboard collection for an AIRS dynamic portfolio."""
    key = re.sub(r"[^a-z0-9]+", "", (account_name or "").casefold())
    return _MANAGEMENT_GROUPS.get(key, "topselecties")


def _management_name(name: str | None, group: str, account_name: str | None) -> str | None:
    """Remove a redundant risk suffix only from single-variant building blocks."""
    key = re.sub(r"[^a-z0-9]+", "", (account_name or "").casefold())
    if group != "topselecties" or key not in _SINGLE_RISK_PROFILE_ACCOUNTS or not name:
        return name
    return _RISK_PROFILE_SUFFIX.sub("", name).strip() or name


def _nicknames() -> dict[str, str]:
    """The human-chosen name per account (lower-cased key) — `{}` when we cannot read them.

    ⚠ A NICKNAME IS A DECORATION; A MISSING ONE MUST NOT COST THE WHOLE PAGE. `airs_account_display_name`
    is the NEWEST table this endpoint touches, and the hosted databases are migrated by hand
    (`npx supabase db push`), so there is always a window where deployed code is ahead of the
    schema it reads. Unguarded, that window turned the entire portfolios overview into a 500 —
    and, because an unhandled exception escapes the CORS middleware (see
    `_error_middleware.cors_safe_errors`), the browser reported it as *"No 'Access-Control-Allow-Origin'
    header"*, which sends you looking at CORS config for a schema problem.

    Every other recently-added read on this path already fails open for exactly this reason —
    `_airs_accounts._hidden_accounts`, `_live_accounts`, `_missing_reports`. This was the one
    holdout. Falling back costs the nicknames only: the name chain drops to the model's
    `display_name`, then AIRS's own code, which is what every row showed before this table existed.
    """
    try:
        rows = (supabase.table("airs_account_display_name")
                .select("portefeuille,display_name").limit(2000).execute().data or [])
    except Exception:  # noqa: BLE001 — a missing table must not blank the page
        return {}
    return {(r["portefeuille"] or "").strip().lower(): r["display_name"]
            for r in rows if r.get("display_name")}


def _overview_name(account_name: str | None, account_nicknames: dict[str, str],
                   direct_model_nicknames: dict[str, str],
                   paired_model: dict | None) -> tuple[str | None, bool]:
    """The reader-facing name for one AIRS account and whether it was explicitly chosen.

    The Links editor saves a nickname against an AIRS *model* name.  An account can itself have a
    model row (``AITopSelectie OFF DYN``) while its analysis pairing points to the sibling Fixed
    model (``AITopSelectie OFF FX``).  Looking only through that pairing discarded the nickname
    the editor had just saved.  An exact account-name model is the account's own identity and
    must therefore win before the paired-model fallback.
    """
    raw = (account_name or "").strip()
    key = raw.lower()
    if account_nicknames.get(key):
        return account_nicknames[key], True
    # The checked-in AIRS map covers the shared strategy vocabulary (and works before an admin has
    # touched Links in a fresh database). It is the reviewed source of truth for these standard
    # strategies, so it intentionally beats an older model nickname; a per-account name above is
    # still available for a genuinely bespoke account.
    from routers._airs_strategy_map import nickname_for  # noqa: PLC0415
    if mapped := nickname_for(raw):
        return mapped, True
    if direct_model_nicknames.get(key):
        return direct_model_nicknames[key], True
    if (paired_model or {}).get("display_name"):
        return paired_model["display_name"], False
    return account_name, False


def list_overview() -> list[dict]:
    """One row per AIRS Dynamic portfolio, named by the Fixed portfolio it runs."""
    from ._airs_account_links import list_account_links  # noqa: PLC0415  (circular at import)
    from ._airs_accounts import list_accounts  # noqa: PLC0415

    links = {a["portefeuille"]: a for a in list_account_links()["accounts"]}
    # ⚠ THE ACCOUNT'S OWN NICKNAME BEATS THE MODEL'S. A human typed it for THIS book; the model's
    # `display_name` names a different object and is only borrowed when nothing better exists —
    # which is also why a book paired with no model could not be named at all before.
    nicknames = _nicknames()
    models = {m["id"]: m for m in (supabase.table("airs_model_portfolio")
                                   .select("id,name,display_name,omschrijving,portfolio_type")
                                   .limit(500).execute().data or [])}
    # Nicknames saved in Links are keyed by a model's AIRS name.  Keep this separate from the
    # account-nickname table above: a model nickname decorates only an account with the exact same
    # AIRS name, never every account that happens to be paired to that strategy.
    direct_model_nicknames = {
        (m.get("name") or "").strip().lower(): m["display_name"]
        for m in models.values() if (m.get("name") or "").strip() and m.get("display_name")
    }
    # ⚠ THE `airs_model_portfolio_position` READ THAT USED TO SIT HERE IS GONE (2026-08-11). It
    # counted positions per model into a dict that NOTHING READ — the `isins` column moved to the
    # account's own count (see below) and only the *use* was deleted, leaving the query behind. It
    # cost one round trip and 982 rows on every single page load, invisibly, because a read whose
    # result is discarded cannot produce a wrong answer — only a slow one. Measured: the overview
    # went 13 → 12 round trips and 3,760 → 2,778 rows.
    out: list[dict] = []
    for a in list_accounts():
        link = links.get(a["portefeuille"]) or {}
        m = models.get(link.get("model_portfolio_id")) if link.get("model_portfolio_id") else None
        display_name, name_is_custom = _overview_name(
            a.get("portefeuille"), nicknames, direct_model_nicknames, m)
        management_group = _management_group(a.get("portefeuille"))
        display_name = _management_name(display_name, management_group, a.get("portefeuille"))
        out.append({
            # The name a human gave it. Falls back to AIRS's code rather than to a blank: an
            # unlinked book is still a book, and a nameless row is unreadable.
            # Precedence: this book's nickname > the model's display name > AIRS's own code. Every
            # step down is a FALLBACK, never a preference.
            "name": display_name,
            "management_group": management_group,
            # True when a human named this row, so the UI can show it as chosen rather than derived.
            "name_is_custom": name_is_custom,
            "description": (m or {}).get("omschrijving"),
            "dynamic_portefeuille": a["portefeuille"],
            "fixed_name": (m or {}).get("name"),
            "fixed_portfolio_id": (m or {}).get("id"),
            "fixed_type": (m or {}).get("portfolio_type"),
            # ⚠ THE ACCOUNT'S OWN ISIN COUNT — see `list_accounts`. It was the paired MODEL's
            # position count, which is a different object and absent entirely for an unpaired book:
            # BUS_WTS_StMerken_Dyn showed "—" while holding 22 ISINs. `positions` is still read for
            # nothing else, so it goes with it.
            "isins": a.get("isins"),
            "link_source": link.get("source") or "none",
            "link_reason": link.get("reason"),
            # --- AIRS's own, all of it, all the year's. Never recomputed here. ---
            "as_of": a.get("as_of"),
            "periode": a.get("periode"),
            "months": a.get("months"),
            "ytd_pct": a.get("ytd_pct"),
            "latest_month_pct": a.get("latest_month_pct"),
            "price_result_eur": a.get("price_result_eur"),
            "income_eur": a.get("income_eur"),
            "investment_result_eur": a.get("investment_result_eur"),
            "deposits_eur": a.get("deposits_eur"),
            "withdrawals_eur": a.get("withdrawals_eur"),
            "begin_value_eur": a.get("begin_value_eur"),
            "end_value_eur": a.get("end_value_eur"),
            "holdings": a.get("holdings"),
            "reconciles": a.get("reconciles"),
            # Carried straight through from `list_accounts` — the overview is a composition of that
            # row with its Fixed pairing, so a caveat about the row's freshness belongs on it.
            "missing_reports": a.get("missing_reports") or [],
            # ⚠ THE PAIR IS THE POINT — see `_airs_accounts._fetched_at`. `as_of` is when AIRS
            # VALUED the book; this is when WE last read it. Without both, an old valuation reads
            # as our staleness and the badge sends the reader to a button that cannot help.
            "fetched_at": a.get("fetched_at"),
            "residual_eur": a.get("residual_eur"),
        })
    # Linked first (they are the ones with a name), then by that name.
    out.sort(key=lambda r: (r["fixed_name"] is None, (r["name"] or "").lower()))
    return out


async def list_overview_async() -> list[dict]:
    return await asyncio.to_thread(list_overview)

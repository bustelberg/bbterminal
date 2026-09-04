"""Pairing an AIRS ACCOUNT to the MODEL it runs.

WHY THIS CANNOT BE DERIVED, AND MUST BE DECIDED
    The ISINs and the money live on opposite sides of a wall (see the migration
    20260717020000): a model has weights + ISINCode and AIRS values none of it; an account has
    real returns and no ISIN at all. Overlap: zero, of 58 models and 31 accounts. Pairing them
    is the only bridge — and neither side carries a key for it.

    ⚠ THE HOLDINGS CANNOT IDENTIFY THE MODEL, WHICH IS THE OPPOSITE OF WHAT YOU EXPECT.
    Matching on what a portfolio holds is the obvious escape from unreliable names, and it is
    exactly useless here: BUS_FTS_Bepoff_AFS / BUS_FTS_DEF_AFS / BUS_FTS_NEU_AFS hold the SAME
    27 ISINs (measured: 27 of 27 shared, all three pairs), and BUS_FTS_OFF_AFS's 25 are a
    subset of each. One strategy, four risk weightings, one instrument list. A content matcher
    scores all four 100 and picks whichever it enumerated first — confidently, and wrong three
    times in four.

    So the NAME is the only discriminator that exists. And it is four conventions and a typo
    (see `_stem`). Hence: guessed conservatively, decided by a human, stored.

HOW THE GUESS EARNS THE RIGHT TO BE SHOWN
    `_stem` removes ONLY the venue suffixes and then the remainder must match EXACTLY. That is
    what makes it safe rather than merely accurate: `busftsdef` and `busftsneu` are different
    strings, so the matcher physically cannot confuse two risk profiles. When the convention is
    one it does not know (`BUS_BM_AAND...` -> `BUS_BM_AAN..._d`, where AIRS mangles the word
    itself), it produces NO guess instead of a near one. A refusal is a cheap manual link; a
    plausible wrong guess is a portfolio measured against someone else's strategy.
"""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

from deps import supabase
from routers._airs_ref import models as ref_models, positions as ref_positions

# Suffixes that name the VENUE/variant of a portfolio rather than the strategy. Stripped from
# both sides before comparing.
#
# ⚠ `off` IS NOT IN HERE AND MUST NOT BE. It looks like a suffix and is not — it is
# "Offensief", the RISK PROFILE, and it is the only thing separating `AITopSelectie OFF FX`
# from a hypothetical defensive sibling. Strip it and every profile of a strategy collapses
# onto one stem, which is the wrong-link bug this module exists to prevent. Same for `def`,
# `neu`, `bepoff`, `beh`, `afs`... — except `afs`, which is genuinely both (see below).
_VENUE_SUFFIXES = (
    "dyn",      # the live account
    "fx",       # the model
    "afs",      # ⚠ BOTH a model suffix (BUS_FTS_OFF_AFS) and part of an account's stem
                #   (BUS_MTS_OFF_AFS_DYN). Stripping repeatedly from both sides makes the two
                #   conventions agree: ...offafsdyn -> ...offafs -> ...off, and ...offafs ->
                #   ...off. Safe only because the comparison is exact afterwards.
    "dy",       # VTopSelectie OFF DY — missing its N. A real name, not a truncation (19 chars,
                #   against AIRS's 24-char cap).
    "d",        # BUS_BM_..._d
)


def _norm(name: str) -> str:
    """Case- and separator-insensitive. AIRS mixes both freely — `BUS_BepOffensief_Dyn` and
    `BUS_Bep_offensief_FX` are the same strategy written two ways."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _stem(name: str) -> str:
    """The strategy, with the venue suffixes taken off. Applied to BOTH sides.

    Repeated, because the conventions nest: `BUS_MTS_OFF_AFS_DYN` -> `busmtsoffafs` ->
    `busmtsoff`, which is where `BUS_MTS_OFF_AFS` also lands. Without the repeat, the account
    keeps an `afs` its model has already lost and the pair misses.
    """
    s = _norm(name)
    changed = True
    while changed and s:
        changed = False
        for suf in _VENUE_SUFFIXES:
            if s.endswith(suf) and len(s) > len(suf):
                s, changed = s[: -len(suf)], True
                break
    return s


def guess_model(account: str, models: list[dict]) -> tuple[dict | None, str]:
    """(the model this account is running, why) — or (None, why not).

    EXACT on the stem, never fuzzy, and never a best-of. Two models on one stem is an
    ambiguity, not a tie to break: they differ by something `_stem` threw away, and picking
    one would be a coin flip dressed as an answer.

    Two gates below are not tie-breakers on the name — they are what makes the name usable at
    all, and both were caught by the first run over real data.
    """
    stem = _stem(account)
    if not stem:
        return None, "no name to match on"
    hits = [m for m in models if _stem(m["name"]) == stem]
    if not hits:
        return None, f"no model has the stem '{stem}'"

    # ⚠ THE PERFECT NAME MATCH IS THE PORTFOLIO ITSELF, AND IT IS A CYCLE. `TOPS_AZTS_L` is
    # both an account AND a one-line model row, so it matched ITSELF at a perfect score —
    # "this account runs itself", which is no information wearing the look of certainty. The
    # same shape `_airs_portfolio_links` hit: there the closest string to a certificate's name
    # was the wrapper holding it.
    hits = [m for m in hits if _norm(m["name"]) != _norm(account)]
    if not hits:
        return None, "the only stem match is the account itself"

    # ⚠ A ONE-POSITION MODEL IS A WRAPPER, NOT A STRATEGY. It holds a single Leonteq AMC
    # certificate standing in for another portfolio; linking an account to it says nothing
    # about what the account is running. Kept OUT of the guess but left IN the pick-list
    # below, because a human may know something we do not — the single-instrument BUS_BM_*
    # benchmark models are also one position each, and they are real targets.
    strategies = [m for m in hits if (m.get("positions") or 0) > 1]
    if not strategies:
        return None, "the only stem match holds one position — a wrapper, not a strategy"

    if len(strategies) == 1:
        return strategies[0], f"exact stem match on '{stem}'"
    names = ", ".join(sorted(m["name"] for m in strategies))
    return None, f"ambiguous — {len(strategies)} models share the stem '{stem}': {names}"


def _models() -> list[dict]:
    """The models a link may point at: the ones with a composition. A model with no positions
    is not a strategy an account can be running — it is a row we scraped and nothing more.

    ⚠⚠ THE POSITION READ MUST PAGE, AND A TRUNCATION HERE SILENTLY UNPAIRS PORTFOLIOS.
        `.limit(20000)` is not a bound the server honours — `db-max-rows` is, and it is **1,000
        on Supabase cloud** against 10,000 locally. Every model whose position rows fall past
        the cut counts as ZERO positions, is dropped by the filter on the last line, and then
        matches no account: `guess_model` reports "no stem match", the pairing vanishes, and
        Analyse silently falls back to an unpaired basket for a book that is perfectly fine.
        Nothing raises; the page just quietly loses features per portfolio, and WHICH ones
        depends on row order.

        Measured 2026-08-03: this table holds **919** rows — under the cloud cap, but 92% of the
        way to it, and it grows by ~20 rows every time a model is scanned. It is the same trap
        that made `_year_perf` serve June's return in production while local served July's, one
        table over, waiting for the next scan to trip it.
    """
    # ⚠ WAS ITS OWN PAGED READ, ORDERED ON `(portfolio_id, isin)` — WHICH IS NOT UNIQUE. Measured:
    # this table holds one genuine duplicate pair (a model listing the same instrument at two
    # weights), and Postgres promises nothing about tied rows across separate LIMIT/OFFSET
    # queries, so a page boundary landing inside that tie could serve the row twice or never.
    # `_airs_ref` pages on the PRIMARY KEY and is shared with every other reader, so this is both
    # a correctness fix and two round trips removed.
    rows = ref_models()
    # ⚠ COUNTS EVERY POSITION ROW, NOT ONLY ISIN-BEARING ONES — deliberately NOT
    # `_airs_ref.position_counts()`, which excludes rows without an ISIN because the /portfolios
    # grid counts *instruments* and a cash line is not one. The two are genuinely different
    # numbers here: 31 position rows have no ISIN, spread over 30 portfolios, so swapping in the
    # grid's definition would quietly change this list's `positions` for more than half of them.
    counts: dict[int, int] = {}
    for p in ref_positions():
        counts[p["portfolio_id"]] = counts.get(p["portfolio_id"], 0) + 1
    return [{**m, "positions": counts.get(m["id"], 0)} for m in rows if counts.get(m["id"], 0)]


def _stored() -> dict[str, dict]:
    rows = (supabase.table("airs_account_model_link")
            .select("id,portefeuille,model_portfolio_id,note").limit(500).execute().data or [])
    return {r["portefeuille"].lower(): r for r in rows}


def list_account_links() -> dict:
    """Every AIRS-valued account, with the model it runs — decided, guessed, or neither.

    The accounts come from `airs_performance` (the front-office scrape), NOT from
    `airs_model_portfolio`: 18 of the 51 accounts have no row in the models list at all, and
    those are precisely the ones a models-table-driven view would never show.

    ⚠⚠ IT IS A FLEET FACT, AND THE ANALYSE PATH ASKS FOR IT FIVE TIMES. This answers nothing about
    any one portfolio — it is the whole account/model pairing table — and the callers want ONE row
    out of it, which they find with a linear `next(...)` over the rebuilt list. There are five such
    lookups in `_airs_portfolio_analysis` alone, plus `_airs_attribution_basis` and
    `_airs_holding_timing` on the same request. So it is a LEG in exactly the sense
    `_analysis_cache` means: same fingerprint, same staleness guarantee, no dependence on which
    book is open.

    ⚠ `read_cache` ALREADY ABSORBS THE ROUND TRIPS, and that is why this is worth only what it is
    worth. Measured inside one `read_cache` block: the first call 1,390 ms, each repeat **23-31 ms**
    — the HTTP is served from the memo and what remains is re-PARSING the ~150 KB `airs_performance`
    body and re-running `_year_perf`'s monthly reduction over 1,968 rows. Caching the assembled
    result removes ~100 ms per modal open; it is not the 2.5s that counting bare calls suggests,
    and anyone re-measuring this outside a `read_cache` block will get that wrong number.

    ⚠ THE RESULT IS SHARED BY REFERENCE AND MUST BE TREATED AS IMMUTABLE — the leg store's standing
    rule. Audited at the time of writing: all fourteen call sites READ (`next(...)` lookups, dict
    and list comprehensions that build new objects); not one assigns into a returned row. If you
    add a caller, read — do not decorate.
    """
    from common import read_cache  # noqa: PLC0415

    # ⚠ ONLY INSIDE A DECLARED-EXPENSIVE UNIT OF WORK, and never across a write — the same gate as
    # `_airs_ref._paged`. Outside one, the leg store would cost a FINGERPRINT (one
    # `pg_stat_user_tables` COPY) to answer a question nobody is about to ask again, making a lone
    # caller slower by a round trip. `wrote()` opts a writer out so it cannot be handed a snapshot
    # from before its own write.
    if read_cache.active() is None or read_cache.wrote():
        return _list_account_links_uncached()
    from routers._analysis_cache import leg  # noqa: PLC0415  (cycle at module level)
    return leg(("account-links",), _list_account_links_uncached)


def _list_account_links_uncached() -> dict:
    """The assembly itself. Split out so the memo above wraps it without duplicating any of it."""
    from routers._airs_accounts import _year_perf  # noqa: PLC0415  (circular at module level)

    perf = _year_perf()
    models = _models()
    stored = _stored()
    by_id = {m["id"]: m for m in models}

    out = []
    for name, p in sorted(perf.items(), key=lambda kv: kv[0].lower()):
        link = stored.get(name.lower())
        g, why = guess_model(name, models)
        # A stored row always wins, INCLUDING one that stores NULL — that is a human saying
        # "not a model", and letting the guess speak over it would make the decision
        # un-clearable.
        decided = link is not None
        model_id = link["model_portfolio_id"] if decided else (g["id"] if g else None)
        m = by_id.get(model_id) if model_id else None
        out.append({
            "portefeuille": name,
            "ytd_pct": p.get("cumulatief_rendement"),
            "months": p.get("months"),
            "model_portfolio_id": model_id,
            "model_name": m["name"] if m else None,
            "model_positions": m["positions"] if m else None,
            "source": "manual" if decided else ("guess" if g else "none"),
            "reason": link.get("note") if decided else why,
        })
    return {
        "accounts": out,
        # The pick-list for the dropdown. Sent once, not per row.
        "models": sorted(
            [{"id": m["id"], "name": m["name"], "positions": m["positions"]} for m in models],
            key=lambda m: m["name"].lower()),
    }


def set_account_link(portefeuille: str, model_portfolio_id: int | None, note: str | None = None) -> dict:
    """Record a human's decision. `model_portfolio_id=None` stores "explicitly not a model",
    which is a different fact from having no row at all (that one means "nobody has looked").

    Select-then-update/insert rather than an upsert, because the uniqueness is on
    `lower(portefeuille)` — an EXPRESSION index, which `ON CONFLICT (portefeuille)` cannot use
    ("no unique or exclusion constraint matching the ON CONFLICT specification"). Same shape as
    the airs_model_portfolio_link writer.
    """
    payload = {"portefeuille": portefeuille, "model_portfolio_id": model_portfolio_id,
               "note": note, "updated_at": datetime.now(timezone.utc).isoformat()}
    existing = _stored().get(portefeuille.lower())
    if existing:
        (supabase.table("airs_account_model_link").update(payload)
         .eq("id", existing["id"]).execute())
    else:
        supabase.table("airs_account_model_link").insert(payload).execute()
    return {"portefeuille": portefeuille, "model_portfolio_id": model_portfolio_id}


def clear_account_link(portefeuille: str) -> dict:
    """Forget the decision entirely — the guess speaks again. NOT the same as storing NULL.

    Deletes by id. ⚠ NOT by `ilike(portefeuille)`: `_` is a single-character WILDCARD in LIKE
    and AIRS names are full of them, so `BUS_BM_AAN_kw_EUR_2026_d` would match rows it has
    nothing to do with — and this is a delete.
    """
    existing = _stored().get(portefeuille.lower())
    if existing:
        supabase.table("airs_account_model_link").delete().eq("id", existing["id"]).execute()
    return {"portefeuille": portefeuille, "cleared": bool(existing)}


async def list_account_links_async() -> dict:
    return await asyncio.to_thread(list_account_links)


async def set_account_link_async(p: str, m: int | None, note: str | None = None) -> dict:
    return await asyncio.to_thread(set_account_link, p, m, note)


async def clear_account_link_async(p: str) -> dict:
    return await asyncio.to_thread(clear_account_link, p)

"""Link a POSITION to the model portfolio it actually is.

Some holdings in an AIRS model are not instruments at all — they are other model portfolios,
wrapped as a Leonteq actively-managed certificate so they can be held like a security:

    CH1381833321   "Star Selection Index"          held by 11 models
    CH1550442797   "MomentumTopSelectie Index"     held by  5 models
    CH1571717235   "AzieTopSelectie Index"         held by  5 models

Every one of them is a CH ISIN that Yahoo cannot price (`asset_execution.status='not_found'`,
and it is not a mapping bug — there is no listing for a structured product), so today they sit
in the positions table as a dead row: no price, no return, and their weight comes out of the
coverage denominator. The link is what lets us look THROUGH the certificate to the model that
defines it.

WHY THE OBVIOUS MATCHER PICKS THE WRONG PORTFOLIO
    Name similarity alone answers "Star Selection Index" with **TOPS_STS_L**, whose description
    is literally "StarTopSelectie" — the closest string in the whole list. It is also the one
    answer that is definitely wrong: TOPS_STS_L *holds the certificate at 100%*. It is a
    WRAPPER, not the strategy. Following that link walks straight back to the row you started
    from. The portfolio the certificate actually tracks is `StarTopSelectie OFF FX`, which holds
    24 real stocks and whose name scores WORSE.

    So the gates below are not tie-breakers on top of a name score — they are what makes the
    name score usable at all. Rank first, and the wrapper wins every time.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from rapidfuzz import fuzz

# Boilerplate that every strategy name carries and which therefore DISCRIMINATES NOTHING.
# "StarTopSelectie Offensief fixed" and "MerkenTopSelectie offensief AFS" share everything but
# the one word that matters. Strip it and what remains is the strategy stem: star, merken, ai,
# vastgoed, europa, azie, momentum, dividend, familie.
#
# ⚠ THESE MUST COME OFF AS SUBSTRINGS, NOT TOKENS. "Familietopselectie" and "Merkentopselectie"
# are written as ONE word, so a tokenizer sees an opaque token and removes nothing — the noise
# survives inside it and then dominates the comparison. Longest-first so "topselectie" is taken
# before "selectie" can carve it in half.
_NOISE_SUBSTR = ["topselectie", "topselect", "topsel", "selectie", "selection", "index"]

# Whole words that survive as separate tokens: product-line prefixes, risk profiles, wrappers.
_NOISE_WORDS = {
    "bus", "tops", "beh", "afs", "wts", "mts", "fts", "sts", "dts", "ets", "l",
    "fx", "dyn", "dynamisch", "dynamic", "fixed", "fix", "off", "offensief", "offensive",
    "defensief", "neutraal", "beperkt", "bep", "maatwerk", "pens", "pensioen",
    "fund", "fonds", "certificate", "cert", "ucits", "etf", "a",
}


def _norm(s: str | None) -> str:
    """Fold case, accents and punctuation. `Azië` and `Azie` are the same word — AirSPMS serves
    ISO-8859-1 and we have already been bitten once by treating them as different."""
    t = unicodedata.normalize("NFKD", (s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()


def stem(s: str | None) -> str:
    """The discriminating part of a strategy name, boilerplate removed.

        'Star Selection Index'             -> 'star'
        'StarTopSelectie Offensief fixed'  -> 'star'
        'BUS_Merkentopselectie offensief'  -> 'merken'
        'Shell PLC EUR'                    -> 'shell plc eur'   (matches no strategy — correct)
    """
    t = _norm(s)
    for sub in _NOISE_SUBSTR:
        t = t.replace(sub, " ")
    words = [w for w in t.split() if w and w not in _NOISE_WORDS]
    return " ".join(words)


def is_topselectie(*fields: str | None) -> bool:
    """Is this a *TopSelectie product? (In any of the fields given — a portfolio's code and its
    description disagree constantly, e.g. `TOPS_ETS_L` / 'EuropaTopSelectie'.)

    ⚠ THE STEMMER DELETES EXACTLY THE WORD THAT SEPARATES TWO PRODUCT LINES, so it has to be
    asked for separately BEFORE it is stripped. Both of these stem to 'europa':

        'EuropaTopSelectie Index'   (the certificate)     -> europa
        'Europa Offensief FX'       (BUS_EUR_OFF_FX)      -> europa      ← a DIFFERENT strategy

    and `BUS_EUR_OFF_FX` is the one with 27 real positions, so it wins the gates and scores a
    perfect 100. The link came out at 0.99 confidence and pointed at the wrong product — the
    worst failure this module can have, because a confident wrong answer is one nobody checks.
    (The genuine `EuropaTopSelect OFF FX` stores NO composition at all, so the honest output for
    that certificate is no guess.)
    """
    return any(
        marker in _norm(f)
        for f in fields
        for marker in ("topselectie", "topselect", "topsel", "selectie", "selection")
    )


def _score(fonds: str, name: str, omschrijving: str | None) -> float:
    """0-100, over the stems.

    ⚠ `token_sort_ratio`, NEVER `token_set_ratio`. token_set scores a SUBSET as a perfect
    match, and the portfolio codes are short: stem('BUS_EUR_OFF_FX') is 'eur', which is a
    subset of stem('Shell PLC EUR') — so token_set called it 100 and quietly linked *every*
    EUR-quoted holding (Shell, iShares ACWI, Vanguard Japan...) to the Europa portfolio at 0.89
    confidence. Same false-friend family as the raw `_name_score` floor in `resolve()`.
    token_sort penalises the unmatched tokens, which is the entire question here.

    The description carries the human name ('MomentumTopSelectie Fixed'); the name is often a
    code ('MoTopSelectie_FX', 'TOPS_STS_L'). Take whichever agrees — a code that happens to
    match is still a match, and token_sort makes a coincidental one expensive.
    """
    f = stem(fonds)
    if not f:
        return 0.0
    return float(max(
        fuzz.token_sort_ratio(f, stem(name)),
        fuzz.token_sort_ratio(f, stem(omschrijving)),
    ))


# A stem that agrees this well IS the strategy. Below it we are pattern-matching on boilerplate
# that survived the filter, which is how "Amazon.com" acquires a portfolio.
_MIN_SCORE = 70.0
# Two candidates this close are not a guess, they are a coin flip — say so rather than pick.
_AMBIGUOUS_MARGIN = 8.0


@dataclass
class Guess:
    linked_portfolio_id: int
    confidence: float          # 0-1
    reason: str


def guess_link(
    *,
    fonds: str,
    isin: str | None,
    owner_id: int,
    portfolios: list[dict],
    composition: dict[int, list[dict]],
) -> Guess | None:
    """The model portfolio this holding IS, or None when we cannot honestly say.

    `portfolios`   : [{id, name, omschrijving}]
    `composition`  : portfolio_id -> its position rows (used by the gates, not the score)
    """
    holders_of_isin = {
        pid for pid, rows in composition.items()
        if isin and any((r.get("isin") or "") == isin for r in rows)
    }
    fonds_family = is_topselectie(fonds)

    scored: list[tuple[float, dict, str]] = []
    for p in portfolios:
        pid = p["id"]

        # GATE 1 — NO SELF-REFERENCE. A portfolio cannot be its own holding.
        if pid == owner_id:
            continue

        # GATE 2 — NO CIRCULARITY, and this is the load-bearing one. A portfolio that HOLDS
        # this certificate is a wrapper around it, not the strategy behind it. TOPS_STS_L holds
        # "Star Selection Index" at 100% and is the single best name match in the list; without
        # this gate it wins, and the link points back at the row we started from.
        if pid in holders_of_isin:
            continue

        # GATE 3 — IT MUST HAVE A MODEL TO LOOK THROUGH TO. A `normaal`/`meervoudig` portfolio
        # stores no composition at all (StarTopSelectie OFF DYN: zero positions), and a
        # single-position portfolio is another wrapper. Linking to either buys nothing: the
        # point of the link is to reach the underlying holdings.
        if len(composition.get(pid, [])) <= 1:
            continue

        # GATE 4 — SAME PRODUCT LINE. A *TopSelectie certificate tracks a *TopSelectie model.
        # The stemmer has to strip "TopSelectie" to see past it (every one of them carries it),
        # and in doing so it erases the only thing separating "EuropaTopSelectie" from "Europa
        # Offensief". Ask before stripping. See `is_topselectie`.
        if is_topselectie(p.get("name"), p.get("omschrijving")) != fonds_family:
            continue

        sc = _score(fonds, p.get("name") or "", p.get("omschrijving"))
        if sc >= _MIN_SCORE:
            scored.append((sc, p, f"name '{fonds}' matches '{p.get('name')}'"))

    if not scored:
        return None
    scored.sort(key=lambda x: -x[0])
    top, p, reason = scored[0]

    # Confidence is the SCORE, discounted when a runner-up is breathing down its neck. A 95 that
    # beat another 94 is not a 95 — it is a coin flip we happened to win.
    runner_up = scored[1][0] if len(scored) > 1 else 0.0
    margin = top - runner_up
    conf = top / 100.0
    if len(scored) > 1 and margin < _AMBIGUOUS_MARGIN:
        conf *= 0.6
        reason += f" (ambiguous: '{scored[1][1].get('name')}' scores nearly the same)"

    return Guess(linked_portfolio_id=p["id"], confidence=round(min(conf, 0.99), 2), reason=reason)


# ─────────────────────────────────────────────────────────────────────────────────────────────
# Resolution: what the API actually serves for a row.
# ─────────────────────────────────────────────────────────────────────────────────────────────

def link_key(isin: str | None, fonds: str | None) -> str:
    """The identity a link is stored against — the HOLDING, not the (parent, holding) pair.
    'Star Selection Index' is `StarTopSelectie OFF FX` in all 11 models that hold it; storing
    that eleven times is eleven chances for the copies to disagree."""
    return (isin or "").strip() or (fonds or "").strip().lower()


@dataclass
class ResolvedLink:
    linked_portfolio_id: int | None
    source: str                # 'manual' | 'auto'
    confidence: float | None   # None for a manual link — a human choice is not a guess
    reason: str | None


def _load_context(supabase) -> tuple[list[dict], dict[int, list[dict]], dict[str, dict]]:
    portfolios = (supabase.table("airs_model_portfolio")
                  .select("id,name,omschrijving").execute().data or [])
    comp: dict[int, list[dict]] = {}
    for r in (supabase.table("airs_model_portfolio_position")
              .select("portfolio_id,isin,fonds").execute().data or []):
        comp.setdefault(r["portfolio_id"], []).append(r)
    stored = {
        link_key(r.get("isin"), r.get("fonds")): r
        for r in (supabase.table("airs_model_portfolio_link")
                  .select("isin,fonds,linked_portfolio_id").execute().data or [])
    }
    return portfolios, comp, stored


def resolve_links(supabase, owner_id: int, rows: list[dict],
                  *, context: tuple[list[dict], dict[int, list[dict]], dict[str, dict]] | None = None,
                  ) -> dict[str, ResolvedLink]:
    """Every row's link, keyed by `link_key`. A STORED row is a human decision and always wins —
    including a stored NULL, which means "explicitly not a portfolio" and must survive, or a
    wrong guess could only ever be re-pointed, never dismissed.

    `rows` are `{isin, fonds}` dicts (the position rows of `owner_id`).

    `context` is the `_load_context` triple, passed in when resolving MANY portfolios in one pass
    (the perf loop resolves all 56): `_load_context` is three full-table reads, so re-loading it
    per portfolio would be 168 queries for data that does not change between them.
    """
    portfolios, comp, stored = context if context is not None else _load_context(supabase)
    out: dict[str, ResolvedLink] = {}
    for r in rows:
        isin, fonds = (r.get("isin") or None), (r.get("fonds") or "")
        key = link_key(isin, fonds)
        if key in out:
            continue
        if key in stored:
            out[key] = ResolvedLink(stored[key].get("linked_portfolio_id"), "manual", None, None)
            continue
        g = guess_link(fonds=fonds, isin=isin, owner_id=owner_id,
                       portfolios=portfolios, composition=comp)
        out[key] = (ResolvedLink(g.linked_portfolio_id, "auto", g.confidence, g.reason)
                    if g else ResolvedLink(None, "auto", None, None))
    return out


def linkable_context(supabase, owner_id: int) -> dict:
    """Everything the row dropdowns of ONE portfolio need, in ONE call.

    `options` is every model except this one (no self-reference). `excluded_by_isin` names, per
    holding, the portfolios that already HOLD it — a link there is a cycle (TOPS_STS_L holds
    'Star Selection Index' at 100%). It is small: only a handful of holdings are held by any
    portfolio at all. Doing this per row instead would be ~30 requests to open one portfolio.
    """
    portfolios, comp, _ = _load_context(supabase)
    excluded: dict[str, list[int]] = {}
    for pid, rows in comp.items():
        for r in rows:
            if r.get("isin"):
                excluded.setdefault(r["isin"], []).append(pid)
    return {
        "options": [
            {"id": p["id"], "name": p["name"], "omschrijving": p.get("omschrijving"),
             "positions": len(comp.get(p["id"], []))}
            for p in sorted(portfolios, key=lambda p: (p["name"] or "").lower())
            if p["id"] != owner_id
        ],
        "excluded_by_isin": excluded,
    }


def linkable_portfolios(supabase, owner_id: int, isin: str | None) -> list[dict]:
    """What the dropdown may offer for a row of `owner_id`: every portfolio except the ones a
    link to would be a cycle.

      * the owner itself — a portfolio is not its own holding (the user's explicit rule), and
      * any portfolio that HOLDS this same holding — following that link walks straight back to
        the row you started from. TOPS_STS_L holds 'Star Selection Index' at 100%; offering it
        is offering a loop.
    """
    portfolios, comp, _ = _load_context(supabase)
    holders = {
        pid for pid, rows in comp.items()
        if isin and any((r.get("isin") or "") == isin for r in rows)
    }
    return [
        {"id": p["id"], "name": p["name"], "omschrijving": p.get("omschrijving"),
         "positions": len(comp.get(p["id"], []))}
        for p in sorted(portfolios, key=lambda p: (p["name"] or "").lower())
        if p["id"] != owner_id and p["id"] not in holders
    ]

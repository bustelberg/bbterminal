"""Expand a model's composition through the certificates that ARE other models.

WHY THE UNEXPANDED COMPOSITION CANNOT BE ANALYSED
    Some positions are not instruments: they are other model portfolios wrapped as a Leonteq AMC
    certificate so they can be held like a security. They are CH ISINs Yahoo can never price, so
    every analysis treats them as a dead weight — unclassified in the sector chart, dropped from
    the attribution, and subtracted from the coverage denominator.

    Measured on ToppenbergBeheer Defensief (TOPS_DEF_BEH): NINE of its twelve positions are
    certificates, carrying 44.56% of the portfolio. A sector breakdown over the remaining 55%
    is not a view of that portfolio; it is a view of its two bond ETFs and a cash line.

    The link (`airs_model_portfolio_link`) already records which model each certificate IS. This
    is what spends it: replace the certificate with the holdings of the model behind it, scaled
    by the certificate's own weight, so the analysis sees the actual stocks.

⚠ ONE HOP, AND A VISITED SET. A model can hold a certificate of a model that holds a certificate;
    `TOPS_STS_L` holds "Star Selection Index" at 100% and IS reachable as a link target elsewhere,
    so an unguarded expansion recurses until the stack ends. One hop is what the data needs today
    and it is bounded by construction; the visited set makes a cycle impossible rather than
    unlikely.

⚠ A CERTIFICATE WHOSE TARGET HAS NO COMPOSITION IS LEFT ALONE. Looking through to nothing would
    silently delete its weight from the portfolio — the total would still read 100% because
    everything else renormalises around it, so the loss would be invisible. It stays an opaque
    leg, exactly as before, and `opaque_pct` reports it.

⚠ THE SAME STOCK REACHED TWICE IS ONE LEG. AITopSelectie and MomentumTopSelectie can both hold
    NVIDIA; emitted as two legs, every downstream consumer either double-counts it or dedupes it
    by its own rule. Merged here, once, with the weights summed — the one place that knows both
    halves came from the same underlying.
"""
from __future__ import annotations

import logging

from deps import supabase

from ._airs_portfolio_links import link_key, resolve_links

_log = logging.getLogger(__name__)


def _positions_of(portfolio_id: int, datum: str | None) -> list[dict]:
    """One model's stored composition, at its own effective date."""
    rows = (supabase.table("airs_model_portfolio_position")
            .select("isin,fonds,percentage,datum,categorie")
            .eq("portfolio_id", portfolio_id).execute().data or [])
    if datum:
        rows = [r for r in rows if r.get("datum") == datum]
    return rows


def _datum_of(portfolio_id: int) -> str | None:
    r = (supabase.table("airs_model_portfolio").select("positions_datum")
         .eq("id", portfolio_id).limit(1).execute().data or [])
    return (r[0].get("positions_datum") if r else None)


def expand_positions(portfolio_id: int, datum: str | None,
                     positions: list[dict] | None = None) -> tuple[list[dict], dict]:
    """`(legs, info)` — the composition with linked certificates replaced by what they hold.

    Each leg is the stored position shape (`isin`, `fonds`, `percentage`, …) plus:
        `via`        the certificate it came through, or None for a direct holding
        `via_name`   that certificate's model name, for the UI

    `info` reports what happened, because a number computed over an expanded portfolio and one
    computed over the raw composition are different numbers and the reader has to be able to tell:
        `looked_through_pct`  weight that was expanded
        `opaque_pct`          weight still sitting in a certificate we could not expand
        `expanded`            [{fonds, weight_pct, target, holdings}]
    """
    pos = positions if positions is not None else _positions_of(portfolio_id, datum)
    links = resolve_links(supabase, portfolio_id,
                          [{"isin": r.get("isin"), "fonds": r.get("fonds")} for r in pos])
    names = {p["id"]: (p.get("display_name") or p["name"]) for p in (
        supabase.table("airs_model_portfolio").select("id,name,display_name")
        .limit(500).execute().data or [])}

    legs: list[dict] = []
    expanded: list[dict] = []
    looked_through = opaque = 0.0
    # ⚠ The PARENT is already visited. A certificate that links back to the portfolio being
    # analysed is the cycle this exists to stop, not a special case to notice later.
    visited = {portfolio_id}

    for r in pos:
        w = float(r.get("percentage") or 0)
        lk = links.get(link_key(r.get("isin"), r.get("fonds") or ""))
        target = lk.linked_portfolio_id if lk else None
        if not target or w <= 0 or target in visited:
            legs.append({**r, "via": None, "via_name": None, "via_names": []})
            continue

        child = _positions_of(target, _datum_of(target))
        inner = sum(float(c.get("percentage") or 0) for c in child)
        if not child or inner <= 0:
            # Nothing to look through to — keep the opaque leg rather than delete the weight.
            legs.append({**r, "via": None, "via_name": None, "via_names": []})
            opaque += w
            continue

        for c in child:
            cw = float(c.get("percentage") or 0)
            if cw <= 0:
                continue
            # ⚠ Scaled by the child's OWN total, not by 100. A composition that sums to 98.7%
            # (rounding, or a position AIRS dropped) would otherwise quietly shrink the parent's
            # weight in it and hand the difference to everything else.
            legs.append({**c, "percentage": w * cw / inner,
                         "via": r.get("isin"), "via_name": names.get(target),
                         "via_names": [names[target]] if names.get(target) else []})
        looked_through += w
        expanded.append({"fonds": r.get("fonds"), "isin": r.get("isin"),
                         "weight_pct": w, "target": names.get(target),
                         "target_id": target, "holdings": len(child)})

    merged = _merge_by_isin(legs)
    return merged, {
        "looked_through_pct": round(looked_through, 4),
        "opaque_pct": round(opaque, 4),
        "expanded": expanded,
    }


def merge_by_isin(legs: list[dict], fields: tuple[str, ...] = ("percentage",)) -> list[dict]:
    """One leg per ISIN, `fields` summed. Legs with no ISIN (cash) are never merged — they are
    not the same instrument just because neither has an identifier.

    ⚠ EVERY EXPANDED LIST MUST GO THROUGH THIS. AITopSelectie and MomentumTopSelectie both hold
    NVIDIA, and a portfolio can hold a stock directly AND through two certificates — so an
    unmerged expansion emits the same ISIN three times. Downstream that is not a tidiness
    problem: React renders the drill-down keyed by ISIN and logs "Encountered two children with
    the same key", which it documents as unsupported and free to duplicate or DROP a row. A
    holdings list that silently omits a position is the failure this prevents.

    `fields` differs by caller because the two sides weight differently: the model composition
    carries `percentage`, the book carries EUR values — and the book's START value has to be
    summed alongside the current one or the merged leg's return is computed against the wrong base.
    """
    out: list[dict] = []
    by_isin: dict[str, dict] = {}
    for leg in legs:
        isin = leg.get("isin")
        if not isin:
            out.append(leg)
            continue
        prev = by_isin.get(isin)
        if prev is None:
            by_isin[isin] = dict(leg)
            out.append(by_isin[isin])
            continue
        for f in fields:
            prev[f] = float(prev.get(f) or 0) + float(leg.get(f) or 0)
        # ⚠ THE ROUTES IN ARE UNIONED, NOT OVERWRITTEN. A stock reached through three certificates
        # is ONE position, but "which of my strategies put me in NVIDIA" has three answers and
        # keeping the first (or collapsing to "several") throws away the only thing the merged row
        # could still tell you about where the exposure came from.
        prev["via_names"] = sorted({*(prev.get("via_names") or []),
                                    *(leg.get("via_names") or [])})
        if prev.get("via_name") and leg.get("via_name") and prev["via_name"] != leg["via_name"]:
            prev["via_name"] = "several"
    return out


# Back-compat alias for the internal call below.
_merge_by_isin = merge_by_isin

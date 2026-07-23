"""Manual ISIN aliases: one ISIN served by another's instrument.

`asset_isin_alias` says "US8740391003 is served by TW0002330008". Applying it copies the CANONICAL
row's instrument onto the aliased execution row — the same `analysis_id` (so literally the same
price series, not a copy of it), the same Yahoo symbol, currency, exchange and listing country, and
the same GuruFocus listing.

⚠ THE SAME `analysis_id`, NOT A DUPLICATE SERIES. `asset_execution` is many-to-one on
    `asset_analysis` by design — one instrument, several venues that trade it. Pointing the alias
    at the canonical's analysis row means there is ONE series and it cannot drift; copying the
    symbol alone would leave two rows that agree today and diverge at the next price refresh.

⚠ RE-APPLIED AFTER EVERY RESOLUTION, OR IT IS NOT AN OVERRIDE. `fast_resolve`, the repointers and
    the queue worker all write `asset_execution` per ISIN and would each hand the aliased ISIN a
    listing of its own again. `apply_aliases()` runs after them and puts it back. Idempotent — a
    no-op once the row already matches.

⚠ IT DOES NOT TOUCH THE CANONICAL ROW. The alias is one-directional on purpose: the canonical is
    an ordinary instrument that other things depend on, and an override that edited both ends would
    make "which one is authoritative" unanswerable.

⚠ THE OPENFIGI IDENTITY STAYS THE ALIAS'S OWN. `openfigi_figi`/`_name`/`_type` describe the
    SECURITY, and the two are genuinely different securities (`Depositary Receipt` vs
    `Common Stock`). Overwriting them would erase the only record that this row is an ADR, which is
    exactly what a reader needs to interpret the shared price.
"""
from __future__ import annotations

import logging

from deps import supabase

_log = logging.getLogger(__name__)

# What the alias inherits: the instrument and how to price it. Deliberately NOT the OpenFIGI
# identity fields — see the module docstring.
_INHERITED = ("analysis_id", "yahoo_symbol", "name", "exchange", "currency", "med_adv_eur",
              "first_date", "years", "status", "asset_class", "listing_country", "is_leveraged")


def load_aliases() -> dict[str, str]:
    """{aliased isin: canonical isin}."""
    rows = (supabase.table("asset_isin_alias").select("isin,canonical_isin")
            .limit(2000).execute().data or [])
    return {r["isin"]: r["canonical_isin"] for r in rows}


def apply_aliases(only_isin: str | None = None) -> int:
    """Point every aliased execution row at its canonical's instrument. Returns rows changed."""
    aliases = load_aliases()
    if only_isin:
        aliases = {k: v for k, v in aliases.items() if k == only_isin}
    if not aliases:
        return 0

    wanted = sorted(set(aliases.values()))
    canon = {r["isin"]: r for r in (supabase.table("asset_execution")
                                    .select("*").in_("isin", wanted).execute().data or [])}
    changed = 0
    for isin, canonical_isin in aliases.items():
        src = canon.get(canonical_isin)
        if not src:
            _log.warning("[isin_alias] %s -> %s: the canonical has no execution row; skipped",
                         isin, canonical_isin)
            continue
        patch = {k: src.get(k) for k in _INHERITED}
        cur = (supabase.table("asset_execution").select("*")
               .eq("isin", isin).limit(1).execute().data or [])
        if not cur:
            _log.warning("[isin_alias] %s has no execution row to alias; skipped", isin)
            continue
        if all(cur[0].get(k) == patch[k] for k in _INHERITED):
            continue                                   # already aliased — idempotent no-op
        supabase.table("asset_execution").update(patch).eq("isin", isin).execute()
        changed += 1
        _log.info("[isin_alias] %s now served by %s (%s)", isin, canonical_isin,
                  patch.get("yahoo_symbol"))

        # The GuruFocus listing follows the same rule: same instrument, same listing. A stale
        # cached pick for the alias would otherwise keep pointing at its own venue.
        gf = (supabase.table("gurufocus_listing").select("*")
              .eq("isin", canonical_isin).limit(1).execute().data or [])
        if gf:
            row = {k: v for k, v in gf[0].items() if k != "isin"}
            supabase.table("gurufocus_listing").upsert({**row, "isin": isin},
                                                       on_conflict="isin").execute()
        else:
            # ⚠ DELETED, NOT LEFT BEHIND. The canonical has no GuruFocus listing, so the alias
            # must not keep its own — that is the exact drift this function exists to prevent.
            supabase.table("gurufocus_listing").delete().eq("isin", isin).execute()
    return changed


def canonical(isin: str | None) -> str | None:
    """The ISIN whose instrument actually serves `isin` — itself when it is not aliased.

    ⚠ EVERY LOOKUP KEYED ON AN ISIN NEEDS THIS, NOT JUST THE PRICE PATH. An alias points
    `asset_execution` at the canonical's instrument, but `company`, `gurufocus_listing` and the
    earnings metrics are all still keyed on the RAW ISIN — so an aliased row reads as having no
    company, no fundamentals and no coverage while its canonical has all three. Measured: the TSMC
    ADR (US8740391003) showed as "not ingested" in a portfolio's fundamentals coverage while
    TW0002330008 sat there as company 3223.

    One hop only. An alias whose canonical is itself aliased is a chain nobody has needed and
    which would need a cycle guard; if that ever appears, this is where to add it.
    """
    if not isin:
        return isin
    return load_aliases().get(isin, isin)


def canonical_map(isins: list[str]) -> dict[str, str]:
    """{raw isin: the isin that serves it} for a batch — one query rather than one per ISIN."""
    aliases = load_aliases()
    return {i: aliases.get(i, i) for i in isins}

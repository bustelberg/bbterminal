"""What series exist, where they physically live, and who they're keyed by.

The whole point of this file: a caller names a series (`"gf.close"`) and never
learns which table, which vendor, or which id space it came from. Adding a
series — an EPS line, a Yahoo volume, a derived metric — is a row here, not a
new loader.

TWO ENTITY DOMAINS, AND THEY DO NOT MIX
    `company` (`company_id`, GuruFocus universe, ~2.8k rows) and `asset`
    (`analysis_id`, Yahoo universe, ~8k rows) are disjoint id spaces with no
    bridge — only 2,065 of 16,150 `asset_execution` rows match a `company` row
    by ISIN. Mixing them in one query would silently join unrelated securities,
    so `load_series` refuses a request that spans domains.

VENDOR IS A PROPERTY OF THE SERIES, NOT THE ENGINE
    `gf.close` and `yf.close` are both "the close", from different vendors, with
    different values (different adjustment conventions, different FX). The
    scheduled /schedule strategy is priced off GuruFocus; re-pointing it at Yahoo
    would change its holdings. Keeping the vendor in the series key is what lets
    one signal engine serve both without ever silently swapping one for the other.

TWO PHYSICAL SHAPES
    `metric_data` is EAV: one row per (company, metric_code, date), the value in
    `numeric_value`. Two series there (close vs volume) are different ROWS, so
    they cannot be fetched in one column-wise scan.
    `asset_price` is wide: one row per (asset, date) with `close` and `volume` as
    columns. Two series there are different COLUMNS of the same row, so they CAN
    be fused into a single query — which is what `asset_pipeline` already relies
    on.
    `load_series` fuses when the shape allows and refuses when it doesn't, rather
    than quietly issuing two queries and hoping they align.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Domain = Literal["company", "asset"]
Vendor = Literal["gurufocus", "yahoo"]


@dataclass(frozen=True)
class TableSpec:
    """A physical table one or more series live in."""

    name: str
    entity_col: str
    date_col: str
    # Rows where this column IS NULL are excluded. `asset_price` carries rows
    # with a volume but no close; every existing asset-pipeline query drops
    # them (`close IS NOT NULL`), so the filter belongs to the table, not to
    # whichever series happens to be requested.
    require_non_null: str | None = None
    # PostgREST fallback is only implemented for `metric_data`; asset-pipeline
    # callers have always treated "no COPY" as "skip", and still do.
    has_postgrest_fallback: bool = False


COMPANY_METRICS = TableSpec(
    name="metric_data",
    entity_col="company_id",
    date_col="target_date",
    has_postgrest_fallback=True,
)

ASSET_PRICES = TableSpec(
    name="asset_price",
    entity_col="analysis_id",
    date_col="target_date",
    require_non_null="close",
)


@dataclass(frozen=True)
class SeriesSpec:
    key: str
    domain: Domain
    vendor: Vendor
    table: TableSpec
    # Column holding the value. For an EAV table this is always `numeric_value`
    # and `row_filters` selects WHICH series; for a wide table it names the column.
    value_col: str
    # Column name this series gets in the returned frame.
    alias: str
    # Literal equality filters that isolate this series in an EAV table.
    row_filters: dict[str, str] = field(default_factory=dict)

    @property
    def is_eav(self) -> bool:
        return bool(self.row_filters)


SERIES: dict[str, SeriesSpec] = {
    s.key: s
    for s in (
        SeriesSpec(
            key="gf.close",
            domain="company",
            vendor="gurufocus",
            table=COMPANY_METRICS,
            value_col="numeric_value",
            alias="close",
            row_filters={"metric_code": "close_price", "source_code": "gurufocus"},
        ),
        SeriesSpec(
            key="gf.volume",
            domain="company",
            vendor="gurufocus",
            table=COMPANY_METRICS,
            value_col="numeric_value",
            alias="volume",
            row_filters={"metric_code": "volume", "source_code": "gurufocus"},
        ),
        SeriesSpec(
            key="yf.close",
            domain="asset",
            vendor="yahoo",
            table=ASSET_PRICES,
            value_col="close",
            alias="close",
        ),
        SeriesSpec(
            key="yf.volume",
            domain="asset",
            vendor="yahoo",
            table=ASSET_PRICES,
            value_col="volume",
            alias="volume",
        ),
    )
}


def resolve(keys: str | list[str] | tuple[str, ...]) -> list[SeriesSpec]:
    """Series keys -> specs, validating that they can be served by ONE query."""
    if isinstance(keys, str):
        keys = [keys]
    if not keys:
        raise ValueError("no series requested")

    specs = []
    for k in keys:
        try:
            specs.append(SERIES[k])
        except KeyError:
            raise KeyError(
                f"unknown series {k!r}; known: {sorted(SERIES)}"
            ) from None

    domains = {s.domain for s in specs}
    if len(domains) > 1:
        raise ValueError(
            f"series span entity domains {sorted(domains)} — `company_id` and "
            "`analysis_id` are disjoint id spaces with no bridge. Load them "
            "separately."
        )

    if len(specs) > 1:
        tables = {s.table.name for s in specs}
        if len(tables) > 1:
            raise ValueError(f"series span tables {sorted(tables)}; load them separately")
        if any(s.is_eav for s in specs):
            # close_price and volume are different ROWS of metric_data, not
            # different columns. Fusing them would need a pivot and would change
            # how missing values line up.
            raise ValueError(
                f"{sorted(s.key for s in specs)} live in the EAV table "
                f"{specs[0].table.name!r} as separate rows and cannot be fused "
                "into one query — request them one at a time"
            )
        aliases = [s.alias for s in specs]
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"duplicate aliases in {aliases}")

    return specs

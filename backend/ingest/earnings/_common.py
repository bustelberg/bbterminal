"""Shared constants, dataclass, storage helpers, and generic parsers.

Every earnings submodule (financials / analyst_estimates / indicators)
imports from here, and nothing in here imports from the submodules.
"""
from __future__ import annotations

import calendar
import gzip
import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from supabase import Client

_BUCKET = "gurufocus-raw"

US_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "CBOE"}

# Indicator keys we need for the earnings dashboard (quarterly variants)
# Each entry here = one GuruFocus API call per refresh. We only keep
# indicators that aren't already in the financials JSON. Everything else
# (ROE, ROIC, Gross/Net Margin, Interest Coverage, PEG, FCF Yield) is
# derived from the financials response in `_parse_financials` — those rows
# land in metric_data with `annuals__/quarterly__Ratios__...` codes and
# the dashboard reads them directly. Forward P/E stays here because it's
# forward-looking (price ÷ next-year EPS estimate) and isn't in the
# historical financials block.
INDICATOR_KEYS = [
    "forward_pe_ratio",
]


@dataclass
class EarningsResult:
    source: str = ""  # "financials", "analyst_estimates", "indicators"
    rows_loaded: int = 0
    #: Rows the vendor gave us that were ALREADY in the database, byte for byte, and so were not
    #: written. ⚠ `rows_loaded == 0 and rows_unchanged > 0` IS THE HEALTHY, COMMON OUTCOME of a
    #: refresh — see `_upsert_metric_rows`. Only `rows_loaded == 0 and rows_unchanged == 0` means
    #: the fetch itself came back with nothing.
    rows_unchanged: int = 0
    metrics_found: int = 0
    cache_status: str = ""  # "cache_hit", "api_fresh", "api_error"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    is_forbidden: bool = False  # True if 403 / unsubscribed region
    api_calls: int = 0  # Number of GuruFocus API requests made


# ---------------------------------------------------------------------------
# Storage helpers (shared with prices.py patterns)
# ---------------------------------------------------------------------------

def _build_symbol(ticker: str, exchange: str) -> str:
    from ingest.prices import normalize_gurufocus_ticker  # noqa: PLC0415
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    if exchange.upper() in US_EXCHANGES:
        return ticker
    return f"{exchange}:{ticker}"


def refuse_unsubscribed(exchange: str, source: str) -> EarningsResult | None:
    """The refusal for an exchange our GuruFocus subscription does not cover, or None to proceed.

    ⚠⚠ IT LIVES IN THE FETCHERS BECAUSE THE VENDOR DOES NOT RELIABLY REFUSE. The 403 path below
    each caller (`api.is_forbidden`) assumes an unsubscribed region answers with an error. Diploma
    plc, `LSE:DPLM`, proves it does not: GuruFocus returned a full statements payload — 53 quarterly
    periods across hundreds of section keys — whose price column is **0 for 1998-2013** and then
    **frozen at 11.1 for seven years** (2016-09 to 2023-03) while the real share price went £8.79 to
    £28.10, before stepping 3.81x in a single period. `Market Cap` carries the identical step. All
    of it plausible, none of it true, and nothing downstream could tell.

    ⚠⚠ SO THE GATE IS THE EXCHANGE, NOT THE VALUES — and that is a measured conclusion, not a
    preference. A payload-shape heuristic was tried first and rejected: over the 1,782 companies
    holding a price series, "mostly zeros" fires on Alphabet (46%), CRH (52%) and NetEase (77%),
    because GuruFocus zero-fills quarters it does not publish; and "frozen for many periods" cannot
    be separated from a genuine trading halt — the same rule that catches Diploma also refuses
    Nebius Group, whose price is legitimately frozen through its ~2.5-year suspension. What we KNOW
    is untrustworthy is the subscription, and that is a fact about the request rather than a guess
    about the answer.

    ⚠ IT IS HERE AND NOT AT THE CALLERS. `routers/_fundamental_backfill.eligible` already applies
    this rule, so the /benchmarks and Long-Equity fills were never the leak; the per-company SSE
    refresh (`/api/earnings/{id}/refresh/{source}`) and the universe bulk fetch both called the
    fetchers directly and were not covered. Two known callers meant two places to forget, and the
    next caller would have been a third.

    ⚠ NO CALL IS SPENT. The refusal is pre-flight, so an unsubscribed name costs nothing rather
    than costing a request that returns junk — which also means a region's monthly quota is no
    longer drained by names we cannot use.

    ⚠ OTC PINK IS SUBSCRIBED, so a foreign company with a US OTC line still resolves through it —
    the usual fix for exactly these names. See `FEASIBLE_GF_EXCHANGES`.
    """
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    if is_gf_subscribed_exchange(exchange):
        return None
    r = EarningsResult(source=source)
    # ⚠ `is_forbidden`, THE SAME FLAG THE REAL 403 SETS. Callers already count and report that
    # ("skipped_region"); a new status word would need every one of them to learn it, and this is
    # the same fact arrived at one step earlier.
    r.is_forbidden = True
    r.cache_status = "unsubscribed"
    r.error = (f"{exchange or 'unknown exchange'} is outside the GuruFocus subscription — "
               f"not requested (the vendor answers some of these with plausible wrong data)")
    r.logs.append(r.error)
    return r


def _storage_path(ticker: str, exchange: str, endpoint: str) -> str:
    from ingest.prices import normalize_gurufocus_ticker  # noqa: PLC0415
    ticker = normalize_gurufocus_ticker(ticker, exchange)
    return f"{exchange.upper()}_{ticker.upper()}/{endpoint}.json"


#: The bucket is created once per PROCESS, not once per fetch. See `_ensure_bucket`.
_bucket_ready = False


def _ensure_bucket(supabase: Client) -> None:
    """Make sure the raw-response bucket exists.

    ⚠ ONCE PER PROCESS. Every one of the three feed fetchers called this at the top, so a bulk fill
    spent THREE Storage round trips per company creating a bucket that has existed since the first
    ingest — 5,136 of them on a 1,712-constituent press. It is 7ms locally and a cloud round trip in
    production, which is minutes of the run buying literally nothing.

    ⚠ A RACE HERE IS HARMLESS AND UNGUARDED ON PURPOSE. Two workers arriving together make one extra
    call that the `except` already swallows; a lock would serialise every fetcher in the fill on a
    no-op.
    """
    global _bucket_ready
    if _bucket_ready:
        return
    try:
        supabase.storage.create_bucket(_BUCKET, options={"public": False})
    except Exception:
        pass
    # ⚠ SET EVEN WHEN IT RAISED — "already exists" is the overwhelmingly common failure and is the
    # answer we wanted. A real outage would fail the upload a moment later with a message that says
    # so, which is more use than retrying the create 5,000 times.
    _bucket_ready = True


# Magic bytes for gzip (RFC 1952). Magic-byte sniff on read keeps this
# layer backward compatible with already-stored uncompressed objects --
# new writes are gzipped, legacy reads still decode.
_GZIP_MAGIC = b"\x1f\x8b"


def _fetch_from_storage(supabase: Client, path: str) -> dict | list | None:
    try:
        raw = supabase.storage.from_(_BUCKET).download(path)
        if raw.startswith(_GZIP_MAGIC):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return None


def _upload_to_storage(supabase: Client, path: str, data: Any) -> None:
    # Same gzip rationale as ingest.prices._upload_to_storage: earnings JSON
    # is highly compressible (repeated keys, numeric values). 8-12x size cut
    # is typical, which adds up across the per-ticker x per-endpoint cache.
    json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    content = gzip.compress(json_bytes, compresslevel=6)
    file_options = {
        "content-type": "application/json",
        "content-encoding": "gzip",
    }
    try:
        supabase.storage.from_(_BUCKET).upload(
            path, content, file_options=file_options,
        )
    except Exception as e:
        msg = str(e).lower()
        if "already exists" not in msg and "duplicate" not in msg and "409" not in msg:
            raise
        try:
            supabase.storage.from_(_BUCKET).update(
                path, content, file_options=file_options,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Generic value / date parsers + DB upsert helper
# ---------------------------------------------------------------------------

def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if s.upper() in {"", "N/A", "NA", "NONE", "NULL", "-"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _yyyy_mm_to_month_end(yyyy_mm: str) -> date | None:
    """'YYYY-MM' → last day of that month."""
    s = str(yyyy_mm).strip().replace("-", "")
    if len(s) < 6:
        return None
    try:
        year = int(s[:4])
        month = int(s[4:6])
        day = calendar.monthrange(year, month)[1]
        return date(year, month, day)
    except Exception:
        return None


#: `company` column recording when we last ASKED for each feed. See the migration
#: `20260817000000_company_feed_fetched_at` for why all three exist and what breaks without them.
FETCHED_AT_COLUMN = {
    "financials": "financials_fetched_at",
    "analyst_estimates": "estimates_fetched_at",
    "indicators": "indicators_fetched_at",
}


def _stamp_fetched(supabase: Client, company_id: int, source: str, _log: callable) -> None:
    """Record that we ASKED for `source` — see the migration `20260817000000`.

    ⚠⚠ STAMPED WHENEVER WE GOT AN ANSWER, INCLUDING AN EMPTY ONE, and that is the entire point.
    The smart refresh's other signal is `max(recorded_at)` on the feed's sentinel row, which only
    moves when a ROW APPEARS — so a company GuruFocus publishes no consensus for never advances it
    and is re-asked on every press, for ever. Measured on ACWI: 2,392 of 4,326 calls in one press.
    Gating this on rows-loaded would leave it NULL for exactly those companies.

    ⚠ AND IT NEVER FAILS THE INGEST. The data is already written by the time this runs; a stamp we
    could not save is a worse decision next time, not a failed fetch.
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    col = FETCHED_AT_COLUMN.get(source)
    if not col:
        return
    try:
        (supabase.table("company")
         .update({col: datetime.now(timezone.utc).isoformat()})
         .eq("company_id", company_id).execute())
    except Exception as e:  # noqa: BLE001 — see the docstring
        _log(f"could not stamp {col}: {e}")


def _upsert_metric_rows(supabase: Client, rows: list[dict]) -> tuple[int, int]:
    """Write only what would actually change. Returns `(rows written, rows already identical)`.

    Thin wrapper over the shared `ingest.metric_upsert` pair so the submodule call sites
    (`from ._common import _upsert_metric_rows`) stay put.

    ⚠⚠ THE DIFF IS ON THE EARNINGS PATH ONLY, AND IT IS THE DIFFERENCE BETWEEN A BULK FILL BEING
    AFFORDABLE AND NOT — see `changed_rows`, which carries the measurement. A refresh re-parses the
    whole GuruFocus blob (up to 36,494 rows for one company) and, measured, changes none of it.

    ⚠ IT RETURNS TWO NUMBERS BECAUSE THEY ANSWER TWO QUESTIONS, and collapsing them is a real bug.
    "0 written" now means "nothing moved", which for a company that is up to date is the CORRECT and
    expected outcome — while it used to be reachable only when the vendor returned nothing. Anything
    that reads a zero as "the fetch came back empty" (the bulk fill's retry-once did exactly that)
    would spend a second API call on every healthy company. The second number is what tells them
    apart."""
    from ingest.metric_upsert import changed_rows, upsert_metric_rows  # noqa: PLC0415
    fresh, unchanged = changed_rows(supabase, rows)
    return upsert_metric_rows(supabase, fresh), unchanged

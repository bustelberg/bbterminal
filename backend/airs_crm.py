"""Parse the CRM 'Alle relaties' Excel export into typed, human-readable rows
and store them in `airs_crm_relatie` (one row per relation per snapshot date).

Complements the raw .xlsx blob in `airs_crm_relaties_raw`: that keeps the
original file for byte-for-byte re-download; this is the queryable, Supabase-
Studio-readable form. The daily AIRS refresh (`airs_vermogen.py`) writes both.

Anything not in `COLUMN_MAP` lands in the `extra` jsonb column, so an AIRS
column add/rename is never silently dropped.
"""
from __future__ import annotations

import io
import math
from datetime import date, datetime

import pandas as pd

from deps import supabase

# Excel header → DB column (the 23 columns the export currently has).
COLUMN_MAP: dict[str, str] = {
    "id": "crm_id",
    "portefeuille": "portefeuille",
    "zoekveld": "zoekveld",
    "naam": "naam",
    "contactTijd": "contact_tijd",
    "Depotbank": "depotbank",
    "Accountmanager": "accountmanager",
    "Risicoklasse": "risicoklasse",
    "ModelPortefeuille": "model_portefeuille",
    "laatsteWaarde": "laatste_waarde",
    "rendement": "rendement",
    "rendementQTD": "rendement_qtd",
    "Startdatum": "startdatum",
    "email": "email",
    "adres": "adres",
    "plaats": "plaats",
    "land": "land",
    "roepnaam": "roepnaam",
    "achternaam": "achternaam",
    "geboortedatum": "geboortedatum",
    "part_roepnaam": "part_roepnaam",
    "part_achternaam": "part_achternaam",
    "part_geboortedatum": "part_geboortedatum",
}
_INT_COLS = {"crm_id", "contact_tijd"}
_NUM_COLS = {"laatste_waarde", "rendement", "rendement_qtd"}
_DATE_COLS = {"startdatum", "geboortedatum", "part_geboortedatum"}


def _clean(v):
    """NaN/NaT → None; otherwise the value unchanged."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass  # pd.isna chokes on some array-likes — keep the value
    return v


def _to_jsonable(v):
    """Coerce a leftover (unmapped) value into something JSON-serialisable."""
    v = _clean(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if hasattr(v, "item"):       # numpy scalar → native Python
        return v.item()
    return v


def _to_date_str(v):
    """Coerce a date-ish value to an ISO `YYYY-MM-DD` string, else None."""
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if v is None:
        return None
    try:
        return pd.to_datetime(v).date().isoformat()
    except Exception:
        return None


def parse_crm_relaties(raw: bytes) -> list[dict]:
    """Parse the raw export bytes into DB-ready row dicts (typed, snake_case
    columns + an `extra` dict for any unmapped Excel columns)."""
    try:
        df = pd.read_excel(io.BytesIO(raw), engine="xlrd")   # legacy .xls (BIFF)
    except Exception:
        df = pd.read_excel(io.BytesIO(raw))                  # .xlsx (current export)

    rows: list[dict] = []
    for _, r in df.iterrows():
        row: dict = {}
        extra: dict = {}
        for col, val in r.items():
            if col in COLUMN_MAP:
                row[COLUMN_MAP[col]] = _clean(val)
            else:
                jv = _to_jsonable(val)
                if jv is not None:
                    extra[str(col)] = jv

        for k in _INT_COLS:
            if row.get(k) is not None:
                try:
                    row[k] = int(row[k])
                except (TypeError, ValueError):
                    row[k] = None
        for k in _NUM_COLS:
            if row.get(k) is not None:
                try:
                    row[k] = float(row[k])
                except (TypeError, ValueError):
                    row[k] = None
        for k in _DATE_COLS:
            if k in row:
                row[k] = _to_date_str(row[k])

        row["extra"] = extra or None
        rows.append(row)
    return rows


def store_crm_relaties(as_of: str, rows: list[dict]) -> int:
    """OVERWRITE airs_crm_relatie with `rows`: wipe the WHOLE table (EVERY prior
    snapshot date, not just `as_of`) then insert, so it always holds exactly the
    latest CRM 'Alle relaties' export and old data never lingers. Rows without a
    `crm_id` are skipped, and duplicate crm_ids are de-duped (last wins) so the
    (as_of_date, crm_id) primary key can't collide. Returns rows stored.

    Guard: if nothing parsed (`payload` empty — e.g. a transient download/parse
    failure), the table is left UNTOUCHED rather than wiped to empty."""
    by_id: dict[int, dict] = {}
    for row in rows:
        cid = row.get("crm_id")
        if cid is None:
            continue
        by_id[cid] = {**row, "as_of_date": as_of}
    payload = list(by_id.values())
    if not payload:
        return 0   # don't wipe good data on an empty/failed parse

    # Full overwrite: delete every existing row (any date), then insert.
    supabase.table("airs_crm_relatie").delete().gte("as_of_date", "1000-01-01").execute()
    for i in range(0, len(payload), 200):
        supabase.table("airs_crm_relatie").insert(payload[i:i + 200]).execute()
    return len(payload)


def run_crm_relaties_refresh_sync() -> dict:
    """Download the CRM 'Alle relaties' export from AirSPMS and OVERWRITE
    airs_crm_relatie with the fresh snapshot (+ refresh the raw .xlsx blob in
    airs_crm_relaties_raw for byte-for-byte re-download). Blocking (Playwright +
    DB) — call from a thread. Returns {ok, as_of, rows, bytes}. Used by the daily
    11:00 scheduler job AND the AIRS 'Refresh now' bundle."""
    import base64  # noqa: PLC0415

    from airs_scanner import download_crm_relaties_sync  # noqa: PLC0415

    as_of = date.today().isoformat()
    raw = download_crm_relaties_sync()
    supabase.table("airs_crm_relaties_raw").upsert({
        "as_of_date": as_of,
        "filename": f"crm_relaties_{as_of}.xlsx",
        "content_base64": base64.b64encode(raw).decode("ascii"),
        "byte_size": len(raw),
    }, on_conflict="as_of_date").execute()
    rows = store_crm_relaties(as_of, parse_crm_relaties(raw))
    return {"ok": True, "as_of": as_of, "rows": rows, "bytes": len(raw)}

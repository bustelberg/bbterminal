"""Persistence for the asset pipeline.

Upserts the analysis asset (dedup by symbol) + the execution instrument (by
ISIN, many->one), and stores the analysis instrument's daily CLOSE + VOLUME
series (once per asset). Idempotent — safe to re-run over the same ISIN list."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from deps import supabase

from . import geo, yahoo

_CHUNK = 500

# Fields persisted to asset_price, as (Yahoo-quote-key, db-column). We store
# CLOSE + VOLUME only for now. To store full OHLCV later: (1) add open/high/low
# columns in a migration, (2) add ("open","open"),("high","high"),("low","low")
# here. The row builder, the /store + /storage responses, and the UI all read
# this one list, so nothing else changes. `close` always gates "a real bar" — so
# keep it present regardless.
STORED_FIELDS: list[tuple[str, str]] = [("close", "close"), ("volume", "volume")]


def stored_columns() -> list[str]:
    """The db columns currently persisted per bar (drives the 'what we store'
    messaging everywhere)."""
    return [col for _yk, col in STORED_FIELDS]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _analysis_row(symbol: str) -> dict | None:
    r = (
        supabase.table("asset_analysis")
        .select("analysis_id, domicile_country, continent, msci_region")
        .eq("symbol", symbol).limit(1).execute()
    )
    return r.data[0] if r.data else None


def _sync_geo(row: dict, listing_country: str | None) -> None:
    """Re-derive continent + msci_region for an analysis asset from its (already
    backfilled) domicile and the listing country we just resolved.

    Reads the STORED domicile rather than clobbering it: a re-store of an
    existing asset must not downgrade a domicile-derived region back to a
    listing-derived one (Linde would flip Europe -> North America). Writes only
    on a real change."""
    g = geo.resolve_geo(row.get("domicile_country"), listing_country)
    upd = {k: g[k] for k in ("continent", "msci_region") if g[k] != row.get(k)}
    if upd:
        supabase.table("asset_analysis").update(upd).eq("analysis_id", row["analysis_id"]).execute()


def upsert_asset(res: dict, figi: dict | None = None) -> dict:
    """Upsert asset_analysis (by symbol) + — for an ISIN input — asset_execution
    (by ISIN, linked to the analysis asset). `figi` is an optional
    `openfigi.extract_columns` dict (the 5 openfigi_* columns) merged into the
    execution row. Returns ids + the analysis symbol."""
    an = res.get("analysis") or {}
    symbol = an.get("symbol")
    if not symbol:
        raise ValueError("no analysis symbol to store")

    # Listing country is free — it falls out of the symbol suffix. Domicile needs
    # a per-symbol assetProfile call, so it's left to scripts/asset_backfill_geo.py.
    listing_country = geo.country_from_symbol(symbol, res.get("asset_class"))
    supabase.table("asset_analysis").upsert(
        {
            "symbol": symbol,
            "asset_class": res.get("asset_class"),
            "label": an.get("name"),
            "sector": res.get("sector"),
            "currency": an.get("currency"),
            "first_date": an.get("first_date"),
            "years": an.get("years"),
            "listing_country": listing_country,
            "updated_at": _now_iso(),
        },
        on_conflict="symbol",
    ).execute()
    arow = _analysis_row(symbol)
    analysis_id = arow["analysis_id"] if arow else None
    if arow:
        _sync_geo(arow, listing_country)

    execution_id = None
    if res.get("id_type") == "isin":
        ex = res.get("execution") or {}
        payload = {
            "isin": res["input"].strip().upper(),
            "analysis_id": analysis_id,
            "yahoo_symbol": ex.get("symbol"),
            "name": ex.get("name"),
            "exchange": ex.get("exchange"),
            "listing_country": geo.country_from_exchange(ex.get("exchange")),
            "currency": ex.get("currency"),
            "med_adv_eur": ex.get("med_adv_eur"),
            "first_date": ex.get("first_date"),
            "years": ex.get("years"),
            "wrapper": res.get("wrapper"),
            "is_leveraged": bool(res.get("is_leveraged")),
            "status": "ok",
            "asset_class": res.get("asset_class"),
            "updated_at": _now_iso(),
        }
        # OpenFIGI confirmation verdict: does the independent OpenFIGI name agree
        # with the yfinance listing we resolved? (Drives the grid's Match badge.)
        from .resolve import identity_status  # noqa: PLC0415
        payload["identity_status"] = identity_status(
            ex.get("name"), (figi or {}).get("openfigi_name")
        )
        if figi:  # merge the 5 openfigi_* columns
            payload.update(figi)
        eup = supabase.table("asset_execution").upsert(payload, on_conflict="isin").execute()
        execution_id = eup.data[0]["execution_id"] if eup.data else None

    return {"analysis_id": analysis_id, "execution_id": execution_id, "symbol": symbol}


def upsert_unmapped(
    isin: str,
    status: str,
    reason: str | None,
    asset_class: str | None = None,
    name: str | None = None,
    figi: dict | None = None,
) -> None:
    """Persist a row for an input ISIN that did NOT resolve to a Yahoo listing
    (status: bond | not_found | error) — `analysis_id` NULL, so it shows in the
    flat grid with its status but contributes no price series (an unmapped
    row). `figi` merges the openfigi_* columns — often the most
    useful data on a bond/not-found row. Upsert by isin — idempotent."""
    payload = {
        "isin": isin.strip().upper(),
        "analysis_id": None,
        "yahoo_symbol": None,   # clear any stale mapping if a row flips ok→fail
        "status": status,
        "reason": reason,
        "asset_class": asset_class,
        "name": name,
        # No yfinance instrument to confirm — clear any stale verdict.
        "identity_status": "unknown",
        "updated_at": _now_iso(),
    }
    if figi:  # OpenFIGI identity even when Yahoo couldn't price it
        payload.update(figi)
        if not payload.get("name"):
            payload["name"] = figi.get("openfigi_name")  # fall back to the OpenFIGI name
    supabase.table("asset_execution").upsert(payload, on_conflict="isin").execute()


def store_series(analysis_id: int, symbol: str, first_ts: int | None) -> int:
    """Fetch the FULL daily close+volume series for `symbol` and upsert into
    asset_price. Uses a period-window fetch (range=max coarsens to quarterly).
    Returns rows stored."""
    if not first_ts:
        r0 = yahoo.chart(symbol, rng="3mo")
        first_ts = ((r0 or {}).get("meta") or {}).get("firstTradeDate")
    if not first_ts:
        return 0
    w = yahoo.chart_window(symbol, int(first_ts), int(time.time()), "1d")
    if not w:
        return 0
    ts = w.get("timestamp") or []
    q = ((w.get("indicators") or {}).get("quote") or [{}])[0]
    closes = q.get("close") or []
    series = {ykey: (q.get(ykey) or []) for ykey, _col in STORED_FIELDS}
    rows = []
    for i in range(len(ts)):
        c = closes[i] if i < len(closes) else None
        if c is None or not yahoo.is_closed_bar(ts[i]):  # close gates a real, closed bar
            continue
        row: dict = {
            "analysis_id": analysis_id,
            "target_date": yahoo.utc_dt(ts[i]).date().isoformat(),
        }
        for ykey, col in STORED_FIELDS:
            arr = series[ykey]
            val = arr[i] if i < len(arr) else None
            if col == "volume" and val is None:
                val = 0  # keep volume aligned 1:1 with the stored close (no-volume day = 0)
            row[col] = val
        rows.append(row)
    # Trim the leading zero-volume backfill head (settlement-only / carried
    # rows) so the stored series starts at real trading data. No-op for FX/index.
    rows = yahoo.trim_leading_no_volume(rows)
    stored = 0
    for i in range(0, len(rows), _CHUNK):
        supabase.table("asset_price").upsert(
            rows[i:i + _CHUNK], on_conflict="analysis_id,target_date",
        ).execute()
        stored += len(rows[i:i + _CHUNK])
    # Drop any previously-stored current/future (partial) bar so a re-run that
    # now excludes today doesn't leave a stale partial close behind.
    today = datetime.now(timezone.utc).date().isoformat()
    supabase.table("asset_price").delete().eq("analysis_id", analysis_id).gte("target_date", today).execute()

    # Denormalized coverage stats onto asset_analysis — the asset_grid view READS
    # these instead of running 6 correlated subqueries per row over the 14M-row
    # asset_price table (which was blowing the statement timeout). Computed from
    # the rows we just stored. (ISO date strings sort correctly, so min/max work.)
    upd: dict = {"updated_at": _now_iso()}
    if rows:
        dates = [r["target_date"] for r in rows]
        vol_dates = [r["target_date"] for r in rows if (r.get("volume") or 0) > 0]
        n_zero = sum(1 for r in rows if (r.get("volume") or 0) == 0)
        upd.update({
            "price_from": min(dates), "price_to": max(dates), "bars": len(rows),
            "volume_from": min(vol_dates) if vol_dates else None,
            "volume_to": max(vol_dates) if vol_dates else None,
            "zero_vol_frac": round(n_zero / len(rows), 6),
        })
    # Full OHLCV+actions parquet archive (alongside asset_price) — built from the
    # SAME window `w`, so no extra Yahoo call. Best-effort: never fails the store.
    try:
        from . import parquet  # noqa: PLC0415
        pq = parquet.write(symbol, parquet.build_frame(w))
        if pq:
            upd["parquet_path"], upd["parquet_rows"] = pq
    except Exception:  # noqa: BLE001
        pass
    supabase.table("asset_analysis").update(upd).eq("analysis_id", analysis_id).execute()
    return stored


def extend_series(analysis_id: int, symbol: str, since: str) -> int | None:
    """Fetch ONLY the bars after `since` and append them. Returns rows stored, or None when the
    caller must fall back to a full `store_series` (see below).

    ⚠ WHY THIS IS NOT JUST `store_series` WITH A LATER `first_ts`.
        `store_series` derives the grid's denormalized coverage stats — `price_from`, `bars`,
        `zero_vol_frac` — FROM THE ROWS IT JUST FETCHED, and rewrites the parquet archive from
        the same window. Hand it a two-week window and it will faithfully record that Meta
        Platforms has 8 bars beginning in July 2026, and truncate its archive to match. The stats
        are not a cache of the series; they ARE what `asset_grid` reads (they exist because the
        correlated subqueries over 14M rows blew the statement timeout).

        So an incremental append MUST recompute those stats from the DATABASE, over the whole
        series, not from the slice it happened to fetch. That is what this does — one grouped
        query — and it needs the COPY path to do it exactly. Without COPY it returns None rather
        than guessing, and the caller re-runs the full path: slower, correct, never wrong.

    The parquet archive is deliberately left alone (it is a full-history artifact and this only
    saw a window; AlphaLab reads `asset_price` via COPY, not parquet). It therefore lags an
    incremental refresh — worth knowing before anything starts trusting it as current.

    Why it matters: refreshing 197 stale held instruments through `store_series` re-downloads and
    re-upserts every bar of every one of them — decades of history — to add eight days.
    """
    from common.pg import _run_copy  # noqa: PLC0415

    # Start a few days before the last stored close: Yahoo's window is inclusive-ish and a
    # re-fetched overlapping bar is an idempotent upsert, whereas a missed one is a hole.
    start = datetime.fromisoformat(since).replace(tzinfo=timezone.utc) - timedelta(days=5)
    w = yahoo.chart_window(symbol, int(start.timestamp()), int(time.time()), "1d")
    if not w:
        return 0

    ts = w.get("timestamp") or []
    q = ((w.get("indicators") or {}).get("quote") or [{}])[0]
    closes = q.get("close") or []
    series = {ykey: (q.get(ykey) or []) for ykey, _col in STORED_FIELDS}
    rows = []
    for i in range(len(ts)):
        c = closes[i] if i < len(closes) else None
        if c is None or not yahoo.is_closed_bar(ts[i]):
            continue
        row: dict = {
            "analysis_id": analysis_id,
            "target_date": yahoo.utc_dt(ts[i]).date().isoformat(),
        }
        for ykey, col in STORED_FIELDS:
            arr = series[ykey]
            val = arr[i] if i < len(arr) else None
            if col == "volume" and val is None:
                val = 0
            row[col] = val
        rows.append(row)
    # NOT `trim_leading_no_volume` — that trims the head of a FULL series (the settlement-only
    # backfill). Here the "head" is just wherever this window happens to start, and trimming it
    # would silently drop real bars.

    stored = 0
    for i in range(0, len(rows), _CHUNK):
        supabase.table("asset_price").upsert(
            rows[i:i + _CHUNK], on_conflict="analysis_id,target_date",
        ).execute()
        stored += len(rows[i:i + _CHUNK])

    today = datetime.now(timezone.utc).date().isoformat()
    supabase.table("asset_price").delete().eq("analysis_id", analysis_id).gte(
        "target_date", today).execute()

    # Stats over the WHOLE stored series, from the DB. Anything else corrupts the grid.
    buf = _run_copy(
        "COPY (SELECT min(target_date)::text, max(target_date)::text, count(*), "
        "min(target_date) FILTER (WHERE volume > 0)::text, "
        "max(target_date) FILTER (WHERE volume > 0)::text, "
        "count(*) FILTER (WHERE coalesce(volume, 0) = 0) "
        "FROM asset_price WHERE analysis_id = %s AND close IS NOT NULL) TO STDOUT WITH CSV",
        (analysis_id,),
    )
    if buf is None:
        return None                       # no exact stats -> caller falls back to the full path

    line = buf.getvalue().decode().strip()
    if not line:
        return stored
    p_from, p_to, n, v_from, v_to, n_zero = line.split(",")
    if not n or int(n) == 0:
        return stored
    supabase.table("asset_analysis").update({
        "updated_at": _now_iso(),
        "price_from": p_from, "price_to": p_to, "bars": int(n),
        "volume_from": v_from or None, "volume_to": v_to or None,
        "zero_vol_frac": round(int(n_zero) / int(n), 6),
    }).eq("analysis_id", analysis_id).execute()
    return stored


def store_one(identifier: str) -> dict:
    """Resolve + persist ONE identifier: the analysis asset (dedup by symbol) +
    the execution (by ISIN) + the analysis series' close+volume. Returns what was
    stored, including the exact `stored_fields`. Used by the single-ISIN 'Store'
    action; the batch flow reuses upsert_asset + store_series directly."""
    from . import openfigi  # noqa: PLC0415
    from .resolve import detect_id_type, resolve  # noqa: PLC0415
    # Fetch OpenFIGI identity first so it can anchor the resolution (below).
    fig = (openfigi.extract_columns(openfigi.lookup_isins([identifier]).get(identifier.strip().upper(), []))
           if detect_id_type(identifier) == "isin" else None)
    res = resolve(identifier, with_candles=False, figi_hint=fig)  # anchor to OpenFIGI identity
    an = res.get("analysis") or {}
    if not an.get("symbol"):
        reason = res.get("reason") or "no analysis instrument resolved"
        if res.get("id_type") == "isin":  # record the unmapped ISIN in the grid
            ac = res.get("asset_class")
            upsert_unmapped(identifier, "bond" if ac == "bond" else "not_found",
                            reason, ac, res.get("sector"), figi=fig)
        raise ValueError(reason)
    ids = upsert_asset(res, figi=fig)
    rows = store_series(ids["analysis_id"], an["symbol"], an.get("first_ts"))

    # A RESOLUTION WITH NO PRICE SERIES IS NOT A RESOLUTION.
    #
    # Measured 2026-07-13: ten distinct Leonteq structured products (CH ISINs, Guernsey
    # branch) each resolved to the SAME symbol GODE.DE -- a German certificate with zero
    # bars, no price_from, no price_to. Yahoo has no listing for a structured product, so
    # its search returned a name-alike, the ranker took it (nothing better was on offer),
    # and ten unrelated instruments were written as `status='ok'` pointing at one empty
    # series. Ten confident rows, no data behind any of them.
    #
    # Zero bars is the tell, and it is unambiguous: this grid exists to price instruments,
    # so an instrument with no prices was not found. Record the ISIN as unmapped -- which
    # is the honest answer for a structured product -- instead of keeping a mapping that
    # only looks like one.
    if not rows:
        reason = (f"resolved to {an['symbol']} but it has NO price series "
                  f"(0 bars) — treating as unresolved rather than storing an empty mapping")
        if res.get("id_type") == "isin":
            ac = res.get("asset_class")
            upsert_unmapped(identifier, "bond" if ac == "bond" else "not_found",
                            reason, ac, res.get("sector"), figi=fig)
        raise ValueError(reason)

    try:
        set_default_executions()
    except Exception:  # noqa: BLE001
        pass
    return {
        "analysis_id": ids["analysis_id"],
        "analysis": an.get("symbol"),
        "execution": (res.get("execution") or {}).get("symbol"),
        "asset_class": res.get("asset_class"),
        "rows": rows,
        "stored_fields": stored_columns(),
    }


def refresh_row(identifier: str) -> dict:
    """Resolve ONE row's OpenFIGI + yfinance data and persist it, returning a
    per-SOURCE outcome so the UI/script can report exactly what was found or is
    missing. Unlike `store_one` it never raises for a legit 'not found' — it
    upserts the unmapped row and returns `found=False` for that source.

    Result shape:
      {isin, status, identity_status,
       openfigi:{found,name,figi}, yfinance:{found,symbol,name,currency,rows,analysis_id}}

    OpenFIGI is fetched first because it anchors the yfinance pick (the wrong-
    company guard); the batch script parallelizes ACROSS rows for throughput."""
    from . import openfigi  # noqa: PLC0415
    from .resolve import detect_id_type, identity_status, resolve  # noqa: PLC0415

    ident = identifier.strip()
    isin = ident.upper()
    idt = detect_id_type(ident)
    fig = (openfigi.extract_columns(openfigi.lookup_isins([ident]).get(isin, []))
           if idt == "isin" else None)
    of = {"found": bool(fig and fig.get("openfigi_figi")),
          "name": (fig or {}).get("openfigi_name"), "figi": (fig or {}).get("openfigi_figi")}

    res = resolve(ident, with_candles=False, figi_hint=fig)  # anchored to OpenFIGI
    an = res.get("analysis") or {}
    if not an.get("symbol"):
        ac = res.get("asset_class")
        db_status = "bond" if ac == "bond" else "not_found"
        if idt == "isin":
            upsert_unmapped(ident, db_status, res.get("reason"), ac, res.get("sector"), figi=fig)
        return {
            "isin": isin, "status": db_status, "identity_status": "unknown",
            "openfigi": of,
            "yfinance": {"found": False, "symbol": None, "name": None,
                         "currency": None, "rows": 0, "analysis_id": None},
            "message": res.get("reason") or "No yfinance price series for this identifier.",
        }

    ids = upsert_asset(res, figi=fig)
    rows = store_series(ids["analysis_id"], an["symbol"], an.get("first_ts"))
    try:
        set_default_executions()
    except Exception:  # noqa: BLE001
        pass
    ex = res.get("execution") or {}
    return {
        "isin": isin, "status": "ok",
        "identity_status": identity_status(ex.get("name"), (fig or {}).get("openfigi_name")),
        "openfigi": of,
        "yfinance": {
            "found": True, "symbol": an.get("symbol"),
            "name": ex.get("name") or an.get("name"),
            "currency": ex.get("currency") or an.get("currency"),
            "rows": rows, "analysis_id": ids["analysis_id"],
        },
        "message": f"Resolved {an.get('symbol')} · {rows:,} bars stored.",
    }


def set_default_executions() -> int:
    """Per analysis asset, flag the most-liquid execution `is_default=true` (the
    rest false) — the auto 'best listing to trade', keeping the others on the
    table. Cheap end-of-batch pass. Returns rows touched."""
    rows = (
        supabase.table("asset_execution")
        .select("execution_id, analysis_id, med_adv_eur, is_default")
        .not_.is_("analysis_id", "null")  # unmapped rows have no analysis to default within
        .execute()
    ).data or []
    by: dict[int, list[dict]] = {}
    for r in rows:
        by.setdefault(r["analysis_id"], []).append(r)
    touched = 0
    for execs in by.values():
        best = max(execs, key=lambda r: (r.get("med_adv_eur") or 0))
        for r in execs:
            want = r["execution_id"] == best["execution_id"]
            if bool(r.get("is_default")) != want:  # only write on change
                supabase.table("asset_execution").update({"is_default": want}).eq(
                    "execution_id", r["execution_id"]
                ).execute()
                touched += 1
    return touched


def storage_summary() -> dict:
    """Live counts + a rough on-disk estimate for the asset-pipeline tables."""
    a = supabase.table("asset_analysis").select("analysis_id", count="exact").limit(1).execute()
    e = supabase.table("asset_execution").select("execution_id", count="exact").limit(1).execute()
    p = supabase.table("asset_price").select("analysis_id", count="exact").limit(1).execute()
    pv = supabase.table("asset_price").select("analysis_id", count="exact").not_.is_("volume", "null").limit(1).execute()
    n_assets, n_exec, n_rows, n_vol = (a.count or 0), (e.count or 0), (p.count or 0), (pv.count or 0)
    # asset_price is the only large table: ~48B heap + ~40B PK index per row.
    est_mb = round(n_rows * 88 / 1e6, 1)
    return {
        "analysis_assets": n_assets,
        "executions": n_exec,
        "price_rows": n_rows,
        "volume_rows": n_vol,   # rows with a positive traded volume
        "est_price_mb": est_mb,
        "stored_fields": stored_columns(),  # what we persist per bar (close, volume)
    }

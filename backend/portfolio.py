"""
portfolio.py
Business logic for AIRS portfolio upload and management.
"""
from __future__ import annotations

import difflib
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import pandas as pd
from supabase import Client


# ─────────────────────────── Excel parsing ───────────────────────────────────

def _detect_weight_scale(w: pd.Series) -> str:
    x = pd.to_numeric(w, errors="coerce").dropna()
    if x.empty:
        return "fraction"
    s = float(x.sum())
    mx = float(x.max())
    if mx <= 1.5 and 0.5 <= s <= 1.5:
        return "fraction"
    if mx <= 150 and 50 <= s <= 150:
        return "percent"
    return "percent" if mx > 1.5 else "fraction"


def _to_fraction_weights(w: pd.Series) -> pd.Series:
    scale = _detect_weight_scale(w)
    x = pd.to_numeric(w, errors="coerce")
    return x / 100.0 if scale == "percent" else x


@dataclass
class ParsedHolding:
    holding_name: str
    mv_eur: Optional[float]
    weight: Optional[float]
    currency: str
    include: bool
    weight_source: str


def parse_airs_excel(file_bytes: bytes) -> list[ParsedHolding]:
    """
    Parse AIRS Excel export.
    Expected Dutch columns: Fondsomschrijving, Huidige waarde  EUR,
    optionally Weging and Valuta.
    """
    df = pd.read_excel(BytesIO(file_bytes))

    col_name = "Fondsomschrijving"
    col_mv_eur = "Huidige waarde  EUR"
    col_weight = "Weging"
    col_ccy = "Valuta"

    missing = [c for c in [col_name, col_mv_eur] if c not in df.columns]
    if missing:
        raise ValueError(
            f"Excel missing columns: {missing}. "
            f"Expected at least '{col_name}' and '{col_mv_eur}'."
        )

    out = pd.DataFrame()
    out["holding_name"] = df[col_name].astype(str).fillna("").str.strip()
    out["currency"] = (
        df[col_ccy].astype(str).fillna("").str.strip()
        if col_ccy in df.columns else ""
    )
    out["mv_eur"] = pd.to_numeric(df[col_mv_eur], errors="coerce")

    lower = out["holding_name"].str.lower()
    is_effectenrekening = lower.str.contains("effectenrekening", na=False)

    weight_source: str
    if col_weight in df.columns and pd.to_numeric(df[col_weight], errors="coerce").notna().any():
        out["weight"] = _to_fraction_weights(df[col_weight])
        weight_source = "excel"
    else:
        mv = out["mv_eur"].copy()
        mv.loc[is_effectenrekening] = 0.0
        total = float(mv.fillna(0).sum())
        out["weight"] = (mv / total) if total > 0 else float("nan")
        weight_source = "computed"

    fund_terms = [" index", "etf", "tracker", "fonds", "fund", "selection index"]
    fund_mask = out["holding_name"].str.lower().str.contains("|".join(fund_terms), na=False)

    out["include"] = (
        (out["holding_name"].str.len() > 0)
        & (~is_effectenrekening)
        & (~fund_mask)
    )
    out = out[out["holding_name"].str.len() > 0].reset_index(drop=True)

    results: list[ParsedHolding] = []
    for _, row in out.iterrows():
        mv = row["mv_eur"]
        w = row["weight"]
        results.append(ParsedHolding(
            holding_name=str(row["holding_name"]),
            mv_eur=float(mv) if pd.notna(mv) else None,
            weight=float(w) if pd.notna(w) else None,
            currency=str(row["currency"]),
            include=bool(row["include"]),
            weight_source=weight_source,
        ))
    return results


# ─────────────────────────── Fuzzy matching ──────────────────────────────────

def _normalize(s: str) -> str:
    return (
        (s or "").lower()
        .replace("&", "and").replace("-", " ").replace(".", " ")
        .replace(",", " ").replace("  ", " ").strip()
    )


def match_holding(
    holding_name: str,
    companies: list[dict],
    *,
    min_score: float = 0.55,
) -> tuple[Optional[int], str, float]:
    """Returns (company_id|None, label, score)."""
    if not companies:
        return None, "", 0.0
    q = _normalize(holding_name)
    if not q:
        return None, "", 0.0

    names_norm = [_normalize(c.get("company_name") or "") for c in companies]
    matches = difflib.get_close_matches(q, names_norm, n=1, cutoff=0.0)
    if not matches:
        return None, "", 0.0

    best_norm = matches[0]
    score = round(difflib.SequenceMatcher(None, q, best_norm).ratio(), 3)
    idx = names_norm.index(best_norm)
    c = companies[idx]
    label = f"{c['company_name']} — {c['primary_ticker']} ({c['primary_exchange']})"
    matched_id = int(c["company_id"]) if score >= min_score else None
    return matched_id, label, score


# ─────────────────────────── Supabase CRUD ───────────────────────────────────

def get_all_companies(supabase: Client) -> list[dict]:
    resp = (
        supabase.table("company")
        .select("company_id,company_name,primary_ticker,primary_exchange,country,sector")
        .limit(10000)
        .execute()
    )
    return resp.data or []


def list_portfolios(supabase: Client) -> list[dict]:
    resp = (
        supabase.table("portfolio")
        .select("portfolio_id,portfolio_name,snapshot_id,snapshot(target_date,published_at)")
        .order("portfolio_id", desc=True)
        .execute()
    )
    results = []
    for r in (resp.data or []):
        snap = r.get("snapshot") or {}
        results.append({
            "portfolio_id": r["portfolio_id"],
            "portfolio_name": r["portfolio_name"],
            "snapshot_id": r["snapshot_id"],
            "target_date": snap.get("target_date"),
            "published_at": snap.get("published_at"),
        })
    return results


def get_portfolio_weights(supabase: Client, portfolio_id: int) -> list[dict]:
    resp = (
        supabase.table("portfolio_weight")
        .select("company_id,weight_value,company(company_name,primary_ticker,primary_exchange)")
        .eq("portfolio_id", portfolio_id)
        .execute()
    )
    results = []
    for r in (resp.data or []):
        c = r.get("company") or {}
        results.append({
            "company_id": r["company_id"],
            "weight_value": r["weight_value"],
            "company_name": c.get("company_name"),
            "primary_ticker": c.get("primary_ticker"),
            "primary_exchange": c.get("primary_exchange"),
        })
    results.sort(key=lambda x: x["weight_value"], reverse=True)
    return results


def _resolve_or_create_snapshot(supabase: Client, target_date: str, published_at: str) -> int:
    resp = (
        supabase.table("snapshot")
        .select("snapshot_id")
        .eq("target_date", target_date)
        .eq("published_at", published_at)
        .execute()
    )
    if resp.data:
        return int(resp.data[0]["snapshot_id"])
    ins = (
        supabase.table("snapshot")
        .insert({"target_date": target_date, "published_at": published_at})
        .execute()
    )
    return int(ins.data[0]["snapshot_id"])


def create_portfolio(
    supabase: Client,
    *,
    portfolio_name: str,
    target_date: str,
    published_at: str,
    weights: list[dict],
    normalize: bool = True,
) -> int:
    if normalize and weights:
        total = sum(w["weight"] for w in weights)
        if total > 0:
            weights = [{"company_id": w["company_id"], "weight": w["weight"] / total} for w in weights]

    snapshot_id = _resolve_or_create_snapshot(supabase, target_date, published_at)
    ins = (
        supabase.table("portfolio")
        .insert({"portfolio_name": portfolio_name, "snapshot_id": snapshot_id})
        .execute()
    )
    portfolio_id = int(ins.data[0]["portfolio_id"])
    if weights:
        supabase.table("portfolio_weight").insert([
            {"portfolio_id": portfolio_id, "company_id": int(w["company_id"]), "weight_value": float(w["weight"])}
            for w in weights
        ]).execute()
    return portfolio_id


def update_portfolio_weights(
    supabase: Client,
    portfolio_id: int,
    weights: list[dict],
    normalize: bool = True,
) -> None:
    if normalize and weights:
        total = sum(w["weight"] for w in weights)
        if total > 0:
            weights = [{"company_id": w["company_id"], "weight": w["weight"] / total} for w in weights]
    supabase.table("portfolio_weight").delete().eq("portfolio_id", portfolio_id).execute()
    if weights:
        supabase.table("portfolio_weight").insert([
            {"portfolio_id": portfolio_id, "company_id": int(w["company_id"]), "weight_value": float(w["weight"])}
            for w in weights
        ]).execute()


def delete_portfolio(supabase: Client, portfolio_id: int) -> None:
    supabase.table("portfolio_weight").delete().eq("portfolio_id", portfolio_id).execute()
    supabase.table("portfolio").delete().eq("portfolio_id", portfolio_id).execute()

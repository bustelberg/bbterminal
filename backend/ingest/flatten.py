from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype

# Anything the pipeline can hand us as an Excel source
ExcelSource = Union[Path, bytes]


def _open_xls(source: ExcelSource) -> pd.ExcelFile:
    """Open a pandas ExcelFile from either a Path or raw bytes."""
    if isinstance(source, bytes):
        return pd.ExcelFile(BytesIO(source))
    return pd.ExcelFile(source)


def _clean_header_cell(x: object) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower().startswith("unnamed:"):
        return ""
    if s.lower() in {"nan", "none"}:
        return ""
    return re.sub(r"\s+", " ", s)


def _make_unique(names: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        base = n if n else "col"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def _read_excel_with_grouped_headers(
    source: ExcelSource,
    *,
    sheet_name: str | int | None = 0,
    header_row: int = 0,
    subheader_row: int = 1,
    data_start_row: int = 2,
    sep: str = " - ",
) -> pd.DataFrame:
    xls = _open_xls(source)
    raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

    if raw.shape[0] <= data_start_row:
        raise ValueError(
            f"Not enough rows: header_row={header_row}, "
            f"subheader_row={subheader_row}, data_start_row={data_start_row}."
        )

    top = raw.iloc[header_row].map(_clean_header_cell)
    sub = raw.iloc[subheader_row].map(_clean_header_cell)
    top_ffill = top.replace("", pd.NA).ffill().fillna("")

    cols: list[str] = []
    for g, s in zip(top_ffill.tolist(), sub.tolist()):
        g = _clean_header_cell(g)
        s = _clean_header_cell(s)
        if g and s:
            cols.append(f"{g}{sep}{s}")
        elif s:
            cols.append(s)
        elif g:
            cols.append(g)
        else:
            cols.append("")

    cols = _make_unique(cols)
    df = raw.iloc[data_start_row:].copy()
    df.columns = cols
    df = df.loc[:, [c for c in df.columns if c and c != "col"]]
    return df


def _normalize_column_name(col: str) -> str:
    if not col:
        return ""
    s = str(col).lower().strip()
    s = s.replace(" - ", "_")
    s = s.replace("-", "_")
    s = s.replace("(bn)", "bn")
    s = s.replace("%", "pct")
    s = s.replace(".", "")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def _strip_unicode_text(x: object) -> object:
    if x is None:
        return x
    if isinstance(x, float) and pd.isna(x):
        return x
    if isinstance(x, str):
        return x.encode("ascii", "ignore").decode("ascii")
    return x


def _remove_unicode_from_values(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if is_object_dtype(s.dtype) or is_string_dtype(s.dtype):
            out[col] = s.map(_strip_unicode_text)
    return out


@dataclass(frozen=True)
class DetectedLayout:
    sheet_name: str | int
    header_row: int
    subheader_row: int
    data_start_row: int
    score: int
    reason: str


EXPECTED_SUBHEADERS = {"country", "ticker", "company", "sector"}


def _cell_str(x: object) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    if s.lower().startswith("unnamed:"):
        return ""
    if s.lower() in {"nan", "none"}:
        return ""
    return re.sub(r"\s+", " ", s)


def _canon_header_cell(s: str) -> str:
    s = s.lower().strip().replace("_", " ")
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _row_contains_expected_subheaders(row: pd.Series) -> tuple[bool, set[str], dict[str, int]]:
    idx_map: dict[str, int] = {}
    found: set[str] = set()
    for i, v in enumerate(row.tolist()):
        t = _canon_header_cell(_cell_str(v))
        if t in EXPECTED_SUBHEADERS and t not in idx_map:
            idx_map[t] = i
            found.add(t)
    return EXPECTED_SUBHEADERS.issubset(found), found, idx_map


def _is_stringish(v: object) -> bool:
    s = _cell_str(v)
    return bool(s and re.search(r"[A-Za-z]", s))


def _validate_data_start(
    raw: pd.DataFrame,
    *,
    subheader_row: int,
    idx_map: dict[str, int],
    require_two_rows: bool,
) -> tuple[bool, str]:
    col_idxs = [idx_map[k] for k in ["country", "ticker", "company", "sector"]]

    def check_row(r: int) -> tuple[bool, str]:
        if r >= raw.shape[0]:
            return False, "row_out_of_bounds"
        row = raw.iloc[r]
        vals = [row.iloc[i] if i < raw.shape[1] else None for i in col_idxs]
        if not all(_is_stringish(v) for v in vals):
            return False, f"not_stringish: {[repr(_cell_str(v)) for v in vals]}"
        return True, "ok"

    ok1, why1 = check_row(subheader_row + 1)
    if not ok1:
        return False, f"row+1 {why1}"
    if require_two_rows:
        ok2, why2 = check_row(subheader_row + 2)
        if not ok2:
            return False, f"row+2 {why2}"
        return True, "row+1 ok; row+2 ok"
    return True, "row+1 ok"


def autodetect_grouped_headers(
    source: ExcelSource,
    *,
    max_scan_rows: int = 80,
    require_two_data_rows: bool = False,
) -> DetectedLayout:
    xls = _open_xls(source)
    best: Optional[DetectedLayout] = None
    last_err: Optional[Exception] = None

    for sh in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sh, header=None)
        if raw.empty:
            continue

        scan_rows = min(max_scan_rows, raw.shape[0])
        for sub_r in range(1, scan_rows):
            ok, found, idx_map = _row_contains_expected_subheaders(raw.iloc[sub_r])
            if not ok:
                continue

            header_r = sub_r - 1
            ok_data, why_data = _validate_data_start(
                raw, subheader_row=sub_r, idx_map=idx_map,
                require_two_rows=require_two_data_rows,
            )
            if not ok_data:
                continue

            data_start_r = sub_r + 1
            try:
                df = _read_excel_with_grouped_headers(
                    source, sheet_name=sh, header_row=header_r,
                    subheader_row=sub_r, data_start_row=data_start_r,
                )
                df.columns = [_normalize_column_name(c) for c in df.columns]
                hits = len(EXPECTED_SUBHEADERS & set(df.columns))
                score = hits * 1000 + min(df.shape[1], 150) * 5 + min(df.shape[0], 3000) // 5
                cand = DetectedLayout(
                    sheet_name=sh, header_row=header_r, subheader_row=sub_r,
                    data_start_row=data_start_r, score=int(score),
                    reason=f"hits={hits} cols={df.shape[1]} rows={df.shape[0]} data_check={why_data}",
                )
                if best is None or cand.score > best.score:
                    best = cand
            except Exception as e:
                last_err = e

    if best is None:
        raise ValueError(f"Could not autodetect Excel layout. Last error: {last_err}")
    return best


def flatten_excel(source: ExcelSource) -> pd.DataFrame:
    """
    Flatten a grouped-header Excel file into a single DataFrame.
    Accepts either a Path (local file) or raw bytes (in-memory).
    """
    if isinstance(source, Path) and not source.exists():
        raise FileNotFoundError(f"Excel file not found: {source}")

    layout = autodetect_grouped_headers(source)
    df = _read_excel_with_grouped_headers(
        source,
        sheet_name=layout.sheet_name,
        header_row=layout.header_row,
        subheader_row=layout.subheader_row,
        data_start_row=layout.data_start_row,
    )
    df.columns = [_normalize_column_name(c) for c in df.columns]
    df = _remove_unicode_from_values(df)

    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        df[obj_cols] = df[obj_cols].astype("string")

    return df

# flatten.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Optional
from dataclasses import dataclass
import pandas as pd
from pandas.api.types import is_object_dtype, is_string_dtype
from quick_insight.utils import _print_rows_pretty


def _clean_header_cell(x: object) -> str:
    """Normalize header cells like NaN, 'Unnamed: 3', whitespace, etc."""
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower().startswith("unnamed:"):
        return ""
    if s.lower() in {"nan", "none"}:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s


def _make_unique(names: Iterable[str]) -> list[str]:
    """Ensure column names are unique by appending _2, _3, ... when needed."""
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
    excel_path: Path,
    *,
    sheet_name: str | int | None = 0,
    header_row: int = 0,
    subheader_row: int = 1,
    data_start_row: int = 2,
    sep: str = " - ",
) -> pd.DataFrame:
    """
    Reads an Excel sheet where:
      - header_row contains group headers (with blanks/Unnamed)
      - subheader_row contains subheaders
    Group headers apply left-to-right until the next group header.

    Produces flat columns like:
      "{Group} - {Subheader}"  OR just "{Subheader}" if group is empty.
    """
    raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

    if raw.shape[0] <= data_start_row:
        raise ValueError(
            f"Not enough rows in sheet to use header_row={header_row}, "
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

    # drop empty columns
    df = df.loc[:, [c for c in df.columns if c and c != "col"]]
    return df


def _normalize_column_name(col: str) -> str:
    """Normalizes Excel-style column headers into canonical, DuckDB-safe names."""
    if not col:
        return ""

    s = str(col).lower().strip()

    # Replace common separators with underscore
    s = s.replace(" - ", "_")
    s = s.replace("-", "_")

    # Units & punctuation
    s = s.replace("(bn)", "bn")
    s = s.replace("%", "pct")
    s = s.replace(".", "")

    # whitespace -> underscore
    s = re.sub(r"\s+", "_", s)

    # remove non-alphanumeric
    s = re.sub(r"[^a-z0-9_]", "", s)

    # collapse underscores
    s = re.sub(r"_+", "_", s)

    return s.strip("_")


def _strip_unicode_text(x: object) -> object:
    """
    Removes non-ASCII characters from string values (e.g., emojis/flags).
    Leaves non-strings untouched.
    """
    if x is None:
        return x
    if isinstance(x, float) and pd.isna(x):
        return x
    if isinstance(x, str):
        # "remove unicode characters" -> keep ASCII only
        return x.encode("ascii", "ignore").decode("ascii")
    return x


def _remove_unicode_from_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies unicode stripping only to object/string columns for speed.
    """
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if is_object_dtype(s.dtype) or is_string_dtype(s.dtype):
            out[col] = s.map(_strip_unicode_text)
    return out


# Auto detect code
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
    s = re.sub(r"\s+", " ", s)
    return s


def _canon_header_cell(s: str) -> str:
    """
    Case-insensitive normalization for header matching:
    - lower
    - collapse whitespace
    - keep only a-z0-9 and underscores/spaces
    """
    s = s.lower().strip()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]+", "", s)  # remove punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _row_contains_expected_subheaders(row: pd.Series) -> tuple[bool, set[str], dict[str, int]]:
    """
    Returns:
      ok: True if all EXPECTED_SUBHEADERS are present in this row (case-insensitive)
      found: which ones were found
      idx_map: canonical name -> column index where it was found
    """
    idx_map: dict[str, int] = {}
    found: set[str] = set()

    for i, v in enumerate(row.tolist()):
        t = _canon_header_cell(_cell_str(v))
        if t in EXPECTED_SUBHEADERS and t not in idx_map:
            idx_map[t] = i
            found.add(t)

    ok = EXPECTED_SUBHEADERS.issubset(found)
    return ok, found, idx_map


def _is_stringish(v: object) -> bool:
    """
    Data validation: value looks like a real string field.
    - must not be empty
    - must contain at least one letter
    """
    s = _cell_str(v)
    if not s:
        return False
    return bool(re.search(r"[A-Za-z]", s))


def _validate_data_start(
    raw: pd.DataFrame,
    *,
    subheader_row: int,
    idx_map: dict[str, int],
    require_two_rows: bool,
) -> tuple[bool, str]:
    """
    Checks that rows below the subheader look like actual data:
      - in the columns where (country,ticker,company,sector) appear,
        the next row contains string-ish values.
      - optionally require the second row below as well.
    """
    needed = ["country", "ticker", "company", "sector"]
    col_idxs = [idx_map[k] for k in needed]

    def check_row(r: int) -> tuple[bool, str]:
        if r >= raw.shape[0]:
            return False, "row_out_of_bounds"
        row = raw.iloc[r]
        vals = [row.iloc[i] if i < raw.shape[1] else None for i in col_idxs]
        ok = all(_is_stringish(v) for v in vals)
        if not ok:
            pretty = [repr(_cell_str(v)) for v in vals]
            return False, f"not_stringish: {pretty}"
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
    excel_path: Path | str,
    *,
    max_scan_rows: int = 80,
    require_two_data_rows: bool = False,
) -> DetectedLayout:
    """
    Detect (sheet, header_row, subheader_row, data_start_row) using strict rules:

    - subheader_row must contain the exact expected subheaders (case-insensitive):
        country, ticker, company, sector
      in a single row.
    - header_row is the row above subheader_row.
    - data_start_row is the row immediately below subheader_row, and that row
      must have string-ish values in those four columns
      (and optionally also the next row below).
    """
    path = Path(excel_path).expanduser().resolve()
    xls = pd.ExcelFile(path)

    best: Optional[DetectedLayout] = None
    last_err: Optional[Exception] = None

    for sh in xls.sheet_names:
        raw = pd.read_excel(path, sheet_name=sh, header=None)
        if raw.empty:
            continue

        scan_rows = min(max_scan_rows, raw.shape[0])

        for sub_r in range(1, scan_rows):  # needs row above
            ok, found, idx_map = _row_contains_expected_subheaders(raw.iloc[sub_r])
            if not ok:
                continue

            header_r = sub_r - 1

            ok_data, why_data = _validate_data_start(
                raw,
                subheader_row=sub_r,
                idx_map=idx_map,
                require_two_rows=require_two_data_rows,
            )
            if not ok_data:
                continue

            data_start_r = sub_r + 1

            # Confirm by actually flattening with your grouped-header reader
            try:
                df = _read_excel_with_grouped_headers(
                    path,
                    sheet_name=sh,
                    header_row=header_r,
                    subheader_row=sub_r,
                    data_start_row=data_start_r,
                )
                df.columns = [_normalize_column_name(c) for c in df.columns]

                cols = set(df.columns)
                hits = len(EXPECTED_SUBHEADERS & cols)
                n_cols = df.shape[1]
                n_rows = df.shape[0]

                # score: prioritize required hits, then width/rows
                score = 0
                score += hits * 1000
                score += min(n_cols, 150) * 5
                score += min(n_rows, 3000) // 5

                reason = (
                    f"subheaders_found={sorted(found)} idx={idx_map} "
                    f"data_check={why_data} hits={hits} cols={n_cols} rows={n_rows}"
                )

                cand = DetectedLayout(
                    sheet_name=sh,
                    header_row=header_r,
                    subheader_row=sub_r,
                    data_start_row=data_start_r,
                    score=int(score),
                    reason=reason,
                )

                if best is None or cand.score > best.score:
                    best = cand

            except Exception as e:
                last_err = e
                continue

    if best is None:
        raise ValueError(f"Could not autodetect headers for {path.name}. Last error: {last_err}")

    return best




def flatten_excel(
    excel_path: Path | str,
    *,
    print_preview: bool = False,
    preview_rows: int = 2,
    strip_unicode_values: bool = True,
    sheet_name: str | int | None = 0,
    header_row: int = 0,
    subheader_row: int = 1,
    data_start_row: int = 2,
) -> pd.DataFrame:
    """
    Flatten a grouped-header Excel file into a single DataFrame.

    Args:
        excel_path: Path to the source .xlsx file.
        print_preview: If True, prints `preview_rows` in "column (dtype) : value" format.
        preview_rows: Number of rows to print when print_preview=True.
        strip_unicode_values: If True, removes non-ASCII chars from string/object cells.
        sheet_name, header_row, subheader_row, data_start_row:
            Layout controls for the grouped-header sheet.

    Returns:
        A flattened DataFrame with normalized column names (and optionally stripped string values).
    """
    path = Path(excel_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")
    
    layout = autodetect_grouped_headers(excel_path)
    df = _read_excel_with_grouped_headers(
        Path(excel_path),
        sheet_name=layout.sheet_name,
        header_row=layout.header_row,
        subheader_row=layout.subheader_row,
        data_start_row=layout.data_start_row,
    )
    df.columns = [_normalize_column_name(c) for c in df.columns]

    if strip_unicode_values:
        df = _remove_unicode_from_values(df)


    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        df[obj_cols] = df[obj_cols].astype("string")

    if print_preview:
        _print_rows_pretty(df, rows=preview_rows)

    return df

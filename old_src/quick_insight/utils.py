import pandas as pd


def _print_rows_pretty(df: pd.DataFrame, *, rows: int = 2) -> None:
    """
    Prints the first N rows as:
      column (dtype) : value
    """
    if df.empty:
        print("✅ DataFrame is empty (0 rows).")
        print("Columns:", list(df.columns))
        return

    dtypes = df.dtypes.to_dict()
    print(f"📐 DataFrame shape: {df.shape[0]} rows × {df.shape[1]} columns\n")

    for i, (_, row) in enumerate(df.head(rows).iterrows(), start=1):
        print(f"──────────────── Row {i} ────────────────")
        for col, val in row.items():
            dtype = dtypes.get(col, "unknown")
            if isinstance(val, float) and pd.isna(val):
                v = "NaN"
            else:
                v = val
            print(f"{col:<35} ({dtype}) : {v}")
        print("")
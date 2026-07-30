"""Unit tests for the `timeseries` façade (Phase 1 of the engine unification).

No database. The registry, the SQL it builds, and the long->wide reshape are all
pure. The DB-backed equivalence checks live in `test_timeseries_db.py`.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from timeseries import SERIES, to_panel
from timeseries.loader import DATE_COL, ENTITY_COL, _build_copy_sql
from timeseries.registry import resolve


class TestRegistry:
    def test_every_series_resolves_to_itself(self):
        for key, spec in SERIES.items():
            assert resolve(key) == [spec]

    def test_unknown_series_names_the_known_ones(self):
        with pytest.raises(KeyError, match="gf.close"):
            resolve("gf.closing_price")

    def test_refuses_to_mix_entity_domains(self):
        """`company_id` and `analysis_id` are disjoint id spaces. Joining them
        would silently pair unrelated securities."""
        with pytest.raises(ValueError, match="disjoint id spaces"):
            resolve(["gf.close", "yf.close"])

    def test_refuses_to_fuse_separate_eav_rows(self):
        """close_price and volume are different ROWS of metric_data, not columns."""
        with pytest.raises(ValueError, match="cannot be fused"):
            resolve(["gf.close", "gf.volume"])

    def test_fuses_columns_of_the_same_wide_table(self):
        specs = resolve(["yf.close", "yf.volume"])
        assert [s.alias for s in specs] == ["close", "volume"]
        assert {s.table.name for s in specs} == {"asset_price"}

    def test_vendor_is_part_of_the_series_identity(self):
        """gf.close and yf.close are both 'the close' and are NOT interchangeable."""
        assert SERIES["gf.close"].vendor != SERIES["yf.close"].vendor
        assert SERIES["gf.close"].domain != SERIES["yf.close"].domain

    def test_empty_request_rejected(self):
        with pytest.raises(ValueError, match="no series"):
            resolve([])


class TestCopySql:
    def test_company_series_reproduces_the_legacy_metric_query(self):
        sql, params = _build_copy_sql(
            resolve("gf.close"), [1, 2], date(2025, 1, 1), date(2025, 12, 31), order=True
        )
        assert "FROM metric_data" in sql
        assert "company_id = ANY(%s::int[])" in sql
        assert "metric_code = %s" in sql and "source_code = %s" in sql
        assert "ORDER BY company_id, target_date" in sql
        assert params == ([1, 2], "close_price", "gurufocus", "2025-01-01", "2025-12-31")

    def test_asset_series_keeps_the_close_not_null_filter(self):
        """asset_price has rows with a volume but no close; every legacy query
        dropped them, so the filter lives on the table."""
        sql, params = _build_copy_sql(resolve(["yf.close", "yf.volume"]), [7], None, None, order=False)
        assert "SELECT analysis_id, target_date, close, volume FROM asset_price" in sql
        assert "close IS NOT NULL" in sql
        assert "ORDER BY" not in sql
        assert params == ([7],)

    def test_date_bounds_are_optional_and_inclusive(self):
        sql, params = _build_copy_sql(resolve("yf.close"), [1], "2025-01-01", None, order=False)
        assert "target_date >= %s" in sql and "target_date <= %s" not in sql
        assert params == ([1], "2025-01-01")

    def test_no_id_list_is_ever_interpolated_into_the_sql(self):
        sql, params = _build_copy_sql(resolve("gf.close"), [11, 22, 33], None, None, order=True)
        assert "11" not in sql and "22" not in sql
        assert params[0] == [11, 22, 33]


class TestToPanel:
    def _long(self) -> pd.DataFrame:
        return pd.DataFrame({
            ENTITY_COL: [1, 1, 2, 2, 3],
            DATE_COL: pd.to_datetime(
                ["2025-01-02", "2025-01-03", "2025-01-02", "2025-01-03", "2025-01-03"]
            ),
            "close": [10.0, 11.0, 20.0, 21.0, 30.0],
        })

    def test_matches_pandas_pivot_table(self):
        """The 7x-faster reshape must agree with what it replaces."""
        df = self._long()
        got = to_panel(df, "close")
        ref = df.pivot_table(index=DATE_COL, columns=ENTITY_COL, values="close").sort_index()
        pd.testing.assert_frame_equal(got, ref, check_names=False, check_column_type=False)

    def test_missing_observations_become_nan(self):
        panel = to_panel(self._long(), "close")
        assert panel.shape == (2, 3)
        assert np.isnan(panel.loc[pd.Timestamp("2025-01-02"), 3])
        assert panel.loc[pd.Timestamp("2025-01-03"), 3] == 30.0

    def test_index_is_sorted_by_date_and_columns_by_entity(self):
        df = self._long().iloc[::-1]  # shuffle input order
        panel = to_panel(df, "close")
        assert list(panel.index) == sorted(panel.index)
        assert list(panel.columns) == sorted(panel.columns)

    def test_empty_frame_yields_empty_panel(self):
        empty = self._long().iloc[0:0]
        assert to_panel(empty, "close").empty

    def test_duplicates_take_the_last_write_not_the_mean(self):
        """Documented divergence from pivot_table. Cannot occur via load_series
        (both source tables have a PK on (entity, date)), but be explicit."""
        dup = pd.DataFrame({
            ENTITY_COL: [1, 1],
            DATE_COL: pd.to_datetime(["2025-01-02", "2025-01-02"]),
            "close": [10.0, 20.0],
        })
        assert to_panel(dup, "close").iloc[0, 0] == 20.0
        assert dup.pivot_table(index=DATE_COL, columns=ENTITY_COL, values="close").iloc[0, 0] == 15.0

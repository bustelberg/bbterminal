"""`common.pg.load_rows_via_copy` — one COPY instead of chunked PostgREST round trips.

⚠⚠ THE ROWS ARE SHIPPED AS JSON, NOT AS CSV COLUMNS, AND THAT IS THE POINT OF THESE TESTS.
Every other COPY loader in this codebase parses with `line.split(",")`, which is safe only because
those queries select numbers and dates. This one selects `name`, `gf_company_name`,
`openfigi_name`, `leonteq_name` — and **1,948 rows in `asset_grid` have a comma in `name`**
("Alphabet, Inc."). A comma-split would shift every field after the name by one, producing rows
that parse cleanly and describe the WRONG INSTRUMENT: a sector, a currency and a market cap all
attributed to a different company, with nothing raising.

`row_to_json` additionally preserves TYPES and distinguishes NULL from the empty string, neither of
which bare CSV can do — so the dicts match PostgREST's field for field rather than approximately.
"""
from __future__ import annotations

import io

import common.pg as pg


def _copy_bytes(*json_lines: str) -> io.BytesIO:
    """What `COPY (SELECT row_to_json(t)::text ...) TO STDOUT WITH (FORMAT csv)` emits: one CSV
    field per line, quoted, with `"` doubled."""
    out = []
    for line in json_lines:
        out.append('"' + line.replace('"', '""') + '"')
    return io.BytesIO(("\r\n".join(out) + "\r\n").encode())


def _patch(monkeypatch, buf):
    monkeypatch.setattr(pg, "_db_url", lambda: "postgresql://x/y")
    monkeypatch.setattr(pg, "_run_copy", lambda _sql, _params: buf)


class TestTextSafety:
    def test_a_comma_in_a_name_does_not_shift_the_row(self, monkeypatch):
        """The failure the JSON encoding exists to prevent."""
        _patch(monkeypatch, _copy_bytes(
            '{"isin":"US02079K3059","name":"Alphabet, Inc.","sector":"Technology"}'))
        rows = pg.load_rows_via_copy("asset_grid", "isin,name,sector", "isin", ["x"])
        assert rows == [{"isin": "US02079K3059", "name": "Alphabet, Inc.",
                         "sector": "Technology"}]

    def test_embedded_quotes_survive(self, monkeypatch):
        _patch(monkeypatch, _copy_bytes('{"isin":"X","name":"He said \\"hi\\""}'))
        rows = pg.load_rows_via_copy("asset_grid", "isin,name", "isin", ["x"])
        assert rows[0]["name"] == 'He said "hi"'

    def test_a_newline_inside_a_value_survives(self, monkeypatch):
        """CSV quotes it; a line-oriented parser would split the row in two."""
        _patch(monkeypatch, _copy_bytes('{"isin":"X","name":"Line1\\nLine2"}'))
        rows = pg.load_rows_via_copy("asset_grid", "isin,name", "isin", ["x"])
        assert len(rows) == 1 and rows[0]["name"] == "Line1\nLine2"

    def test_non_ascii_survives(self, monkeypatch):
        _patch(monkeypatch, _copy_bytes('{"isin":"X","name":"Société Générale"}'))
        rows = pg.load_rows_via_copy("asset_grid", "isin,name", "isin", ["x"])
        assert rows[0]["name"] == "Société Générale"


class TestTypesMatchPostgREST:
    def test_null_is_none_and_not_an_empty_string(self, monkeypatch):
        """⚠ The distinction bare CSV cannot make. Callers test `r.get(x) is None` and a `""`
        would read as a real, empty value."""
        _patch(monkeypatch, _copy_bytes('{"isin":"X","delisted_at":null,"name":""}'))
        rows = pg.load_rows_via_copy("asset_grid", "isin,delisted_at,name", "isin", ["x"])
        assert rows[0]["delisted_at"] is None
        assert rows[0]["name"] == ""

    def test_numbers_stay_numbers_and_bools_stay_bools(self, monkeypatch):
        _patch(monkeypatch, _copy_bytes(
            '{"bars":1173,"market_cap_eur":28635061451.39,"is_default":true}'))
        r = pg.load_rows_via_copy("asset_grid", "bars,market_cap_eur,is_default", "isin", ["x"])[0]
        assert isinstance(r["bars"], int) and r["bars"] == 1173
        assert isinstance(r["market_cap_eur"], float)
        assert r["is_default"] is True


class TestFallbackContract:
    """`None` means "fall back to PostgREST" and must never be confused with "no rows"."""

    def test_no_db_url_returns_none(self, monkeypatch):
        monkeypatch.setattr(pg, "_db_url", lambda: None)
        assert pg.load_rows_via_copy("asset_grid", "isin", "isin", ["x"]) is None

    def test_empty_values_returns_none(self, monkeypatch):
        monkeypatch.setattr(pg, "_db_url", lambda: "postgresql://x/y")
        assert pg.load_rows_via_copy("asset_grid", "isin", "isin", []) is None

    def test_copy_unavailable_returns_none(self, monkeypatch):
        _patch(monkeypatch, None)
        assert pg.load_rows_via_copy("asset_grid", "isin", "isin", ["x"]) is None

    def test_an_empty_result_is_an_empty_list_not_none(self, monkeypatch):
        """A query that legitimately matched nothing must NOT trigger the fallback — that would
        re-issue the whole chunked read for a known-empty answer."""
        _patch(monkeypatch, io.BytesIO(b""))
        assert pg.load_rows_via_copy("asset_grid", "isin", "isin", ["x"]) == []


class TestIdentifierGuard:
    """`table` and `key_col` are interpolated into the SQL. Every caller passes a literal, and the
    guard exists so that stays true by construction rather than by review."""

    def test_a_non_identifier_table_is_refused(self, monkeypatch):
        monkeypatch.setattr(pg, "_db_url", lambda: "postgresql://x/y")
        monkeypatch.setattr(pg, "_run_copy", lambda *_: pytest_fail())
        assert pg.load_rows_via_copy("asset_grid; DROP TABLE x", "isin", "isin", ["a"]) is None

    def test_a_non_identifier_key_is_refused(self, monkeypatch):
        monkeypatch.setattr(pg, "_db_url", lambda: "postgresql://x/y")
        monkeypatch.setattr(pg, "_run_copy", lambda *_: pytest_fail())
        assert pg.load_rows_via_copy("asset_grid", "isin", "isin = 1 OR 1=1", ["a"]) is None


def pytest_fail():
    raise AssertionError("_run_copy must not be reached for a refused identifier")

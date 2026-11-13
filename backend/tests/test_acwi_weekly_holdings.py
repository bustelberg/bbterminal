from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from index_universe.acwi import holdings, weekly_holdings
from tests._fake_supabase import FakeSupabase


class Connection:
    def __init__(self, latest=None, fail_membership=False):
        self.latest = latest
        self.fail_membership = fail_membership
        self.writes = []
        self.transaction_open = False

    def execute(self, sql, params=()):
        if sql.startswith("SELECT"):
            return SimpleNamespace(fetchone=lambda: (12,) if "public.universe" in sql else self.latest)
        assert self.transaction_open
        if self.fail_membership and sql.startswith("INSERT INTO public.index_file_membership"):
            raise RuntimeError("publication failed")
        self.writes.append((sql, params))

    @contextmanager
    def transaction(self):
        self.transaction_open = True
        try:
            yield
        except Exception:
            self.writes.clear()
            raise
        finally:
            self.transaction_open = False


@pytest.fixture
def export(monkeypatch):
    today = datetime.now(ZoneInfo("America/New_York")).date()
    text = Path(holdings._CSV_FILE).read_text(encoding="utf-8-sig")
    lines = text.splitlines(keepends=True)
    lines[1] = f'Fund Holdings as of,"{today:%b %d, %Y}"\n'
    raw = "".join(lines).encode()
    monkeypatch.setattr(weekly_holdings.requests, "get",
                        lambda *_a, **_kw: SimpleNamespace(content=raw, raise_for_status=lambda: None))
    monkeypatch.setattr(weekly_holdings, "load_acwi_holdings",
                        lambda: ([None] * 2000, (today - timedelta(days=7)).isoformat()))

    def resolve(snapshot):
        rows, stamp = snapshot
        assert stamp == today.isoformat()
        assert any(row["Ticker"] == "QIA" and row["Published Weight (%)"] is not None for row in rows)
        return [{"analysis_id": 42, "source": "ishares_acwi", "ticker": "QIA"}], {"resolved": 1, "as_of": stamp}

    monkeypatch.setattr(weekly_holdings, "resolve", resolve)
    return SimpleNamespace(check=lambda: None), today


def test_weekly_import_archives_source_and_replaces_mappings_in_one_transaction(export):
    ctx, today = export
    conn = Connection()
    receipt = weekly_holdings.refresh(conn, ctx)
    assert receipt == {"status": "published", "resolved": 1, "as_of": today.isoformat()}
    assert [sql.split()[0] for sql, _ in conn.writes] == ["INSERT", "DELETE", "INSERT"]
    archived = conn.writes[0][1][3].obj
    assert len(archived) > 2000
    assert conn.writes[-1][1][1].obj[0]["analysis_id"] == 42


def test_weekly_import_leaves_last_snapshot_intact_if_membership_write_fails(export):
    ctx, _ = export
    conn = Connection(fail_membership=True)
    with pytest.raises(RuntimeError, match="publication failed"):
        weekly_holdings.refresh(conn, ctx)
    assert conn.writes == []


def test_weekly_import_rejects_older_snapshot(export):
    ctx, today = export
    conn = Connection(latest=(today + timedelta(days=1), "sha", 2200, 1))
    with pytest.raises(ValueError, match="older than the archived"):
        weekly_holdings.refresh(conn, ctx)
    assert conn.writes == []


def test_weekly_import_skips_identical_snapshot(export):
    import hashlib

    ctx, today = export
    checksum = hashlib.sha256(weekly_holdings.requests.get(weekly_holdings.SOURCE_URL).content).hexdigest()
    conn = Connection(latest=(today, checksum, 2195, 1800))
    assert weekly_holdings.refresh(conn, ctx)["status"] == "unchanged"
    assert conn.writes == []


def test_current_holdings_uses_the_newest_archived_snapshot(monkeypatch):
    import deps

    newest = [{"Name": "Newest snapshot"}]
    monkeypatch.setattr(holdings, 'load_acwi_holdings', lambda: ([{'Name': 'Older bundle'}], '15-Apr-2026'))
    monkeypatch.setattr(deps, "supabase", FakeSupabase({"acwi_holdings_snapshot": [
        {"as_of": "2026-10-29", "holdings": []},
        {"as_of": "2026-11-03", "holdings": newest},
    ]}))
    assert holdings.load_latest_acwi_holdings() == (newest, "2026-11-03")


def test_newer_bundled_snapshot_is_not_hidden_by_an_older_production_archive(monkeypatch):
    import deps
    newest = [{'Name': 'NVIDIA CORP'}]
    monkeypatch.setattr(holdings, 'load_acwi_holdings', lambda: (newest, '06-Nov-2026'))
    monkeypatch.setattr(deps, 'supabase', FakeSupabase({'acwi_holdings_snapshot': [
        {'as_of': '2026-11-05', 'holdings': [{'Name': 'Older production snapshot'}]},
    ]}))
    assert holdings.load_latest_acwi_holdings() == (newest, '06-Nov-2026')


def test_missing_bundle_does_not_break_a_valid_archived_snapshot(monkeypatch):
    import deps
    def missing():
        raise FileNotFoundError('No bundled export')
    monkeypatch.setattr(holdings, 'load_acwi_holdings', missing)
    archived = [{'Name': 'Archived member'}]
    monkeypatch.setattr(deps, 'supabase', FakeSupabase({'acwi_holdings_snapshot': [
        {'as_of': '2026-11-05', 'holdings': archived},
    ]}))
    assert holdings.load_latest_acwi_holdings() == (archived, '2026-11-05')


def test_weekly_schedule_imports_before_company_coverage(monkeypatch):
    import psycopg
    import scheduler
    from common import pg
    from routers import _acwi_weekly

    events = []
    conn = SimpleNamespace(execute=lambda _sql: SimpleNamespace(fetchone=lambda: (True,)))

    @contextmanager
    def connect(*_args, **_kwargs):
        yield conn

    def refresh(connection, _ctx):
        assert connection is conn
        events.append("holdings")
        return {"status": "published", "as_of": "2026-11-03"}

    def run(_ctx, **_kwargs):
        events.append("coverage")
        return "Done", {}

    monkeypatch.setattr(pg, "_db_url", lambda: "test")
    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(weekly_holdings, "refresh", refresh)
    monkeypatch.setattr(_acwi_weekly, "run", run)
    _, receipt = scheduler._body_acwi_weekly_coverage()
    assert events == ["holdings", "coverage"]
    assert receipt["holdings_import"]["as_of"] == "2026-11-03"


@pytest.mark.parametrize("age,expected", [(None, True), (3, False), (8, True)])
def test_startup_imports_only_missing_or_stale_holdings(monkeypatch, age, expected):
    import scheduler

    today = datetime.now(ZoneInfo("America/New_York")).date()
    latest = today - timedelta(days=age) if age is not None else None
    conn = SimpleNamespace(execute=lambda _: SimpleNamespace(fetchone=lambda: (latest,)))

    @contextmanager
    def connection():
        yield conn

    imports = []
    monkeypatch.setattr(weekly_holdings, "publication_connection", connection)
    monkeypatch.setattr(weekly_holdings, "refresh", lambda connection, _: imports.append(connection))
    scheduler._bootstrap_acwi_holdings()
    assert imports == ([conn] if expected else [])

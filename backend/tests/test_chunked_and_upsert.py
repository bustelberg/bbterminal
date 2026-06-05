"""The shared chunking + metric-upsert helpers unified during the duplication
mop-up (`deps.chunked`, `ingest.metric_upsert.upsert_metric_rows`).

`chunked` replaced a hand-rolled list-splitter in `ingest.prune_companies`;
`upsert_metric_rows` replaced the verbatim batched-upsert loop duplicated
between `ingest.prices` and `ingest.earnings._common`.
"""
from __future__ import annotations

from deps import IN_CHUNK_SIZE, chunked
from ingest.metric_upsert import METRIC_CONFLICT, upsert_metric_rows

from tests._fake_supabase import FakeSupabase


class TestChunked:
    def test_splits_into_size_slices(self):
        assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]

    def test_exact_multiple(self):
        assert list(chunked([1, 2, 3, 4], 2)) == [[1, 2], [3, 4]]

    def test_empty_yields_nothing(self):
        assert list(chunked([], 3)) == []

    def test_default_size_is_in_chunk_size(self):
        items = list(range(IN_CHUNK_SIZE + 5))
        out = list(chunked(items))
        assert len(out) == 2
        assert len(out[0]) == IN_CHUNK_SIZE
        assert len(out[1]) == 5


def _rows(n: int) -> list[dict]:
    return [
        {"company_id": 1, "metric_code": "close_price",
         "source_code": "gurufocus", "target_date": f"2024-01-{i:02d}", "numeric_value": float(i)}
        for i in range(1, n + 1)
    ]


class TestUpsertMetricRows:
    def test_empty_rows_writes_nothing(self):
        fake = FakeSupabase(tables={"metric_data": []})
        assert upsert_metric_rows(fake, []) == 0
        assert fake.writes == []

    def test_returns_total_written(self):
        fake = FakeSupabase(tables={"metric_data": []})
        assert upsert_metric_rows(fake, _rows(7)) == 7
        assert len(fake.tables["metric_data"]) == 7

    def test_batches_by_batch_size(self):
        fake = FakeSupabase(tables={"metric_data": []})
        total = upsert_metric_rows(fake, _rows(50), batch_size=20)
        assert total == 50
        # 50 rows at batch_size=20 → three upsert calls (20 + 20 + 10).
        upserts = [w for w in fake.writes if w[0] == "insert" and w[1] == "metric_data"]
        assert [w[3] for w in upserts] == [20, 20, 10]

    def test_with_retry_succeeds_without_retrying(self):
        fake = FakeSupabase(tables={"metric_data": []})
        # The happy path never raises, so with_retry just passes through.
        assert upsert_metric_rows(fake, _rows(3), with_retry=True) == 3

    def test_conflict_key_is_metric_natural_key(self):
        assert METRIC_CONFLICT == "company_id,metric_code,source_code,target_date"

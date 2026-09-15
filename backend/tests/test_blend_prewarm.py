"""Startup benchmark prewarm ordering."""

from routers import _blend_prewarm as prewarm


def test_boot_fast_path_only_warms_shared_first_paint_dependencies(monkeypatch):
    caps = object()
    growth = object()
    derived = object()
    monkeypatch.setattr(prewarm, "_endpoints", lambda: [
        ("universe-period-caps", caps),
        ("fundamental-blend-metrics", growth),
        ("margin-inputs", derived),
    ])

    assert prewarm._boot_endpoints() == [
        ("universe-period-caps", caps),
        ("fundamental-blend-metrics", growth),
    ]


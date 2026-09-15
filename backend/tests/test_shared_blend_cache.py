"""The optional Redis tier must be binary-safe and invalidate across workers."""
from __future__ import annotations

import asyncio

from routers import _blend_cache as blends
from routers import _shared_blend_cache as shared


class _FakeRedis:
    def __init__(self):
        self.values: dict[str, bytes] = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, **kwargs):
        if kwargs.get("nx") and key in self.values:
            return False
        self.values[key] = bytes(value)
        return True

    def incr(self, key):
        value = int(self.values.get(key, b"0")) + 1
        self.values[key] = str(value).encode()
        return value


class _FakeRedisFactory:
    @staticmethod
    def from_url(*_args, **_kwargs):
        return _FAKE


_FAKE = _FakeRedis()


def test_shared_cache_uses_a_generation_scoped_binary_key(monkeypatch):
    _FAKE.values.clear()
    monkeypatch.setenv("BLEND_CACHE_REDIS_URL", "redis://cache.example.test/0")
    monkeypatch.setattr(shared, "Redis", _FakeRedisFactory)
    cache = shared.SharedBlendCache()

    generation = cache.generation()
    assert generation == "1"
    key = cache.key(generation, ("margin-inputs", "SP500", "annual", ()))
    cache.put(key, b"\x1f\x8bcompressed")
    assert cache.get(key) == b"\x1f\x8bcompressed"

    cache.invalidate()
    assert cache.generation() == "2"
    assert cache.get(cache.key("2", ("margin-inputs", "SP500", "annual", ()))) is None


def test_cached_blend_reads_another_workers_shared_response(monkeypatch):
    class _Shared:
        def __init__(self):
            self.values: dict[str, bytes] = {}

        def generation(self):
            return "1"

        def key(self, _generation, request_key):
            return repr(request_key)

        def get(self, key):
            return self.values.get(key)

        def put(self, key, value):
            self.values[key] = value

        def invalidate(self):
            pass

    class _Body:
        universe = "SP500"
        cadence = "annual"
        metrics = None

    remote = _Shared()
    monkeypatch.setattr(blends, "shared_blend_cache", remote)
    blends._cache.clear()
    calls: list[int] = []

    @blends.cached_blend("test-shared")
    async def endpoint(_body, request=None):
        calls.append(1)
        return {"from": "compute"}

    first = asyncio.run(endpoint(_Body()))
    assert first.body == b'{"from":"compute"}'
    assert first.headers["x-bb-blend-cache"] == "miss"
    assert "db;dur=" in first.headers["server-timing"]
    blends._cache.clear()  # emulate a second worker with no local RAM
    second = asyncio.run(endpoint(_Body()))

    assert second.body == b'{"from":"compute"}'
    assert second.headers["x-bb-blend-cache"] == "redis"
    assert calls == [1]

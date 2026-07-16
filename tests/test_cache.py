import redis

import api.cache as cache


def test_get_cached_returns_parsed_json(monkeypatch):
    monkeypatch.setattr(cache.r, "get", lambda key: '{"total_cost_usd": 42.0}')
    assert cache.get_cached("some-key") == {"total_cost_usd": 42.0}


def test_get_cached_returns_none_on_miss(monkeypatch):
    monkeypatch.setattr(cache.r, "get", lambda key: None)
    assert cache.get_cached("some-key") is None


def test_get_cached_fails_open_when_redis_unavailable(monkeypatch):
    def raise_error(key):
        raise redis.RedisError("connection refused")

    monkeypatch.setattr(cache.r, "get", raise_error)
    assert cache.get_cached("some-key") is None


def test_set_cached_writes_json_with_ttl(monkeypatch):
    calls = []
    monkeypatch.setattr(cache.r, "set", lambda key, value, ex=None: calls.append((key, value, ex)))

    cache.set_cached("some-key", {"a": 1}, ttl_seconds=60)

    assert calls == [("some-key", '{"a": 1}', 60)]


def test_set_cached_fails_open_when_redis_unavailable(monkeypatch):
    def raise_error(key, value, ex=None):
        raise redis.RedisError("connection refused")

    monkeypatch.setattr(cache.r, "set", raise_error)
    cache.set_cached("some-key", {"a": 1})  # should not raise

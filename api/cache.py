"""
Shared Redis connection + cache-aside helper for the FastAPI app.
Redis is treated as a non-critical accelerator: if it's unreachable, callers
fall back to a cache miss (get_cached -> None) and set_cached becomes a no-op,
rather than the request failing.
"""

import json
import logging
import os

import redis

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# decode_responses=True means Redis returns strings instead of raw bytes
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=2,
)


def get_cached(key: str):
    # returns the cached value (parsed from JSON) if it exists, otherwise None
    try:
        value = r.get(key)
    except redis.RedisError:
        logger.warning("Redis unavailable, treating %s as a cache miss", key)
        return None
    if value is None:
        return None
    return json.loads(value)


def set_cached(key: str, value, ttl_seconds: int = 6 * 60 * 60):
    # stores value as JSON with an expiry, default 6 hours to match the roadmap
    try:
        r.set(key, json.dumps(value), ex=ttl_seconds)
    except redis.RedisError:
        logger.warning("Redis unavailable, skipping cache write for %s", key)

"""
Shared Redis connection + cache-aside helper for the FastAPI app.
"""

import json
import redis

# decode_responses=True means Redis returns strings instead of raw bytes
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def get_cached(key: str):
    # returns the cached value (parsed from JSON) if it exists, otherwise None
    value = r.get(key)
    if value is None:
        return None
    return json.loads(value)


def set_cached(key: str, value, ttl_seconds: int = 6 * 60 * 60):
    # stores value as JSON with an expiry, default 6 hours to match the roadmap
    r.set(key, json.dumps(value), ex=ttl_seconds)

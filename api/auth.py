"""
API key authentication for the FastAPI app.
Clients must send a valid key in the X-API-Key header.
"""

import os
import secrets

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise RuntimeError("API_KEY environment variable is not set")

_api_key_header = APIKeyHeader(name="X-API-Key")


def require_api_key(key: str = Security(_api_key_header)) -> None:
    if not secrets.compare_digest(key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

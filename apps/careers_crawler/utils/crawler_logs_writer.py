from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import certifi
from pymongo import MongoClient

from config.config import MONGO_URI

_client: Optional[MongoClient] = None
_collection_ready = False
_ttl_index_ready = False
_COLLECTION_NAME = "jobs_crawler_logs"
_DB_NAME = "nextrole"
_TTL_DAYS = 12


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI is not set. Check your .env file.")
        _client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return _client


def _ensure_collection() -> None:
    global _collection_ready
    if _collection_ready:
        return
    client = _get_client()
    db = client[_DB_NAME]
    if _COLLECTION_NAME not in db.list_collection_names():
        db.create_collection(_COLLECTION_NAME)
    _collection_ready = True


def _ensure_ttl_index() -> None:
    global _ttl_index_ready
    if _ttl_index_ready:
        return
    client = _get_client()
    db = client[_DB_NAME]
    collection = db[_COLLECTION_NAME]
    collection.create_index("expires_at", expireAfterSeconds=0)
    _ttl_index_ready = True


def write_crawler_log(company_name: str, saved_count: int, status: str) -> None:
    _ensure_collection()
    _ensure_ttl_index()
    client = _get_client()
    db = client[_DB_NAME]
    collection = db[_COLLECTION_NAME]

    now = datetime.now(timezone.utc)
    now_ts = now.isoformat()
    doc_hash = hashlib.sha256(f"{company_name}|{now_ts}".encode("utf-8")).hexdigest()
    expires_at = now + timedelta(days=_TTL_DAYS)

    collection.insert_one(
        {
            "_id": doc_hash,
            "company_name": company_name,
            "saved_count": int(saved_count),
            "date": now.date().isoformat(),
            "status": status,
            "created_at": now_ts,
            "expires_at": expires_at,
        }
    )

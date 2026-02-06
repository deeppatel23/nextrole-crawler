from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Iterable, Tuple

import certifi
from pymongo import MongoClient, UpdateOne

from config.config import MONGO_COLLECTION, MONGO_DB_NAME, MONGO_URI, CAREERS_TTL_DAYS

_client = None
_ttl_index_created = False


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI is not set. Check your .env file.")
        _client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return _client


def append_jobs_mongo(jobs: Iterable) -> Tuple[int, bool]:
    client = _get_client()
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]

    global _ttl_index_created
    if not _ttl_index_created:
        collection.create_index("expires_at", expireAfterSeconds=0)
        _ttl_index_created = True

    payload = [asdict(job) for job in jobs]
    expires_at = datetime.utcnow() + timedelta(days=CAREERS_TTL_DAYS)
    for doc in payload:
        if "job_hash" in doc:
            doc["_id"] = doc["job_hash"]
        doc["expires_at"] = expires_at
    ops = []
    for doc in payload:
        job_hash = doc.get("job_hash")
        if not job_hash:
            continue
        ops.append(
            UpdateOne(
                {"_id": job_hash},
                {"$setOnInsert": doc},
                upsert=True,
            )
        )

    if not ops:
        return 0, False

    result = collection.bulk_write(ops, ordered=False)
    saved_count = int(getattr(result, "upserted_count", 0))
    stop_fetch = saved_count < len(ops)
    return saved_count, stop_fetch

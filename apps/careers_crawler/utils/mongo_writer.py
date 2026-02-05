from dataclasses import asdict
from typing import Iterable

import certifi
from pymongo import MongoClient

from config.config import MONGO_COLLECTION, MONGO_DB_NAME, MONGO_URI

_client = None


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI is not set. Check your .env file.")
        _client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return _client


def append_jobs_mongo(jobs: Iterable) -> None:
    client = _get_client()
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]
    payload = [asdict(job) for job in jobs]
    for doc in payload:
        if "job_hash" in doc:
            doc["_id"] = doc["job_hash"]
    if payload:
        collection.insert_many(payload, ordered=False)

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
    payload = []
    for job in jobs:
        doc = asdict(job)
        interview_hash = doc.get("interview_hash")
        if interview_hash:
            doc["_id"] = interview_hash
        payload.append(doc)
    if payload:
        collection.insert_many(payload)


def has_interview_hash(interview_hash: str) -> bool:
    client = _get_client()
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]
    return collection.find_one({"_id": interview_hash}) is not None

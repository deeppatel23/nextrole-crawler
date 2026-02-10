from __future__ import annotations

from typing import Optional, Set

import certifi
from pymongo import MongoClient

from config.config import MONGO_COLLECTION, MONGO_DB_NAME, MONGO_URI


class MongoJobHashChecker:
    """Mongo-only job_hash existence checker."""

    def __init__(self) -> None:
        self._seen: Set[str] = set()
        self._collection = None
        if MONGO_URI and MONGO_COLLECTION:
            client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
            self._collection = client[MONGO_DB_NAME][MONGO_COLLECTION]

    def exists(self, job_hash: Optional[str]) -> bool:
        if not job_hash:
            return False
        if job_hash in self._seen:
            return True
        if self._collection is None:
            return False
        found = (
            self._collection.find_one({"_id": job_hash}, {"_id": 1}) is not None
        )
        if found:
            self._seen.add(job_hash)
        return found

    def record(self, job_hash: Optional[str]) -> None:
        if not job_hash:
            return
        self._seen.add(job_hash)

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Set

import certifi
from pymongo import MongoClient

from config.config import (
    MONGO_COLLECTION,
    MONGO_DB_NAME,
    MONGO_URI,
    OUTPUT_DESTINATION,
    OUTPUT_FILE,
)


class JobHashChecker:
    def __init__(
        self,
        destination: Optional[str] = None,
        output_file: Optional[str] = None,
    ) -> None:
        self._destination = (destination or OUTPUT_DESTINATION).upper()
        self._output_file = output_file or OUTPUT_FILE
        self._existing: Set[str] = set()
        self._collection = None

        if self._destination == "MONGO":
            if not MONGO_URI:
                raise RuntimeError("MONGO_URI is not set. Check your .env file.")
            if not MONGO_COLLECTION:
                raise RuntimeError("CAREERS_MONGO_COLLECTION is not set. Check your .env file.")
            client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
            self._collection = client[MONGO_DB_NAME][MONGO_COLLECTION]
        else:
            self._existing = _load_existing_hashes(self._output_file)

    def exists(self, job_hash: Optional[str]) -> bool:
        if not job_hash:
            return False
        if self._destination == "MONGO":
            if self._collection is None:
                return False
            return self._collection.find_one({"_id": job_hash}, {"_id": 1}) is not None
        return job_hash in self._existing

    def record(self, job_hash: Optional[str]) -> None:
        if not job_hash:
            return
        if self._destination != "MONGO":
            self._existing.add(job_hash)


def _load_existing_hashes(output_file: str) -> Set[str]:
    path = Path(output_file)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
    except Exception:
        return set()
    if not isinstance(data, list):
        return set()
    hashes = set()
    for item in data:
        if isinstance(item, dict):
            job_hash = item.get("job_hash")
            if isinstance(job_hash, str) and job_hash:
                hashes.add(job_hash)
    return hashes

#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
try:
    import certifi
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    certifi = None


DEFAULT_GEMINI_API_VERSION = os.getenv("GEMINI_API_VERSION", "v1beta")
DEFAULT_GEMINI_EMBEDDING_MODEL = os.getenv(
    "GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"
)
DEFAULT_GEMINI_BASE_URL = os.getenv(
    "GEMINI_BASE_URL",
    f"https://generativelanguage.googleapis.com/{DEFAULT_GEMINI_API_VERSION}",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill title embeddings for job docs in MongoDB."
    )
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", ""))
    parser.add_argument("--db-name", default=os.getenv("MONGO_DB_NAME", ""))
    parser.add_argument(
        "--collection",
        default=os.getenv("CAREERS_MONGO_COLLECTION", "jobs_data"),
    )
    parser.add_argument(
        "--title-field",
        default=os.getenv("JOBS_TITLE_FIELD", "title"),
    )
    parser.add_argument(
        "--embedding-field",
        default=os.getenv("JOBS_TITLE_EMBEDDING_FIELD", "title_embedding"),
    )
    parser.add_argument(
        "--embedding-model-field",
        default=os.getenv("JOBS_TITLE_EMBEDDING_MODEL_FIELD", "title_embedding_model"),
    )
    parser.add_argument(
        "--embedding-updated-at-field",
        default=os.getenv(
            "JOBS_TITLE_EMBEDDING_UPDATED_AT_FIELD", "title_embedding_updated_at"
        ),
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_GEMINI_EMBEDDING_MODEL,
    )
    parser.add_argument(
        "--gemini-api-key",
        default=os.getenv("GEMINI_API_KEY", ""),
        help="Gemini API key. Defaults to GEMINI_API_KEY env var.",
    )
    parser.add_argument(
        "--gemini-base-url",
        default=DEFAULT_GEMINI_BASE_URL,
        help="Gemini API base URL. Defaults to GEMINI_BASE_URL env var.",
    )
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--max-docs", type=int, default=0)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute embeddings even if embedding field already exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show how many docs would be processed, but do not call Gemini or update Mongo.",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds for each Gemini embeddings API call.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for transient Gemini API failures.",
    )
    return parser.parse_args()


def build_mongo_uri() -> str:
    explicit_uri = os.getenv("MONGO_URI", "").strip()
    if explicit_uri:
        return explicit_uri

    template = os.getenv("MONGO_URI_TEMPLATE", "").strip()
    if not template:
        return ""

    username = os.getenv("MONGO_DB_USER", "")
    password = os.getenv("MONGO_DB_PSD", "")
    try:
        return template.format(
            user=username,
            password=password,
            user_escaped=quote_plus(username),
            password_escaped=quote_plus(password),
        ).strip()
    except KeyError as exc:
        raise RuntimeError(f"MONGO_URI_TEMPLATE has unsupported placeholder: {exc}") from exc


def normalize_title(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    return normalized or None


def build_query(
    title_field: str, embedding_field: str, force: bool
) -> Dict[str, object]:
    query: Dict[str, object] = {
        title_field: {"$exists": True, "$type": "string", "$ne": ""},
    }
    if force:
        return query
    query["$or"] = [
        {embedding_field: {"$exists": False}},
        {embedding_field: None},
        {embedding_field: []},
    ]
    return query


def fetch_embeddings(
    session: requests.Session,
    api_key: str,
    base_url: str,
    model: str,
    texts: List[str],
    timeout_seconds: float,
    max_retries: int,
) -> List[List[float]]:
    model_name = model.split("/", 1)[1] if model.startswith("models/") else model
    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "requests": [
            {
                "model": f"models/{model_name}",
                "content": {"parts": [{"text": text}]},
            }
            for text in texts
        ]
    }
    url = f"{base_url}/models/{model_name}:batchEmbedContents"

    last_error = ""
    for attempt in range(max_retries + 1):
        try:
            response = session.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt == max_retries:
                break
            backoff_seconds = min(30, 2**attempt)
            time.sleep(backoff_seconds)
            continue

        if response.status_code == 200:
            try:
                body = response.json()
            except ValueError as exc:
                raise RuntimeError(f"Gemini API returned invalid JSON: {response.text}") from exc
            data = body.get("embeddings", [])
            if len(data) != len(texts):
                raise RuntimeError(
                    f"Gemini returned {len(data)} embeddings for {len(texts)} inputs."
                )
            embeddings: List[List[float]] = []
            for item in data:
                if isinstance(item, dict) and "values" in item:
                    values = item["values"]
                else:
                    values = (item or {}).get("embedding", {}).get("values")
                if not isinstance(values, list):
                    raise RuntimeError("Gemini response missing embedding values.")
                embeddings.append(values)
            return embeddings

        if response.status_code in (408, 409, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                last_error = f"status {response.status_code}: {response.text}"
                break
            backoff_seconds = min(30, 2**attempt)
            time.sleep(backoff_seconds)
            continue

        raise RuntimeError(
            (
                "Gemini API failed "
                f"(status {response.status_code}) for model '{model_name}': {response.text}\n"
                "Try --model gemini-embedding-001 (or set GEMINI_EMBEDDING_MODEL)."
            )
        )

    raise RuntimeError(f"Gemini API failed after retries: {last_error}")


def backfill(
    collection: Collection,
    gemini_api_key: str,
    gemini_base_url: str,
    model: str,
    title_field: str,
    embedding_field: str,
    embedding_model_field: str,
    embedding_updated_at_field: str,
    batch_size: int,
    max_docs: int,
    force: bool,
    dry_run: bool,
    timeout_seconds: float,
    max_retries: int,
) -> None:
    if batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0.")

    query = build_query(title_field=title_field, embedding_field=embedding_field, force=force)
    projection = {"_id": 1, title_field: 1, embedding_field: 1}
    cursor = collection.find(query, projection=projection).sort("_id", 1)

    total_scanned = 0
    total_candidates = 0
    total_updated = 0
    batch: List[Tuple[object, str]] = []

    session = requests.Session()
    started_at = time.time()

    def flush_batch(items: List[Tuple[object, str]]) -> int:
        if not items:
            return 0
        if dry_run:
            return len(items)

        ids = [item[0] for item in items]
        texts = [item[1] for item in items]
        embeddings = fetch_embeddings(
            session=session,
            api_key=gemini_api_key,
            base_url=gemini_base_url,
            model=model,
            texts=texts,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        now = datetime.now(timezone.utc)
        operations = []
        for doc_id, embedding in zip(ids, embeddings):
            operations.append(
                UpdateOne(
                    {"_id": doc_id},
                    {
                        "$set": {
                            embedding_field: embedding,
                            embedding_model_field: model,
                            embedding_updated_at_field: now,
                        }
                    },
                )
            )
        result = collection.bulk_write(operations, ordered=False)
        return int(result.modified_count)

    try:
        for doc in cursor:
            total_scanned += 1
            if max_docs > 0 and total_candidates >= max_docs:
                break

            title = normalize_title(doc.get(title_field))
            if not title:
                continue

            total_candidates += 1
            batch.append((doc["_id"], title))
            if len(batch) >= batch_size:
                total_updated += flush_batch(batch)
                batch = []
                elapsed = round(time.time() - started_at, 1)
                print(
                    f"progress scanned={total_scanned} candidates={total_candidates} "
                    f"updated={total_updated} elapsed_sec={elapsed}"
                )

        if batch:
            total_updated += flush_batch(batch)
    finally:
        cursor.close()
        session.close()

    elapsed = round(time.time() - started_at, 1)
    action = "would_update" if dry_run else "updated"
    print(
        f"done scanned={total_scanned} candidates={total_candidates} "
        f"{action}={total_updated} elapsed_sec={elapsed}"
    )


def main() -> int:
    load_dotenv()
    args = parse_args()

    try:
        mongo_uri = args.mongo_uri or build_mongo_uri()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not mongo_uri:
        print("error: Mongo URI missing. Set MONGO_URI or pass --mongo-uri.", file=sys.stderr)
        return 1
    if not args.db_name:
        print("error: Mongo DB name missing. Set MONGO_DB_NAME or pass --db-name.", file=sys.stderr)
        return 1

    gemini_api_key = (args.gemini_api_key or "").strip()
    if not args.dry_run and not gemini_api_key:
        print("error: GEMINI_API_KEY is required unless --dry-run is used.", file=sys.stderr)
        return 1

    mongo_tls_ca_file = os.getenv("MONGO_TLS_CA_FILE", "").strip() or (
        certifi.where() if certifi else None
    )
    mongo_tls_allow_invalid = os.getenv("MONGO_TLS_ALLOW_INVALID_CERTS", "false").lower() in {
        "1",
        "true",
        "yes",
        "y",
    }
    client = MongoClient(
        mongo_uri,
        tlsCAFile=mongo_tls_ca_file,
        tlsAllowInvalidCertificates=mongo_tls_allow_invalid,
    )
    try:
        collection = client[args.db_name][args.collection]
        backfill(
            collection=collection,
            gemini_api_key=gemini_api_key,
            gemini_base_url=args.gemini_base_url,
            model=args.model,
            title_field=args.title_field,
            embedding_field=args.embedding_field,
            embedding_model_field=args.embedding_model_field,
            embedding_updated_at_field=args.embedding_updated_at_field,
            batch_size=args.batch_size,
            max_docs=args.max_docs,
            force=args.force,
            dry_run=args.dry_run,
            timeout_seconds=args.request_timeout,
            max_retries=args.max_retries,
        )
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

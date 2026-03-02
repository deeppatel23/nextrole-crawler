import os
import time
from typing import Dict, List, Tuple

import requests


DEFAULT_GEMINI_API_VERSION = os.getenv("GEMINI_API_VERSION", "v1beta")
DEFAULT_GEMINI_BASE_URL = os.getenv(
    "GEMINI_BASE_URL",
    f"https://generativelanguage.googleapis.com/{DEFAULT_GEMINI_API_VERSION}",
)
DEFAULT_GEMINI_EMBEDDING_MODEL = os.getenv(
    "GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"
)


def _normalize_title(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _fetch_embeddings(
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
            time.sleep(min(30, 2**attempt))
            continue

        if response.status_code == 200:
            body = response.json()
            data = body.get("embeddings", [])
            if len(data) != len(texts):
                raise RuntimeError(
                    f"Gemini returned {len(data)} embeddings for {len(texts)} inputs."
                )
            out: List[List[float]] = []
            for item in data:
                values = item.get("values")
                if not isinstance(values, list):
                    raise RuntimeError("Gemini response missing embedding values.")
                out.append(values)
            return out

        if response.status_code in (408, 409, 429, 500, 502, 503, 504):
            if attempt == max_retries:
                last_error = f"status {response.status_code}: {response.text}"
                break
            time.sleep(min(30, 2**attempt))
            continue

        raise RuntimeError(
            f"Gemini API failed (status {response.status_code}) for model '{model_name}': {response.text}"
        )

    raise RuntimeError(f"Gemini API failed after retries: {last_error}")


def get_title_embedding_map(
    titles: List[object],
    api_key: str = "",
    model: str = "",
    base_url: str = "",
    batch_size: int = 100,
    timeout_seconds: float = 60.0,
    max_retries: int = 3,
) -> Tuple[Dict[str, List[float]], str]:
    gemini_api_key = (api_key or os.getenv("GEMINI_API_KEY", "")).strip()
    if not gemini_api_key:
        return {}, ""

    model_name = (model or DEFAULT_GEMINI_EMBEDDING_MODEL).strip()
    gemini_base_url = (base_url or DEFAULT_GEMINI_BASE_URL).strip()
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0.")

    unique_titles: List[str] = []
    seen = set()
    for raw in titles:
        normalized = _normalize_title(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_titles.append(normalized)

    if not unique_titles:
        return {}, model_name

    out: Dict[str, List[float]] = {}
    with requests.Session() as session:
        for i in range(0, len(unique_titles), batch_size):
            batch = unique_titles[i : i + batch_size]
            vectors = _fetch_embeddings(
                session=session,
                api_key=gemini_api_key,
                base_url=gemini_base_url,
                model=model_name,
                texts=batch,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
            for title, vector in zip(batch, vectors):
                out[title] = vector

    return out, model_name

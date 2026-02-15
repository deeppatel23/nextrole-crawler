import json
import re
from difflib import SequenceMatcher
from typing import Any, Optional

import requests


_KEYWORDS = (
    "description",
    "descriptions",
    "responsibility",
    "responsibilities",
    "qualification",
    "qualifications",
    "requirement",
    "requirements",
    "summary",
    "summaries",
    "about",
    "skills",
    "skill",
    "capability",
    "capabilities",
    "experience",
    "experiences",
    "job",
    "jobs",
    "role",
    "roles",
    "detail",
    "details",
    "description",
    "job-details",
    "job_description",
    "job-responsibilities",
    "job_qualification",
    "job_qualifications",
    "job_requirements",
    "job_responsibilities",
    "job-qualification",
    "job-requirements"
)


def _matches_key(key: str) -> bool:
    key_norm = re.sub(r"[^a-z0-9]", "", key.lower())
    if any(token in key_norm for token in _KEYWORDS):
        return True
    for token in _KEYWORDS:
        token_norm = re.sub(r"[^a-z0-9]", "", token.lower())
        if SequenceMatcher(None, key_norm, token_norm).ratio() >= 0.8:
            return True
    return False


def _collect_text_fields(obj: Any, out: list[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k)
            if _matches_key(key):
                if isinstance(v, str):
                    out.append(v)
                elif isinstance(v, (dict, list)):
                    _collect_text_fields(v, out)
            else:
                _collect_text_fields(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_text_fields(item, out)


def _extract_json_text(html: str) -> str:
    texts: list[str] = []
    for match in re.findall(r"<script[^>]*>(.*?)</script>", html, flags=re.S | re.I):
        snippet = match.strip()
        if not snippet:
            continue
        if not (snippet.startswith("{") or snippet.startswith("[")):
            continue
        try:
            data = json.loads(snippet)
        except Exception:
            continue
        _collect_text_fields(data, texts)

    combined = " ".join(t for t in texts if t)
    combined = re.sub(r"\s+", " ", combined).strip()
    return combined


def fetch_visible_text(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception:
        return None

    json_text = _extract_json_text(html)

    # Remove scripts/styles
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.S | re.I)

    # Strip tags
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    if text and len(text) >= 300:
        print(f"HTML Utils: Extracted visible text from {url} of length {len(text)}")
        return text

    if json_text:
        print(f"HTML Utils: Extracted visible text from {url} is too short (length {len(text)}), returning JSON text of length {len(json_text)} instead")
        return json_text
    print(f"HTML Utils: No visible text extracted from {url}, returning JSON text of length {len(json_text)}")
    return text or None

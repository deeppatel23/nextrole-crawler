import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.llm_client import LLMClient


_llm_client = None
_DEFAULT_CATEGORY = "Operations"


def _get_llm_client() -> LLMClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


def _categories_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "categories.json"


@lru_cache(maxsize=1)
def _load_category_config() -> Tuple[List[str], Dict[str, List[str]]]:
    try:
        raw = _categories_path().read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception:
        return [_DEFAULT_CATEGORY], {}

    options = data.get("categories")
    if not isinstance(options, list):
        options = data.get("category")
    if not isinstance(options, list):
        options = [_DEFAULT_CATEGORY]

    cleaned = [str(item).strip() for item in options if str(item).strip()]
    if not cleaned:
        cleaned = [_DEFAULT_CATEGORY]

    raw_keyword_map = data.get("keyword_map")
    keyword_map: Dict[str, List[str]] = {}
    if isinstance(raw_keyword_map, dict):
        for key, values in raw_keyword_map.items():
            if not isinstance(key, str):
                continue
            if not isinstance(values, list):
                continue
            cleaned_values = [str(v).strip().lower() for v in values if str(v).strip()]
            if cleaned_values:
                keyword_map[key.strip()] = cleaned_values

    return cleaned, keyword_map


def get_category_options() -> List[str]:
    options, _ = _load_category_config()
    return options


def get_keyword_map() -> Dict[str, List[str]]:
    _, keyword_map = _load_category_config()
    return keyword_map


def _coerce_json(text: str) -> str:
    fence_start = "```"
    if fence_start in text:
        parts = text.split(fence_start)
        for part in parts:
            if "{" in part and "}" in part:
                candidate = part.lstrip()
                if candidate.startswith("json"):
                    candidate = candidate[4:]
                return candidate.strip().strip("\n")

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]

    return text


def _normalize_category(
    candidate: Optional[str],
    options: List[str],
) -> Optional[str]:
    if not candidate:
        return None
    normalized = {opt.lower(): opt for opt in options}
    candidate_clean = candidate.strip()
    if not candidate_clean:
        return None

    exact = normalized.get(candidate_clean.lower())
    if exact:
        return exact

    lowered_candidate = candidate_clean.lower()
    for key, value in normalized.items():
        if key in lowered_candidate or lowered_candidate in key:
            return value
    return None


def _score_by_keywords(text: str, options: List[str]) -> Optional[str]:
    if not text.strip():
        return None
    lowered = text.lower()
    keyword_map = get_keyword_map()
    if not keyword_map:
        return None

    best_option = None
    best_score = 0
    option_lookup = {opt.lower(): opt for opt in options}

    for option, keywords in keyword_map.items():
        mapped_option = option_lookup.get(option.lower())
        if not mapped_option:
            continue
        score = sum(1 for keyword in keywords if keyword in lowered)
        if score > best_score:
            best_score = score
            best_option = mapped_option

    return best_option


def _build_prompt(
    title: Optional[str],
    department: Optional[str],
    skills: Optional[List[str]],
    page_text: str,
    options: List[str],
) -> str:
    skills_text = ", ".join(skills or [])
    choices = ", ".join(options)
    return f"""
You are a job category classifier.
Return ONLY valid JSON. No markdown, no code fences, no extra text.

Schema:
{{
  "category": string
}}

Rules:
- Choose exactly one category from this list: [{choices}]
- Output must match one option text exactly.
- Use the provided title, department, skills, and job text as evidence.
- If uncertain, still choose the closest single category from the allowed list.

Job Title:
{title or ""}

Department:
{department or ""}

Skills:
{skills_text}

Job Text:
{page_text}
""".strip()


def match_category(
    title: Optional[str],
    page_text: Optional[str] = None,
    skills: Optional[List[str]] = None,
    department: Optional[str] = None,
    category_hint: Optional[str] = None,
) -> str:
    options = get_category_options()
    direct = _normalize_category(category_hint, options)
    if direct:
        return direct

    text = " ".join(
        [
            title or "",
            department or "",
            " ".join(skills or []),
            page_text or "",
            category_hint or "",
        ]
    ).strip()

    prompt = _build_prompt(title, department, skills, (page_text or "")[:6000], options)
    try:
        raw = _get_llm_client().extract_json(prompt)
        try:
            data: Dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError:
            data = json.loads(_coerce_json(raw))
        category = _normalize_category(data.get("category"), options)
        if category:
            return category
    except Exception:
        pass

    keyword_choice = _score_by_keywords(text, options)
    if keyword_choice:
        return keyword_choice
    if _DEFAULT_CATEGORY in options:
        return _DEFAULT_CATEGORY
    return options[0]

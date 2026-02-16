import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

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
def get_category_options() -> List[str]:
    try:
        raw = _categories_path().read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception:
        return [_DEFAULT_CATEGORY]

    options = data.get("category")
    if not isinstance(options, list):
        return [_DEFAULT_CATEGORY]

    cleaned = [str(item).strip() for item in options if str(item).strip()]
    return cleaned or [_DEFAULT_CATEGORY]


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

    keyword_map: Dict[str, List[str]] = {
        "Administration & Corporate Support": ["admin", "assistant", "office", "support"],
        "Artificial Intelligence & Machine Learning": ["machine learning", "ai ", "llm", "deep learning", "nlp"],
        "Cloud & Solution Architecture": ["cloud", "solution architect", "aws", "azure", "gcp"],
        "Cybersecurity & Information Security": ["security", "cyber", "infosec", "soc", "vulnerability"],
        "Data Engineering & Analytics": ["data engineer", "analytics", "bi", "sql", "warehouse"],
        "Finance & Accounting": ["finance", "accounting", "fp&a", "audit", "tax"],
        "Hardware & Embedded Engineering": ["embedded", "firmware", "hardware", "rtos", "electronics"],
        "Human Resources & Talent": ["hr", "recruit", "talent", "people operations"],
        "Infrastructure & Platform Engineering": ["sre", "devops", "infrastructure", "platform", "kubernetes"],
        "Marketing & Communications": ["marketing", "brand", "communications", "content"],
        "Operations": ["operations", "ops", "process", "service delivery"],
        "Product & Design": ["product manager", "product design", "ux", "ui", "designer"],
        "Program & Project Management": ["program manager", "project manager", "pm", "scrum"],
        "Research & Investment": ["research", "investment", "quant", "portfolio"],
        "Risk, Compliance & Legal": ["risk", "compliance", "legal", "regulatory", "governance"],
        "Sales & Business Development": ["sales", "account executive", "business development", "partnership"],
        "Software Engineering": ["software engineer", "developer", "backend", "frontend", "full stack"],
        "Specialized / Internal Groups": ["internal", "special projects", "chief of staff", "strategy office"],
        "Supply Chain & Manufacturing": ["supply chain", "manufacturing", "procurement", "logistics"],
    }

    best_option = None
    best_score = 0
    allowed = set(options)
    for option, keywords in keyword_map.items():
        if option not in allowed:
            continue
        score = sum(1 for keyword in keywords if keyword in lowered)
        if score > best_score:
            best_score = score
            best_option = option

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

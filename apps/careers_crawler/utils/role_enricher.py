import json
from typing import Optional

from utils.html_utils import fetch_visible_text
from utils.llm_client import LLMClient


_llm_client = None


def _get_llm_client() -> LLMClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client


def _build_prompt(title: Optional[str], page_text: str) -> str:
    return f"""
You are an information extraction system.
Return ONLY valid JSON. No markdown, no code fences, no extra text.

Schema:
{{
  "top_skills": [string],  // up to 3 items, most important first
  "min_yoe": number | null,
  "max_yoe": number | null
}}

Rules:
- Only extract years of experience if explicitly stated.
- For ranges like "3-5 years", use min_yoe=3 and max_yoe=5.
- For "3+ years" or "at least 3 years", set min_yoe=3 and max_yoe=null.
- If unsure, first try to infer from Job Title and Job Page Text. If still unsure, use null.

Job Title:
{title or ""}

Job Page Text:
{page_text}
""".strip()


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


def get_enrichment(
    title: Optional[str],
    apply_link: Optional[str],
    extra_text: Optional[str] = None,
) -> dict:
    if not apply_link:
        return {"skills": [], "min_yoe": None, "max_yoe": None}

    page_text = extra_text or ""
    if not page_text:
        page_text = fetch_visible_text(apply_link) or ""
        if not page_text:
            print(f"Careers: failed to fetch text for {apply_link}, using title only")

    prompt = _build_prompt(title, page_text[:6000])
    try:
        raw = _get_llm_client().extract_json(prompt)
    except Exception as e:
        print(f"Careers: LLM failed for {apply_link}: {e}")
        return {"skills": [], "min_yoe": None, "max_yoe": None}

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        try:
            data = json.loads(_coerce_json(raw))
        except json.JSONDecodeError:
            print(f"Careers: LLM returned invalid JSON for {apply_link}")
            return {"skills": [], "min_yoe": None, "max_yoe": None}

    skills = data.get("top_skills") or []
    out_skills = []
    if isinstance(skills, list):
        out_skills = [str(s) for s in skills[:3] if s]

    min_yoe = data.get("min_yoe")
    max_yoe = data.get("max_yoe")
    out_min = int(min_yoe) if isinstance(min_yoe, (int, float)) else None
    out_max = int(max_yoe) if isinstance(max_yoe, (int, float)) else None

    return {"skills": out_skills, "min_yoe": out_min, "max_yoe": out_max}

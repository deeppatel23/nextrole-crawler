import json
from models.interview import InterviewExperience, Question
from utils.llm_client import LLMClient
from config.config import EXTRACTOR_PROMPT_MODE


class InterviewExtractor:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    def _build_prompt(self, text: str, title: str | None) -> str:
        base = f"""
You are an information extraction system.
Return ONLY valid JSON. No markdown, no code fences, no extra text.
If the title contains a level like L5 or E5, save it in "level".

Schema:
{{
  "company": string | null,
  "role": string | null,
  "level": string | null,
  "years_of_experience": number | null,
  "location": string | null,
  "questions": [
    {{
      "topics": [string],
      "description": string,
      "links": [string]
    }}
  ],
  "final_verdict": string | null,
  "additional_links": [string]
}}

Rules for "questions":
- Each question must be a single interview question.
- The "topics" list must include exactly one of: "DSA", "Theory", "LLD", "HLD".
- If unsure, choose the closest type and keep topics minimal.
- If a follow-up is clearly tied to the previous question (e.g., starts with "What if..." or "How would you..." about the same scenario), merge it into the same question "description" separated by " / ".

Title:
{title or ""}

Post:
{text}
"""
        if EXTRACTOR_PROMPT_MODE == "only_questions":
            return (
                base
                + """
Instructions:
- Do NOT write any summary.
- Focus on extracting the interview questions accurately.
"""
            )

        return (
            base
            + """
Instructions:
- Focus on extracting interview questions accurately.
"""
        )

    def extract(self, text: str, title: str | None = None) -> InterviewExperience:
        prompt = self._build_prompt(text, title)

        raw = self.llm.extract_json(prompt)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            cleaned = self._coerce_json(raw)
            try:
                data = json.loads(cleaned)
            except json.JSONDecodeError:
                raise ValueError("LLM returned invalid JSON")

        questions = []
        for q in data.get("questions", []):
            if isinstance(q, str):
                questions.append(
                    Question(
                        topics=[],
                        description=q,
                        links=[],
                    )
                )
                continue

            questions.append(
                Question(
                    topics=q.get("topics", []) or [],
                    description=q.get("description", ""),
                    links=q.get("links", []) or [],
                )
            )


        return InterviewExperience(
            company=data.get("company"),
            role=data.get("role"),
            level=data.get("level"),
            years_of_experience=data.get("years_of_experience"),
            location=data.get("location"),
            questions=questions,
            final_verdict=data.get("final_verdict"),
            additional_links=data.get("additional_links", []),
        )

    @staticmethod
    def _coerce_json(text: str) -> str:
        # Strip Markdown code fences if present
        fence_start = "```"
        if fence_start in text:
            parts = text.split(fence_start)
            # Try to find a fenced block that looks like JSON
            for part in parts:
                if "{" in part and "}" in part:
                    candidate = part
                    # Remove optional language tag like "json"
                    candidate = candidate.lstrip()
                    if candidate.startswith("json"):
                        candidate = candidate[4:]
                    return candidate.strip().strip("\n")

        # Fallback: attempt to slice the first JSON object
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start : end + 1]

        return text

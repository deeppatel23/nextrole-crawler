import json
from models.interview import InterviewExperience, InterviewRound
from utils.llm_client import LLMClient
from config.config import EXTRACTOR_PROMPT_MODE


class InterviewExtractor:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    def _build_prompt(self, text: str, title: str | None) -> str:
        base = f"""
You are an information extraction system.
Return ONLY valid JSON.
If the title contains a level starting with L like L5, save it in "role".

Schema:
{{
  "company": string | null,
  "role": string | null,
  "level": string | null,
  "years_of_experience": number | null,
  "location": string | null,
  "mode": string | null,
  "rounds": [
    {{
      "round_number": number,
      "round_type": [string],
      "topics": [string],
      "verdict": string | null,
      "description": string | null,
      "questions": [string]
    }}
  ],
  "final_verdict": string | null,
  "description": string | null,
  "additional_links": [string]
}}

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
- Leave top-level "description" as null.
- Leave each round "description" as null.
- Extract and list the questions asked in each round in "questions".
"""
            )

        return (
            base
            + """
Instructions:
- For "description", provide a detailed, raw-as-possible extract of the relevant experience.
- Preserve original wording, punctuation, and line breaks where helpful.
- If the post includes a problem statement or test cases (inputs/outputs), include them verbatim in "description".
- For each round "description", include round-specific details if present, using the same raw style.
- Extract and list the questions asked in each round in "questions".
"""
        )

    def extract(self, text: str, title: str | None = None) -> InterviewExperience:
        prompt = self._build_prompt(text, title)

        raw = self.llm.extract_json(prompt)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            raise ValueError("LLM returned invalid JSON")

        rounds = []

        for r in data.get("rounds", []):
            rounds.append(
                InterviewRound(
                    round_number=r.get("round_number"),
                    round_type=r.get("round_type", []),
                    topics=r.get("topics", []),
                    verdict=r.get("verdict"),
                    description=r.get("description"),
                    questions=r.get("questions", []),
                )
            )


        return InterviewExperience(
            company=data.get("company"),
            role=data.get("role"),
            level=data.get("level"),
            years_of_experience=data.get("years_of_experience"),
            location=data.get("location"),
            mode=data.get("mode"),
            rounds=rounds,
            final_verdict=data.get("final_verdict"),
            description=data.get("description"),
            additional_links=data.get("additional_links", []),
        )

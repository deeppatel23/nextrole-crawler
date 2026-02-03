import json
from typing import List
from models.interview import InterviewExperience
from config.config import OUTPUT_FILE


class InterviewRepository:
    def save(self, interviews: List[InterviewExperience]):
        try:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(
                    [self._to_dict(i) for i in interviews],
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
        except IOError as e:
            raise RuntimeError(f"Failed to write output file: {e}")

    def _to_dict(self, interview: InterviewExperience):
        return {
            **interview.__dict__,
            "rounds": [r.__dict__ for r in interview.rounds],
        }

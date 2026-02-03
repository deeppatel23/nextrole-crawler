from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class InterviewRound:
    round_number: int
    round_type: List[str]
    topics: List[str]
    verdict: Optional[str] = None
    description: Optional[str] = None


@dataclass
class InterviewExperience:
    company: Optional[str]
    role: Optional[str]
    level: Optional[str]
    years_of_experience: Optional[int]
    location: Optional[str]
    mode: Optional[str]
    source_url: Optional[str] = None
    additional_links: List[str] = field(default_factory=list)
    rounds: List[InterviewRound] = field(default_factory=list)
    final_verdict: Optional[str] = None
    description: Optional[str] = None

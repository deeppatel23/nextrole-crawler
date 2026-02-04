from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Question:
    topics: List[str]
    description: str
    links: List[str] = None


@dataclass
class InterviewExperience:
    company: Optional[str]
    role: Optional[str]
    location: Optional[str]
    title: Optional[str] = None
    source_url: Optional[str] = None
    source_tags: List[str] = field(default_factory=list)
    additional_links: List[str] = field(default_factory=list)
    questions: List[Question] = field(default_factory=list)
    final_verdict: Optional[str] = None

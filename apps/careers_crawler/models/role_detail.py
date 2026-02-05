from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class RoleDetail:
    # dedup
    job_hash: str
    
    # identity
    job_id: str
    company: str

    # core job info
    title: str
    role: Optional[str] = None
    category: Optional[str] = None

    # experience
    min_yoe: Optional[int] = None
    max_yoe: Optional[int] = None

    # location
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    description: Optional[str] = None
    skills: List[str] = field(default_factory=list)

    # links
    apply_link: Optional[str] = None
    source_type: Optional[str] = None

    # metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

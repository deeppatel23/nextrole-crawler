import re
from typing import Any, Dict, Optional


def get_by_path(obj: Dict[str, Any], path: str):
    """Safely get nested dict value using dot-notation."""
    value: Any = obj
    for key in path.split("."):
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value


def normalize_city(value: Optional[str]) -> Optional[str]:
    """Normalize city casing and remove stray semicolons/spaces around separators."""
    if not isinstance(value, str):
        return None

    cleaned = value.replace(";", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None

    return cleaned.lower().capitalize()

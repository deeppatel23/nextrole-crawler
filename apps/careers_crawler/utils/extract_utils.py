import re
import unicodedata
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
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None

    return cleaned.lower().capitalize()


def normalize_region(value: Optional[str]) -> Optional[str]:
    """Normalize state/region strings (strip diacritics + cleanup whitespace)."""
    if not isinstance(value, str):
        return None

    cleaned = value.replace(";", " ")
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None

    # Example from Uber API: "Karnātaka" -> "Karnataka"
    normalized = unicodedata.normalize("NFKD", cleaned)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return stripped

from typing import Any, Dict


def get_by_path(obj: Dict[str, Any], path: str):
    """Safely get nested dict value using dot-notation."""
    value: Any = obj
    for key in path.split("."):
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value

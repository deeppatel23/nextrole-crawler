from pathlib import Path
from typing import Any, Dict


def _parse_value(raw: str) -> Any:
    value = raw.strip()
    if value.lower() in ("true", "yes", "y", "1"):
        return True
    if value.lower() in ("false", "no", "n", "0"):
        return False
    if value.lower() in ("null", "none", ""):
        return None
    try:
        return int(value)
    except ValueError:
        return value


def read_state(path: str) -> Dict[str, Any]:
    state = {
        "one_time_data_load": True,
        "one_time_post_limit": 9999,
        "skip_posts": 0,
    }
    file_path = Path(path)
    if not file_path.exists():
        return state
    for line in file_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            continue
        key, raw_value = stripped.split(":", 1)
        key = key.strip()
        state[key] = _parse_value(raw_value)
    return state


def write_state(path: str, state: Dict[str, Any]) -> None:
    file_path = Path(path)
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)

    def _fmt(value: Any) -> str:
        if value is True:
            return "true"
        if value is False:
            return "false"
        if value is None:
            return "null"
        return str(value)

    lines = [
        f"one_time_data_load: {_fmt(state.get('one_time_data_load', True))}",
        f"one_time_post_limit: {_fmt(state.get('one_time_post_limit', 9999))}",
        f"skip_posts: {_fmt(state.get('skip_posts', 0))}",
    ]
    file_path.write_text("\n".join(lines) + "\n")


def set_skip_posts(path: str, skip_posts: int) -> None:
    state = read_state(path)
    state["skip_posts"] = skip_posts
    write_state(path, state)

import json


def resolve_value(value):
    if isinstance(value, list):
        if not value:
            return None
        v = value[0]
        if isinstance(v, str) and v.startswith("{"):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v
    return value


def get_by_path(obj, path):
    if not path:
        return None

    current = obj
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)

    return resolve_value(current)

import hashlib
from typing import List, Dict, Any
from models.role_detail import RoleDetail


def _get_by_path(obj: Dict[str, Any], path: str):
    """Safely get nested dict value using dot-notation"""
    value = obj
    for key in path.split("."):
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None
    return value


def _iter_postings(response: Any):
    """
    Supports BOTH Lever response shapes:
    1) Flat list of postings
    2) Grouped list: { title, postings[] }
    """
    if not isinstance(response, list):
        return

    for item in response:
        # Grouped response
        if isinstance(item, dict) and isinstance(item.get("postings"), list):
            for post in item["postings"]:
                yield post
        # Flat response
        elif isinstance(item, dict) and "id" in item:
            yield item


def parse(response: Any, source_cfg: Dict[str, Any]) -> List[RoleDetail]:
    roles: List[RoleDetail] = []

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")
    mapping = source_cfg.get("mapping", {})

    for post in _iter_postings(response):
        # ---- extract mapped fields
        mapped = {
            field: _get_by_path(post, path)
            for field, path in mapping.items()
        }

        job_id = mapped.get("job_id")
        if not job_id:
            continue

        # ---- stable dedup hash
        job_hash = hashlib.sha256(
            f"{company}|{job_id}".encode()
        ).hexdigest()

        # ---- prevent duplicate kwargs
        mapped.pop("job_id", None)

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id,
            company=company,
            source_type=source_type,
            # raw=post, # uncomment for debugging
            **mapped
        )

        roles.append(role)

    return roles

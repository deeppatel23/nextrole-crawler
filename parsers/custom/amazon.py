import hashlib
import re
from typing import List, Dict, Any
from models.role_detail import RoleDetail


def _get_by_path(obj: Dict[str, Any], path: str):
    """
    Supports dot-notation AND Amazon-style array fields.
    """
    value = obj
    for key in path.split("."):
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return None

    # Amazon fields are arrays → unwrap single values
    if isinstance(value, list):
        return value[0] if value else None

    return value


def _iter_amazon_postings(response: Dict[str, Any]):
    """
    Yields each job's `fields` dict
    """
    hits = response.get("searchHits", [])
    for hit in hits:
        fields = hit.get("fields")
        if isinstance(fields, dict):
            yield fields

def _normalize_amazon_job_link(apply_link: str | None) -> str | None:
    if not apply_link:
        return None

    match = re.search(r"/jobs/(\d+)", apply_link)
    if not match:
        return apply_link  # fallback safely

    job_id = match.group(1)
    return f"https://amazon.jobs/en/jobs/{job_id}"

def parse(response: Dict[str, Any], source_cfg: Dict[str, Any]) -> List[RoleDetail]:
    roles: List[RoleDetail] = []

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")
    mapping = source_cfg.get("mapping", {})

    for fields in _iter_amazon_postings(response):
        mapped = {
            field: _get_by_path(fields, path.replace("fields.", ""))
            for field, path in mapping.items()
        }

        job_id = mapped.get("job_id")
        if not job_id:
            continue

        job_hash = hashlib.sha256(
            f"{company}|{job_id}".encode()
        ).hexdigest()

        # avoid duplicate kwargs
        mapped.pop("job_id", None)
        mapped["apply_link"] = _normalize_amazon_job_link(mapped.get("apply_link"))

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id,
            company=company,
            source_type=source_type,
            # raw=fields,
            **mapped
        )

        roles.append(role)

    return roles

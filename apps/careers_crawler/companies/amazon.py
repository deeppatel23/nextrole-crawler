import re
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from models.role_detail import RoleDetail
from utils.extract_utils import get_by_path
from utils.hash_utils import generate_job_hash

API = {
    "method": "POST",
    "url": "https://amazon.jobs/api/jobs/search?is_als=true",
    "headers": {
        "Content-Type": "application/json",
        "Cookie": "",
    },
    "body": {
        "accessLevel": "EXTERNAL",
        "query": "",
        "size": 9999,
        "start": 0,
        "treatment": "OM",
        "sort": {
            "sortOrder": "DESCENDING",
            "sortType": "SCORE",
        },
        "filterFacets": [
            {
                "name": "country",
                "requestedFacetCount": 9999,
                "values": [
                    {
                        "name": "IN",
                    }
                ],
            }
        ],
    },
}

MAPPING = {
    "job_id": "artJobId",
    "title": "title",
    "role": "jobRole",
    "category": "jobFamily",
    "city": "normalizedCityName",
    "state": "normalizedStateName",
    "country": "normalizedCountryCode",
    "description": "description",
    "skills": "basicQualifications",
    "apply_link": "urlNextStep",
    "created_at": "createdDate",
    "updated_at": "updatedDate",
}


def _iter_amazon_fields(response: Dict[str, Any]):
    hits = response.get("searchHits", [])
    for hit in hits:
        fields = hit.get("fields")
        if isinstance(fields, dict):
            yield fields


def _normalize_amazon_job_link(apply_link: Optional[str]) -> Optional[str]:
    if not apply_link:
        return None

    match = re.search(r"/jobs/(\d+)", apply_link)
    if not match:
        return apply_link

    job_id = match.group(1)
    return f"https://amazon.jobs/en/jobs/{job_id}"


def _unwrap_list(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def fetch_roles(source_cfg: Dict[str, Any]) -> List[RoleDetail]:
    response = call_api(
        method=API["method"],
        url=API["url"],
        headers=API.get("headers"),
        body=API.get("body"),
    )

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")

    roles: List[RoleDetail] = []

    for fields in _iter_amazon_fields(response):
        mapped = {
            field: _unwrap_list(get_by_path(fields, path))
            for field, path in MAPPING.items()
        }

        job_id = mapped.get("job_id")
        if not job_id:
            continue

        mapped.pop("job_id", None)
        mapped["apply_link"] = _normalize_amazon_job_link(mapped.get("apply_link"))

        role = RoleDetail(
            job_hash=generate_job_hash(company, str(job_id)),
            job_id=str(job_id),
            company=company,
            source_type=source_type,
            # raw=fields,
            **mapped,
        )

        roles.append(role)

    return roles

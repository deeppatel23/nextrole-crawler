import math
import re
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from models.role_detail import RoleDetail
from config.config import OUTPUT_FILE
from utils.extract_utils import get_by_path
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles

API = {
    "method": "POST",
    "url": "https://amazon.jobs/api/jobs/search?is_als=true",
    "headers": {
        "Content-Type": "application/json",
        "Cookie": "",
    },
    "body": {
        "accessLevel": "EXTERNAL",
        "contentFilterFacets": [
            {
                "name": "primarySearchLabel",
                "requestedFacetCount": 9999,
            }
        ],
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
        "includeFacets": [],
        "jobTypeFacets": [],
        "locationFacets": [
            [
                {
                    "name": "country",
                    "requestedFacetCount": 9999,
                },
                {
                    "name": "normalizedStateName",
                    "requestedFacetCount": 9999,
                },
                {
                    "name": "normalizedCityName",
                    "requestedFacetCount": 9999,
                },
            ]
        ],
        "query": "",
        "size": 100,
        "start": 0,
        "treatment": "OM",
        "cookieInfo": "",
        "sort": {
            "sortOrder": "DESCENDING",
            "sortType": "SCORE",
        },
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


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    first_body = dict(API["body"])
    first_body["start"] = 0
    print(f"Amazon: start iteration 1 calling {API['url']}")
    response = call_api(
        method=API["method"],
        url=API["url"],
        headers=API.get("headers"),
        body=first_body,
    )

    found = response.get("found")
    page_size = first_body.get("size", 100)
    total_pages = 1
    if isinstance(found, int) and page_size:
        total_pages = max(1, math.ceil(found / page_size))

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")

    total_saved = 0

    def _accumulate_from_response(resp: Dict[str, Any]) -> List[RoleDetail]:
        roles: List[RoleDetail] = []
        for fields in _iter_amazon_fields(resp):
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

    first_batch = _accumulate_from_response(response)
    append_roles(OUTPUT_FILE, first_batch)
    total_saved += len(first_batch)
    print(f"Amazon: iteration 1 saved {len(first_batch)} jobs")

    for page_index in range(1, total_pages):
        start = page_index * page_size
        page_body = dict(API["body"])
        page_body["start"] = start
        print(f"Amazon: start iteration {page_index + 1} calling {API['url']}")
        page_response = call_api(
            method=API["method"],
            url=API["url"],
            headers=API.get("headers"),
            body=page_body,
        )
        batch = _accumulate_from_response(page_response)
        append_roles(OUTPUT_FILE, batch)
        total_saved += len(batch)
        print(f"Amazon: iteration {page_index + 1} saved {len(batch)} jobs")

    return total_saved

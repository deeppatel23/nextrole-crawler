from typing import Any, Dict, List

from clients.http_client import call_api
from models.role_detail import RoleDetail
from config.config import OUTPUT_FILE
from utils.extract_utils import get_by_path
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles
from utils.html_utils import fetch_visible_text
from utils.role_enricher import get_enrichment

API = {
    "method": "GET",
    "url": "https://jobs.lever.co/v0/postings/zeta?mode=json",
}

MAPPING = {
    "job_id": "id",
    "title": "text",
    "role": "categories.team",
    "category": "categories.department",
    "city": "categories.location",
    "country": "country",
    "workplace_type": "workplaceType",
    "description": "descriptionPlain",
    "apply_link": "applyUrl",
    "created_at": "createdAt",
}


def _iter_postings(response: Any):
    if not isinstance(response, list):
        return

    for item in response:
        if isinstance(item, dict) and isinstance(item.get("postings"), list):
            for post in item["postings"]:
                yield post
        elif isinstance(item, dict) and "id" in item:
            yield item


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    response = call_api(
        method=API["method"],
        url=API["url"],
    )

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")

    roles: List[RoleDetail] = []

    for post in _iter_postings(response):
        mapped = {field: get_by_path(post, path) for field, path in MAPPING.items()}

        job_id = mapped.get("job_id")
        if not job_id:
            continue

        mapped.pop("job_id", None)

        apply_link = f"https://jobs.lever.co/zeta/{job_id}"
        mapped["apply_link"] = apply_link
        page_text = fetch_visible_text(apply_link) or ""
        enrichment = get_enrichment(
            mapped.get("title"),
            mapped.get("description"),
            apply_link,
            page_text,
        )

        role = RoleDetail(
            job_hash=generate_job_hash(company, str(job_id)),
            job_id=str(job_id),
            company=company,
            source_type=source_type,
            skills=enrichment["skills"],
            min_yoe=enrichment["min_yoe"],
            max_yoe=enrichment["max_yoe"],
            **mapped,
        )

        roles.append(role)

    append_roles(OUTPUT_FILE, roles)
    return len(roles)

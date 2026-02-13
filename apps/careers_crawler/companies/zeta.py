"""
Zeta careers parser.
Backend API: Lever postings API (JSON); no explicit sort control (API order is used).
Logic: maps fields, enriches with page text, and appends RoleDetail entries in one batch.
De-dupe: checks mongo for existing job_hash before enrichment; append_roles handles file-level duplicates.
"""
from datetime import datetime
from typing import Any, Dict, List

from clients.http_client import call_api
from models.role_detail import RoleDetail
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from utils.extract_utils import get_by_path
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
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
    "role": "categories.department",
    "category": "categories.team",
    "city": "categories.location",
    "country": "country",
    "workplace_type": "workplaceType",
    "apply_link": "applyUrl",
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
    today = datetime.utcnow().date().isoformat()
    response = call_api(
        method=API["method"],
        url=API["url"],
    )

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")
    checker = MongoJobHashChecker()

    roles: List[RoleDetail] = []

    for post in _iter_postings(response):
        mapped = {field: get_by_path(post, path) for field, path in MAPPING.items()}

        job_id = mapped.get("job_id")
        if not job_id:
            continue

        job_hash = generate_job_hash(company, str(job_id))
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(f"Zeta: existing job_hash found for job_id={job_id}, skipping.")
            continue

        checker.record(job_hash)
        mapped.pop("job_id", None)

        apply_link = f"https://jobs.lever.co/zeta/{job_id}"
        mapped["apply_link"] = apply_link
        page_text = fetch_visible_text(apply_link) or ""
        enrichment = get_enrichment(
            mapped.get("title"),
            apply_link,
            page_text,
        )

        role = RoleDetail(
            job_hash=job_hash,
            job_id=str(job_id),
            company=company,
            source_type=source_type,
            skills=enrichment["skills"],
            min_yoe=enrichment["min_yoe"],
            max_yoe=enrichment["max_yoe"],
            created_at=today,
            **mapped,
        )

        roles.append(role)

    saved_count, stop_fetch = append_roles(OUTPUT_FILE, roles)
    if stop_fetch:
        print("Zeta: existing job_hash found, stopping further fetch.")
    return saved_count

"""
Arcesium careers parser.
Fetches all jobs from Greenhouse API once, filters India locations, sorts by id DESC,
skips saving if today <= last_saved, and stops early on max_saved_jobs or first existing job_hash.
"""
from __future__ import annotations

from datetime import datetime
from html import unescape
import re
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from config.config import OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://boards-api.greenhouse.io/v1/boards/arcesiumllc/jobs?content=true"
TARGET_CITIES = {"hyderabad", "bangalore", "bengaluru", "gurugram"}


def _normalize_locations(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts


def _extract_primary_city(raw: Optional[str]) -> Optional[str]:
    cities = _normalize_locations(raw)
    return cities[0] if cities else None


def _is_target_location(raw: Optional[str]) -> bool:
    cities = _normalize_locations(raw)
    for city in cities:
        if city.lower() in TARGET_CITIES:
            return True
    return False


def _extract_category(job: Dict[str, Any]) -> Optional[str]:
    departments = job.get("departments")
    if isinstance(departments, list) and departments:
        if isinstance(departments[0], dict):
            return departments[0].get("name")
    return None


def _html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = unescape(value)
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(
                    f"Arcesium: last_saved={last_saved} is >= today={today.isoformat()}, skipping."
                )
                return 0
        except ValueError:
            print(f"Arcesium: invalid last_saved={last_saved}, continuing.")

    print(f"Arcesium: fetching jobs from {API_URL}")
    response = call_api(method="GET", url=API_URL)
    jobs = response.get("jobs") if isinstance(response, dict) else None
    if not isinstance(jobs, list) or not jobs:
        print("Arcesium: no jobs found.")
        return 0

    filtered: List[Dict[str, Any]] = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        location = job.get("location", {})
        location_name = location.get("name") if isinstance(location, dict) else None
        if not _is_target_location(location_name):
            continue
        filtered.append(job)

    if not filtered:
        print("Arcesium: no jobs matched target locations.")
        return 0

    filtered.sort(key=lambda j: j.get("id", 0), reverse=True)
    today_str = today.isoformat()

    total_saved = 0

    for job in filtered:
        if total_saved >= max_saved:
            print(f"Arcesium: reached max_saved_jobs={max_saved}, stopping.")
            break

        job_id = job.get("id")
        if not job_id:
            continue

        location_name = None
        location = job.get("location")
        if isinstance(location, dict):
            location_name = location.get("name")

        city = _extract_primary_city(location_name)
        title = job.get("title")
        category = _extract_category(job)
        apply_link = job.get("absolute_url")
        content_text = _html_to_text(job.get("content"))

        enrichment = get_enrichment(
            title,
            apply_link,
            content_text,
        )

        role = RoleDetail(
            job_hash=generate_job_hash(company, str(job_id)),
            job_id=str(job_id),
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=category,
            city=city,
            country="India",
            apply_link=apply_link,
            skills=enrichment["skills"],
            min_yoe=enrichment["min_yoe"],
            max_yoe=enrichment["max_yoe"],
            created_at=today_str,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        total_saved += saved_count
        if stop_fetch:
            print("Arcesium: existing job_hash found, stopping further fetch.")
            break

    print(f"Arcesium: total saved {total_saved} jobs.")
    return total_saved

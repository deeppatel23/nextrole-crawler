"""
DE Shaw India careers parser.
Core logic: fetch the careers HTML, parse __NEXT_DATA__ JSON, iterate regularJobs,
and for each job/location build a job_hash, skip existing hashes, enrich via LLM,
and save until max_saved_jobs is reached. Skips all saving if today <= last_saved.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests

from config.config import OUTPUT_FILE, OUTPUT_DESTINATION
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

CAREERS_URL = "https://www.deshawindia.com/careers/work-with-us"
APPLY_URL_PREFIX = "https://www.deshawindia.com/careers/"


def _fetch_html(url: str, timeout: int = 30) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.text


def _extract_next_data(html: str) -> Dict[str, Any]:
    match = re.search(
        r"<script[^>]*id=['\"]__NEXT_DATA__['\"][^>]*>(.*?)</script>",
        html,
        flags=re.S | re.I,
    )
    if not match:
        return {}
    raw = match.group(1).strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _find_regular_jobs(obj: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(obj, dict):
        if "regularJobs" in obj and isinstance(obj["regularJobs"], list):
            return obj["regularJobs"]
        for value in obj.values():
            found = _find_regular_jobs(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_regular_jobs(item)
            if found is not None:
                return found
    return None


def _normalize_category(raw: Any) -> Optional[str]:
    if isinstance(raw, list) and raw:
        first = raw[0].get("name") if isinstance(raw[0], dict) else raw[0]
        return str(first) if first is not None else None
    if isinstance(raw, str):
        return raw
    return None


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "location"


def _iter_locations(raw: Any) -> List[Optional[str]]:
    if isinstance(raw, list) and raw:
        names = []
        for item in raw:
            if isinstance(item, dict):
                name = item.get("name")
                if name:
                    names.append(str(name))
            elif isinstance(item, str):
                names.append(item)
        return names or [None]
    if isinstance(raw, dict):
        name = raw.get("name")
        return [str(name)] if name else [None]
    return [None]


def _build_job_id(job_id: Any, location: Optional[str], multi_location: bool) -> str:
    base = str(job_id)
    if multi_location and location:
        return f"{base}-{_slugify(location)}"
    if multi_location and not location:
        return f"{base}-location"
    return base


def _build_apply_link(job_url: Optional[str]) -> Optional[str]:
    if not job_url:
        return None
    job_url = str(job_url).lstrip("/")
    return f"{APPLY_URL_PREFIX}{job_url}"


def _extract_job_data(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw.get("data") if isinstance(raw.get("data"), dict) else raw
    return None


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "DE Shaw"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "HTML").upper()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(
                    f"{company_label}: last_saved={last_saved} is >= today={today.isoformat()}, skipping."
                )
                return 0
        except ValueError:
            print(f"{company_label}: invalid last_saved={last_saved}, continuing.")

    print(f"{company_label}: fetching jobs from {CAREERS_URL}")
    html = _fetch_html(CAREERS_URL)
    next_data = _extract_next_data(html)
    jobs = _find_regular_jobs(next_data)
    if not jobs:
        print(f"{company_label}: regularJobs not found.")
        return 0

    checker = MongoJobHashChecker()
    today_str = today.isoformat()

    total_saved = 0

    for raw in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        data = _extract_job_data(raw)
        if not data:
            continue

        job_id = data.get("id")
        if not job_id:
            continue

        title = data.get("displayName")
        job_desc = data.get("jobDescription") or {}
        website_desc = None
        if isinstance(job_desc, dict):
            website_desc = job_desc.get("websiteDescription")
        locations = _iter_locations(data.get("jobMetadata").get("jobLocations"))
        multi_location = len(locations) > 1

        for location_name in locations:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                break

            job_id_value = _build_job_id(job_id, location_name, multi_location)
            job_hash = generate_job_hash(company, job_id_value)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                continue

            apply_link = _build_apply_link(data.get("jobUrl"))

            enrichment = get_enrichment(
                title,
                apply_link,
                website_desc,
            )

            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_id_value,
                company=company,
                source_type=source_type,
                title=title,
                role=None,
                category=enrichment["category"],
                city=location_name,
                state=None,
                country="India",
                workplace_type=None,
                apply_link=apply_link,
                skills=enrichment["skills"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                created_at=today_str,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
            if stop_fetch:
                print(f"{company_label}: existing job_hash found, skipping.")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

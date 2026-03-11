from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

COMPANY = "ShareChat"
CAREERS_LIST_API = "https://sharechat.com/api/careersList"
APPLY_BASE_URL = "https://sharechat.mynexthire.com/employer/jobs?src=careers&p="
SOURCE_TYPE = "API"
DEFAULT_LIMIT = 100


def _to_str(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_skills(text: str) -> List[str]:
    pool = text.lower()
    tokens = [
        "python",
        "java",
        "javascript",
        "sql",
        "aws",
        "kubernetes",
        "docker",
        "react",
        "node",
        "android",
        "ios",
        "product",
        "design",
        "marketing",
        "operations",
        "sales",
        "finance",
        "content",
    ]

    skills: List[str] = []
    for token in tokens:
        if token in pool:
            if token in {"aws", "ios", "sql"}:
                skills.append(token.upper())
            else:
                skills.append(token.title())
        if len(skills) >= 3:
            break
    return skills


def _build_apply_link(requisition_id: int) -> str:
    payload = {
        "pageType": "jd",
        "cvSource": "careers",
        "reqId": requisition_id,
        "requester": {"id": "", "code": "", "name": ""},
        "page": "careers",
        "bufilter": -1,
        "customFields": {},
    }
    encoded = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")
    return f"{APPLY_BASE_URL}{encoded}"


def _fetch_page(offset_token: Optional[str], limit: int) -> Dict[str, Any]:
    params: Dict[str, Any] = {"limit": limit}
    if offset_token:
        params["offsetToken"] = offset_token

    response = requests.get(
        CAREERS_LIST_API,
        params=params,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = COMPANY
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or SOURCE_TYPE).upper()
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

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0
    limit = _to_int(source_cfg.get("api_limit")) or DEFAULT_LIMIT

    print(f"{company_label}: fetching jobs from {CAREERS_LIST_API}?limit={limit}")

    offset_token: Optional[str] = None
    seen_tokens = set()

    while True:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        payload = _fetch_page(offset_token=offset_token, limit=limit)
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        careers_list = data.get("careersList") if isinstance(data.get("careersList"), list) else []
        has_next = bool(data.get("hasNext"))
        next_offset = _to_str(data.get("offsetToken")) or None

        if not careers_list:
            print(f"{company_label}: no careers rows returned, stopping.")
            break

        stop_all = False

        for group in careers_list:
            if not isinstance(group, dict):
                continue

            group_title = _to_str(group.get("title"))
            jobs = group.get("data") if isinstance(group.get("data"), list) else []

            for job in jobs:
                if total_saved >= max_saved:
                    print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                    stop_all = True
                    break

                if not isinstance(job, dict):
                    continue

                req_id_int = _to_int(job.get("requisitionId"))
                title = _to_str(job.get("requisitionTitle"))
                if req_id_int is None or not title:
                    continue

                job_id = str(req_id_int)
                job_hash = generate_job_hash(company, job_id)

                if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                    print(
                        f"{company_label}: existing job_hash found for job_id={job_id}, "
                        "stopping subsequent jobs (API assumed sorted)."
                    )
                    stop_all = True
                    break

                locations = job.get("officeLocationNames") if isinstance(job.get("officeLocationNames"), list) else []
                city = _to_str(locations[0]) if locations else None
                country = "India"

                min_yoe = _to_int(job.get("yrsOfExpMin"))
                max_yoe = _to_int(job.get("yrsOfExpMax"))

                department = _to_str(job.get("orgUnitName") or group_title)
                designation = _to_str(job.get("designation"))
                employment_type = _to_str(job.get("employmentType"))
                job_level = _to_str(job.get("jobLevel"))
                description = _to_str(job.get("jobDescription"))

                page_text = " ".join(
                    p
                    for p in [
                        title,
                        designation,
                        department,
                        employment_type,
                        job_level,
                        description,
                    ]
                    if p
                )

                skills = _extract_skills(page_text)
                category = match_category(
                    title=title,
                    department=department,
                    skills=skills,
                    page_text=page_text,
                    category_hint=department,
                )

                role = RoleDetail(
                    job_hash=job_hash,
                    job_id=job_id,
                    company=company,
                    source_type=source_type,
                    title=title,
                    role=designation or None,
                    category=category,
                    min_yoe=min_yoe,
                    max_yoe=max_yoe,
                    city=normalize_city(city),
                    state=None,
                    country=country,
                    workplace_type=None,
                    skills=skills,
                    apply_link=_build_apply_link(req_id_int),
                    created_at=now_iso,
                    updated_at=None,
                )

                saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
                if saved_count:
                    total_saved += saved_count
                    checker.record(job_hash)
                    print(f"{company_label}: saved job_id={job_id}")

                if stop_fetch:
                    print(
                        f"{company_label}: duplicate detected while saving job_id={job_id}, stopping further fetch."
                    )
                    stop_all = True
                    break

            if stop_all:
                break

        if stop_all:
            break

        if not has_next or not next_offset:
            print(f"{company_label}: reached final careers page.")
            break

        if next_offset in seen_tokens:
            print(f"{company_label}: repeated offsetToken encountered, stopping to avoid loop.")
            break

        seen_tokens.add(next_offset)
        offset_token = next_offset

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

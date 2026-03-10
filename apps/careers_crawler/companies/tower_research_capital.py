"""
Tower Research Capital careers via Greenhouse Boards API.

Source page:
https://tower-research.com/roles/

API:
GET https://boards-api.greenhouse.io/v1/boards/towerresearchcapital/jobs?content=true
"""
from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

API_URL = (
    "https://boards-api.greenhouse.io/v1/boards/"
    "towerresearchcapital/jobs?content=true"
)


def _clean_html_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = html.unescape(html.unescape(str(raw)))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_yoe(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    source = text.lower()

    mins: List[int] = []
    maxs: List[int] = []

    for lo, hi in re.findall(
        r"(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\s*(?:\+)?\s*(?:years|year|yrs|yr)",
        source,
    ):
        lo_i, hi_i = int(lo), int(hi)
        mins.append(min(lo_i, hi_i))
        maxs.append(max(lo_i, hi_i))

    for lo in re.findall(r"(\d{1,2})\s*\+\s*(?:years|year|yrs|yr)", source):
        mins.append(int(lo))

    if not mins and not maxs:
        return None, None
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _extract_skills(text: str) -> List[str]:
    if not text:
        return []
    pool = text.lower()
    candidates = [
        "python",
        "java",
        "c++",
        "c#",
        "linux",
        "sql",
        "kubernetes",
        "docker",
        "machine learning",
        "trading",
        "workday",
    ]
    out: List[str] = []
    for skill in candidates:
        if skill in pool:
            label = skill.upper() if skill in {"c++", "c#", "sql"} else skill.title()
            out.append(label)
        if len(out) >= 3:
            break
    return out


def _parse_location(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not raw:
        return None, None, None
    parts = [part.strip() for part in str(raw).split(",") if part.strip()]
    if not parts:
        return None, None, None
    city = parts[0]
    country = parts[-1] if len(parts) >= 2 else None
    state = parts[-2] if len(parts) >= 3 else None
    return city, state, country


def _workplace_type(text: str) -> Optional[str]:
    lower = text.lower()
    if "hybrid" in lower:
        return "hybrid"
    if "remote" in lower:
        return "remote"
    if "onsite" in lower or "on-site" in lower:
        return "onsite"
    return None


def _fetch_jobs() -> List[Dict[str, Any]]:
    payload = call_api(method="GET", url=API_URL)
    if isinstance(payload, dict):
        jobs = payload.get("jobs")
        if isinstance(jobs, list):
            return jobs
    return []


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Tower Research Capital"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
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

    print(f"{company_label}: fetching jobs from {API_URL}")
    jobs = _fetch_jobs()
    if not jobs:
        print(f"{company_label}: no jobs found.")
        return 0

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        if not isinstance(job, dict):
            continue

        job_id = job.get("id")
        title = job.get("title")
        if not job_id or not title:
            continue

        job_id_str = str(job_id)
        job_hash = generate_job_hash(company, job_id_str)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(
                f"{company_label}: existing job_hash found for job_id={job_id_str}, "
                "stopping subsequent jobs (API assumed sorted)."
            )
            break

        content = _clean_html_text(job.get("content"))
        skills = _extract_skills(content)
        min_yoe, max_yoe = _extract_yoe(f"{title} {content}")
        category_hint = None
        departments = job.get("departments")
        if isinstance(departments, list) and departments:
            first_dep = departments[0]
            if isinstance(first_dep, dict):
                category_hint = first_dep.get("name")

        office_name = None
        office_location = None
        offices = job.get("offices")
        if isinstance(offices, list) and offices:
            first_office = offices[0]
            if isinstance(first_office, dict):
                office_name = first_office.get("name")
                office_location = first_office.get("location")

        city, state, country = _parse_location(office_location or office_name)
        category = match_category(
            title=title,
            department=category_hint,
            skills=skills,
            page_text=content,
            category_hint=category_hint,
        )

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id_str,
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=category,
            min_yoe=min_yoe,
            max_yoe=max_yoe,
            city=normalize_city(city),
            state=state,
            country=country,
            workplace_type=_workplace_type(content),
            skills=skills,
            apply_link=job.get("absolute_url"),
            created_at=now_iso,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company_label}: saved job_id={job_id_str}")

        if stop_fetch:
            print(
                f"{company_label}: duplicate detected while saving job_id={job_id_str}, stopping further fetch."
            )
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved


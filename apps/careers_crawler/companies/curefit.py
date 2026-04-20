from __future__ import annotations

import html
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles


COMPANY = "CureFit"
CAREERS_URL = "https://careers.cult.fit/"
SOURCE_TYPE = "API"
API_BASE = "https://public.zwayam.com"
COMPANY_ID_B64 = "MTU0NzA="
DOMAIN = "careers.cult.fit"
LIST_API = f"{API_BASE}/jobs/search"
DETAIL_API = f"{API_BASE}/jobs-service/v1/jobs/careersite"


def _as_str(value: Any) -> str:
    return str(value or "").strip()


def _first_present(obj: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for key in keys:
        if key in obj and obj.get(key) not in (None, ""):
            return obj.get(key)
    return None


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_yoe(text: str) -> Tuple[Optional[int], Optional[int]]:
    source = (text or "").lower()
    if not source:
        return None, None
    mins: List[int] = []
    maxs: List[int] = []
    for lo, hi in re.findall(r"(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\s*(?:years|year|yrs|yr)", source):
        lo_i, hi_i = int(lo), int(hi)
        mins.append(min(lo_i, hi_i))
        maxs.append(max(lo_i, hi_i))
    for lo in re.findall(r"(\d{1,2})\s*\+\s*(?:years|year|yrs|yr)", source):
        mins.append(int(lo))
    if not mins and not maxs:
        return None, None
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _extract_skills(text: str) -> List[str]:
    pool = (text or "").lower()
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
        "sales",
        "operations",
        "fitness",
        "analytics",
        "communication",
    ]
    out: List[str] = []
    for token in tokens:
        if token in pool:
            out.append(token.upper() if token in {"aws", "sql"} else token.title())
        if len(out) >= 3:
            break
    return out


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("Result", "result", "data", "jobs", "records", "allJobs"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            nested = _extract_jobs(value)
            if nested:
                return nested
    return []


def _fetch_jobs() -> List[Dict[str, Any]]:
    payload = {
        "filterCri": json.dumps(
            {
                "paginationStartNo": 0,
                "selectedCall": "sort",
                "sortCriteria": {"name": "modifiedDate", "isAscending": False},
                "anyOfTheseWords": "",
            }
        ),
        "domain": DOMAIN,
        "companyId": COMPANY_ID_B64,
    }
    resp = requests.post(
        LIST_API,
        data=payload,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json() if resp.content else {}
    return _extract_jobs(data)


def _fetch_job_detail(job_url: str) -> Dict[str, Any]:
    payload = {
        "jobUrl": job_url,
        "campusURL": "empty",
        "externalSource": "CareerSite",
        "domain": DOMAIN,
        "companyId": COMPANY_ID_B64,
    }
    resp = requests.post(
        DETAIL_API,
        json=payload,
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 400:
        return {}
    try:
        data = resp.json() if resp.content else {}
    except Exception:
        return {}
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
                print(f"{company_label}: last_saved={last_saved} is >= today={today.isoformat()}, skipping.")
                return 0
        except ValueError:
            print(f"{company_label}: invalid last_saved={last_saved}, continuing.")

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0

    print(f"{company_label}: fetching jobs from Zwayam API.")
    try:
        jobs = _fetch_jobs()
    except Exception as exc:
        print(f"{company_label}: failed to fetch jobs list: {exc}")
        return 0

    if not jobs:
        print(f"{company_label}: no jobs returned from jobs/search.")
        return 0

    for row in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        source = row.get("_source") if isinstance(row.get("_source"), dict) else row
        if not isinstance(source, dict):
            continue

        job_url = _as_str(_first_present(source, ["jobUrl", "jobURL", "jobCode", "id"]))
        title = _as_str(_first_present(source, ["jobTitle", "title", "designation", "name"]))
        if not job_url or not title:
            continue

        job_hash = generate_job_hash(company, job_url)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            continue

        department = _as_str(_first_present(source, ["department", "jobDepartment", "team"]))
        location = _as_str(_first_present(source, ["location", "jobLocation", "city"]))
        city = location.split(",")[0].strip() if location else None
        page_text = _clean_text(
            " ".join(
                p
                for p in [
                    title,
                    department,
                    location,
                    _as_str(_first_present(source, ["jobDescription", "description", "summary"])),
                ]
                if p
            )
        )

        # Try detail API if main listing is sparse.
        detail = _fetch_job_detail(job_url)
        detail_src = detail.get("Result") if isinstance(detail.get("Result"), dict) else detail
        if isinstance(detail_src, dict):
            detail_text = _clean_text(
                " ".join(
                    _as_str(detail_src.get(k))
                    for k in ["jobDescription", "description", "roleDescription", "requirements"]
                )
            )
            if detail_text:
                page_text = f"{page_text} {detail_text}".strip()
            if not city:
                loc = _as_str(_first_present(detail_src, ["location", "jobLocation", "city"]))
                if loc:
                    city = loc.split(",")[0].strip()

        min_yoe = _first_present(source, ["minimumExperience", "minExperience", "experienceFrom"])
        max_yoe = _first_present(source, ["maximumExperience", "maxExperience", "experienceTo"])
        try:
            min_yoe = int(min_yoe) if min_yoe not in (None, "") else None
        except (TypeError, ValueError):
            min_yoe = None
        try:
            max_yoe = int(max_yoe) if max_yoe not in (None, "") else None
        except (TypeError, ValueError):
            max_yoe = None
        if min_yoe is None and max_yoe is None:
            parsed_min, parsed_max = _extract_yoe(page_text)
            min_yoe = parsed_min
            max_yoe = parsed_max

        skills = _extract_skills(page_text)
        category = match_category(
            title=title,
            department=department or None,
            skills=skills,
            page_text=page_text,
            category_hint=department or None,
        )

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_url,
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=category,
            min_yoe=min_yoe,
            max_yoe=max_yoe,
            city=normalize_city(city),
            state=None,
            country="India",
            workplace_type=None,
            skills=skills,
            apply_link=f"https://careers.cult.fit/cult/jobview/{job_url}",
            created_at=now_iso,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company_label}: saved job_id={job_url}")
        if stop_fetch:
            print(f"{company_label}: duplicate detected while saving job_id={job_url}, continuing.")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

from __future__ import annotations

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


COMPANY = "Ola"
CAREERS_URL = "https://www.olacabs.com/careers"
SOURCE_TYPE = "API"
TOKEN_API = "https://api.turbohire.co/api/token/noauth"
JOBS_API = "https://api.turbohire.co/api/careerpagev2/filteredjobs"


def _extract_org_id(careers_url: str) -> Optional[str]:
    match = re.search(
        r"careerpage/([0-9a-fA-F-]{36})",
        str(careers_url or ""),
        flags=re.I,
    )
    if match:
        return match.group(1)
    return None


def _as_str(value: Any) -> str:
    return str(value or "").strip()


def _first_present(obj: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for key in keys:
        if key in obj and obj.get(key) not in (None, ""):
            return obj.get(key)
    return None


def _extract_skills(text: str) -> List[str]:
    if not text:
        return []
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
        "analytics",
        "communication",
        "operations",
        "sales",
        "product",
    ]
    out: List[str] = []
    for token in tokens:
        if token in pool:
            out.append(token.upper() if token in {"aws", "sql"} else token.title())
        if len(out) >= 3:
            break
    return out


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


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [j for j in payload if isinstance(j, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("Result", "result", "data", "jobs", "jobRefs", "records"):
        value = payload.get(key)
        if isinstance(value, list):
            return [j for j in value if isinstance(j, dict)]
        if isinstance(value, dict):
            nested = _extract_jobs(value)
            if nested:
                return nested
    return []


def _fetch_token() -> Optional[str]:
    resp = requests.get(TOKEN_API, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        return None
    return _as_str(data.get("access_token")) or None


def _fetch_jobs(org_id: str, token: str) -> List[Dict[str, Any]]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payloads = [
        {},
        {
            "filterCri": {
                "paginationStartNo": 0,
                "selectedCall": "sort",
                "sortCriteria": {"name": "modifiedDate", "isAscending": False},
                "anyOfTheseWords": "",
            },
            "domain": "",
            "companyId": "",
        },
    ]

    for payload in payloads:
        resp = requests.post(
            f"{JOBS_API}?orgId={org_id}&pageType=CAREERPAGE",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if resp.status_code >= 400:
            continue
        data = resp.json() if resp.content else {}
        jobs = _extract_jobs(data)
        if jobs:
            return jobs
    return []


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = COMPANY
    company_label = source_cfg.get("company") or company
    careers_url = source_cfg.get("careers_url") or CAREERS_URL
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

    org_id = _extract_org_id(careers_url)
    if not org_id:
        print(f"{company_label}: unable to parse org id from careers_url={careers_url}")
        return 0

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0

    print(f"{company_label}: fetching TurboHire jobs for org_id={org_id}")
    try:
        token = _fetch_token()
        if not token:
            print(f"{company_label}: failed to obtain TurboHire noauth token.")
            return 0
        jobs = _fetch_jobs(org_id, token)
    except Exception as exc:
        print(f"{company_label}: TurboHire fetch failed: {exc}")
        return 0

    if not jobs:
        print(f"{company_label}: no jobs returned from TurboHire API.")
        return 0

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        source = job.get("_source") if isinstance(job.get("_source"), dict) else job
        if not isinstance(source, dict):
            continue

        job_url = _as_str(_first_present(source, ["jobUrl", "jobURL", "jobCode", "jobRefCode", "id"]))
        title = _as_str(_first_present(source, ["jobTitle", "title", "designation", "name"]))
        if not job_url or not title:
            continue

        job_id = job_url
        job_hash = generate_job_hash(company, job_id)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            continue

        location = _as_str(_first_present(source, ["location", "jobLocation", "city", "locationName"]))
        city = location.split(",")[0].strip() if location else None

        department = _as_str(_first_present(source, ["department", "team", "functionName"]))
        description = _as_str(_first_present(source, ["jobDescription", "description", "jd", "summary"]))
        extra_text = " ".join(
            p for p in [title, department, location, description] if p
        )

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
            parsed_min, parsed_max = _extract_yoe(extra_text)
            min_yoe = parsed_min
            max_yoe = parsed_max

        skills = _extract_skills(extra_text)
        category = match_category(
            title=title,
            department=department or None,
            skills=skills,
            page_text=extra_text,
            category_hint=department or None,
        )

        apply_link = f"https://olacareers.turbohire.co/jobview/{job_url}"
        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id,
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
            apply_link=apply_link,
            created_at=now_iso,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company_label}: saved job_id={job_id}")
        if stop_fetch:
            print(f"{company_label}: duplicate detected while saving job_id={job_id}, continuing.")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

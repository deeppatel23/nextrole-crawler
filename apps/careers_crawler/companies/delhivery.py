from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles


COMPANY = "Delhivery Courier And Logistics"
CAREERS_URL = "https://delhivery.darwinbox.in/ms/candidatev2/main/careers/allJobs"
SOURCE_TYPE = "API"
DEFAULT_JOBS_API = "https://delhivery.darwinbox.in/ms/candidateapi/job/alljobs?companyId=main"
HARDCODED_COOKIE = (
    "__cf_bm=_nU1aSVdmJFkMzl5JFL4LB4OFyl6T2Gsxgfi0H0qiN4-1773236569-1.0.1.1-"
    "jK5rQR_0MTAXifgoIvbwwzDfNf8VZD30shEn9zKparyKYtpbKMNtVdUoALXBEGuxwXrEPqLH7HpWek7RCB0iaQxYBcRUt8IpZ40LFJE.KZY; "
    "_cfuvid=eP63R57z_FvXMog9PJ6eVetBwsKK93KU.sbsEzKnV7o-1773236569962-0.0.1.1-604800000"
)


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
        "excel",
        "operations",
        "logistics",
        "supply chain",
        "analytics",
        "communication",
        "sales",
    ]
    out: List[str] = []
    for token in tokens:
        if token in pool:
            out.append(token.upper() if token == "sql" else token.title())
        if len(out) >= 3:
            break
    return out


def _extract_city(job: Dict[str, Any]) -> Optional[str]:
    # Prefer explicit city-like fields from location arrays.
    for key in ("officelocations_without_area", "tool_tip_locations", "officelocations_area"):
        values = job.get(key)
        if isinstance(values, list):
            for raw in values:
                text = _clean_text(str(raw or "")).replace("\r", " ")
                parts = [p.strip() for p in text.split(",") if p.strip()]
                if not parts:
                    continue
                candidate = parts[0]
                if candidate and candidate.lower() not in {"india", "multiple locations"}:
                    return candidate

    location = _clean_text(str(job.get("locations") or ""))
    if location and location.lower() != "multiple locations":
        parts = [p.strip() for p in location.split(",") if p.strip()]
        if parts:
            return parts[0]
    return None


def _extract_country(job: Dict[str, Any]) -> Optional[str]:
    country = _clean_text(str(job.get("country") or ""))
    return country or None


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


def _build_request_headers(careers_url: str, cookie_header: Optional[str] = None) -> Dict[str, str]:
    parsed = urlparse(careers_url)
    origin = (
        f"{parsed.scheme}://{parsed.netloc}"
        if parsed.scheme and parsed.netloc
        else "https://delhivery.darwinbox.in"
    )
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": careers_url,
    }
    if cookie_header:
        headers["Cookie"] = cookie_header
    return headers


def _fetch_jobs(jobs_api: str, careers_url: str, cookie_header: Optional[str]) -> List[Dict[str, Any]]:
    # Try the request exactly as shared curl first.
    headers = _build_request_headers(careers_url, cookie_header=cookie_header)
    resp = requests.post(
        jobs_api,
        json={},
        headers=headers,
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code < 400:
        payload = resp.json() if resp.content else {}
        jobs = _extract_jobs(payload)
        if jobs:
            return jobs

    # Fallback: use a warmed session (some runs need cloudflare cookies before POST).
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            )
        }
    )

    # Warm the session to collect Cloudflare/Darwinbox cookies before POST.
    session.get(careers_url, timeout=30)

    headers = _build_request_headers(careers_url, cookie_header=cookie_header)
    resp = session.post(
        jobs_api,
        json={},
        headers=headers,
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code == 403:
        raise RuntimeError(
            "Darwinbox API returned 403 (likely bot/cookie protection). "
            "Set delhivery_cookie in careers_sources.yaml or DELHIVERY_COOKIE env."
        )
    resp.raise_for_status()
    payload = resp.json() if resp.content else {}
    return _extract_jobs(payload)


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = source_cfg.get("company") or COMPANY
    source_type = (source_cfg.get("source_type") or SOURCE_TYPE).upper()
    careers_url = source_cfg.get("careers_url") or CAREERS_URL
    jobs_api = source_cfg.get("delhivery_jobs_api") or DEFAULT_JOBS_API
    cookie_header = HARDCODED_COOKIE
    max_saved = int(source_cfg.get("max_saved_jobs", 9999))
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(f"{company}: last_saved={last_saved} is >= today={today.isoformat()}, skipping.")
                return 0
        except ValueError:
            print(f"{company}: invalid last_saved={last_saved}, continuing.")

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0

    print(f"{company}: fetching jobs from Darwinbox API.")
    try:
        jobs = _fetch_jobs(jobs_api, careers_url, cookie_header)
    except Exception as exc:
        print(f"{company}: failed to fetch Darwinbox jobs API: {exc}")
        raise

    if not jobs:
        print(f"{company}: no jobs returned from Darwinbox API.")
        return 0

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company}: reached max_saved_jobs={max_saved}, stopping.")
            break

        job_id = str(job.get("id") or job.get("_id") or "").strip()
        title = _clean_text(str(job.get("title") or job.get("designation_name") or ""))
        if not job_id or not title:
            continue

        job_hash = generate_job_hash(COMPANY, job_id)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(f"{company}: existing job_hash found for job_id={job_id}, skipping.")
            continue

        department = _clean_text(str(job.get("department_name_only") or job.get("department_name") or ""))
        jd = _clean_text(str(job.get("jd") or job.get("jd_summary") or ""))
        experience = _clean_text(str(job.get("experience") or ""))
        location_blob = _clean_text(
            " ".join(
                str(v)
                for v in [
                    job.get("locations"),
                    job.get("officelocation_show_arr"),
                    job.get("officelocations_without_area"),
                    job.get("tool_tip_locations"),
                ]
                if v
            )
        )
        page_text = " ".join(x for x in [title, department, jd, experience, location_blob] if x)

        city = normalize_city(_extract_city(job))
        country = _extract_country(job)
        min_yoe, max_yoe = _extract_yoe(f"{experience} {page_text}")
        skills = _extract_skills(page_text)
        category = match_category(
            title=title,
            department=department or None,
            skills=skills,
            page_text=page_text,
            category_hint=department or None,
        )

        apply_link = f"{careers_url.rstrip('/')}/job/{job_id}"
        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id,
            company=COMPANY,
            source_type=source_type,
            title=title,
            role=None,
            category=category,
            min_yoe=min_yoe,
            max_yoe=max_yoe,
            city=city,
            state=None,
            country=country,
            workplace_type="remote" if int(job.get("is_remote") or 0) == 1 else None,
            skills=skills,
            apply_link=apply_link,
            created_at=now_iso,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company}: saved job_id={job_id}")
        if stop_fetch:
            print(f"{company}: duplicate detected while saving job_id={job_id}, stopping.")
            break

    print(f"{company}: total saved {total_saved} jobs.")
    return total_saved

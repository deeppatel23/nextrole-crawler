from __future__ import annotations

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

COMPANY = "ByteDance"
LIST_API = "https://jobs.bytedance.com/api/v1/career/job/list"
DETAIL_API = "https://jobs.bytedance.com/api/v1/career/job/detail"
PAGE_LIMIT = 50


def _as_str(v: Any) -> str:
    return str(v or "").strip()


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in ("job_list", "jobList", "positions", "items", "list"):
            v = data.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

    for k in ("job_list", "jobList", "positions", "items", "list", "result"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            nested = _extract_jobs(v)
            if nested:
                return nested
    return []


def _extract_skills(text: str) -> List[str]:
    pool = (text or "").lower()
    tokens = [
        "python", "java", "javascript", "sql", "aws", "kubernetes", "docker",
        "react", "node", "go", "golang", "communication", "analytics", "machine learning",
    ]
    out: List[str] = []
    for t in tokens:
        if t in pool:
            out.append(t.upper() if t in {"aws", "sql"} else ("Go" if t == "go" else t.title()))
        if len(out) >= 3:
            break
    return out


def _extract_yoe(text: str) -> Tuple[Optional[int], Optional[int]]:
    import re

    src = (text or "").lower()
    mins: List[int] = []
    maxs: List[int] = []
    for lo, hi in re.findall(r"(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\s*(?:years|year|yrs|yr)", src):
        lo_i, hi_i = int(lo), int(hi)
        mins.append(min(lo_i, hi_i))
        maxs.append(max(lo_i, hi_i))
    for lo in re.findall(r"(\d{1,2})\s*\+\s*(?:years|year|yrs|yr)", src):
        mins.append(int(lo))
    if not mins and not maxs:
        return None, None
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _fetch_list(offset: int, limit: int) -> List[Dict[str, Any]]:
    params = {
        "limit": limit,
        "offset": offset,
        "location": "",
        "function_id": "",
        "keyword": "",
    }
    r = requests.get(LIST_API, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    return _extract_jobs(r.json() if r.content else {})


def _fetch_detail(job_id: str) -> Dict[str, Any]:
    r = requests.get(DETAIL_API, params={"id": job_id}, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    if r.status_code >= 400:
        return {}
    try:
        payload = r.json() if r.content else {}
    except Exception:
        return {}
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return {}


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = COMPANY
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            if today <= datetime.strptime(last_saved, "%Y-%m-%d").date():
                print(f"{company_label}: last_saved={last_saved} is >= today={today.isoformat()}, skipping.")
                return 0
        except ValueError:
            pass

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0
    offset = 0

    print(f"{company_label}: fetching jobs from ByteDance API.")
    while total_saved < max_saved:
        try:
            rows = _fetch_list(offset, PAGE_LIMIT)
        except Exception as exc:
            print(f"{company_label}: list fetch failed at offset={offset}: {exc}")
            break
        if not rows:
            break

        for row in rows:
            if total_saved >= max_saved:
                break

            job_id = _as_str(row.get("id") or row.get("job_id") or row.get("requisition_id"))
            title = _as_str(row.get("title") or row.get("job_title") or row.get("name"))
            if not job_id or not title:
                continue

            job_hash = generate_job_hash(company, job_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                continue

            detail = _fetch_detail(job_id)
            department = _as_str(detail.get("department") or row.get("department"))
            location = _as_str(detail.get("location") or row.get("location"))
            city = location.split(",")[0].strip() if location else None
            description = _as_str(detail.get("description") or detail.get("job_description") or row.get("description"))
            text = " ".join(x for x in [title, department, location, description] if x)

            min_yoe = detail.get("min_experience") or row.get("min_experience")
            max_yoe = detail.get("max_experience") or row.get("max_experience")
            try:
                min_yoe = int(min_yoe) if min_yoe not in (None, "") else None
            except Exception:
                min_yoe = None
            try:
                max_yoe = int(max_yoe) if max_yoe not in (None, "") else None
            except Exception:
                max_yoe = None
            if min_yoe is None and max_yoe is None:
                min_yoe, max_yoe = _extract_yoe(text)

            skills = _extract_skills(text)
            category = match_category(
                title=title,
                department=department or None,
                skills=skills,
                page_text=text,
                category_hint=department or None,
            )

            apply_link = _as_str(row.get("job_url") or detail.get("job_url")) or f"https://jobs.bytedance.com/en/position/{job_id}"
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
                country=None,
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

        if len(rows) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

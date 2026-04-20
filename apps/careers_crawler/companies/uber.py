from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from urllib.parse import urlparse

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

COMPANY = "Uber"
LIST_API = "https://www.uber.com/api/loadFeed"
PAGE_LIMIT = 20
DEFAULT_CAREERS_URL = "https://www.uber.com/in/en/careers/list/"


def _as_str(v: Any) -> str:
    return str(v or "").strip()


def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    # Try common keys first
    for k in ("jobs", "results", "items", "list", "data"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    # Recursive fallback
    out: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            title = _as_str(node.get("title") or node.get("jobTitle"))
            link = _as_str(node.get("url") or node.get("link") or node.get("applyLink"))
            jid = _as_str(node.get("id") or node.get("jobId") or node.get("slug"))
            if title and (link or jid):
                out.append(node)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)

    walk(payload)
    return out


def _extract_skills(text: str) -> List[str]:
    pool = (text or "").lower()
    tokens = ["python", "java", "javascript", "sql", "aws", "kubernetes", "docker", "react", "node", "analytics", "communication"]
    out: List[str] = []
    for t in tokens:
        if t in pool:
            out.append(t.upper() if t in {"aws", "sql"} else t.title())
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


def _build_headers(careers_url: str) -> Dict[str, str]:
    parsed = urlparse(careers_url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "https://www.uber.com"
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": origin,
        "Referer": careers_url,
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest",
    }


def _fetch_list(page: int, limit: int, careers_url: str) -> List[Dict[str, Any]]:
    session = requests.Session()
    headers = _build_headers(careers_url)
    session.headers.update({"User-Agent": headers["User-Agent"]})

    # Warm up cookies/anti-bot state before calling the feed API.
    try:
        session.get(careers_url, timeout=30)
    except Exception:
        pass

    body = {"query": "", "filters": [], "page": page, "limit": limit}
    params = {"bodyParams": json.dumps(body, separators=(",", ":")), "urlType": "career"}
    q = "&".join(f"{k}={quote(str(v), safe='') if k=='bodyParams' else quote(str(v), safe='')}" for k, v in params.items())
    url = f"{LIST_API}?{q}"

    # Primary attempt (GET like browser network call).
    r = session.get(url, headers=headers, timeout=30)
    if r.status_code == 403:
        # Fallback variant: some edges require generic sec-site semantics.
        headers_alt = dict(headers)
        headers_alt["Sec-Fetch-Site"] = "same-origin"
        headers_alt.pop("X-Requested-With", None)
        r = session.get(url, headers=headers_alt, timeout=30)

    r.raise_for_status()
    payload = r.json() if r.content else {}
    return _extract_jobs(payload)


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = COMPANY
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    careers_url = source_cfg.get("careers_url") or DEFAULT_CAREERS_URL
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
    page = 0

    print(f"{company_label}: fetching jobs from Uber careers API.")
    while total_saved < max_saved:
        try:
            rows = _fetch_list(page=page, limit=PAGE_LIMIT, careers_url=careers_url)
        except Exception as exc:
            print(f"{company_label}: list fetch failed at page={page}: {exc}")
            break
        if not rows:
            break

        for row in rows:
            if total_saved >= max_saved:
                break

            title = _as_str(row.get("title") or row.get("jobTitle") or row.get("name"))
            job_id = _as_str(row.get("id") or row.get("jobId") or row.get("slug") or row.get("requisitionId"))
            apply_link = _as_str(row.get("url") or row.get("link") or row.get("applyLink"))
            if not job_id and apply_link:
                job_id = apply_link.rsplit("/", 1)[-1]
            if not title or not job_id:
                continue
            if not apply_link:
                apply_link = f"https://www.uber.com/in/en/careers/list/{job_id}/"

            job_hash = generate_job_hash(company, job_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                continue

            location = _as_str(row.get("location") or row.get("city") or row.get("primaryLocation"))
            city = location.split(",")[0].strip() if location else None
            department = _as_str(row.get("team") or row.get("department") or row.get("function"))
            description = _as_str(row.get("description") or row.get("summary") or row.get("snippet"))
            text = " ".join(x for x in [title, department, location, description] if x)
            min_yoe, max_yoe = _extract_yoe(text)
            skills = _extract_skills(text)
            category = match_category(
                title=title,
                department=department or None,
                skills=skills,
                page_text=text,
                category_hint=department or None,
            )

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
        page += 1

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

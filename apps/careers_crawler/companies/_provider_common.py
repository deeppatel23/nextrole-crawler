"""
Generic multi-provider careers handler.

Supports:
1) Greenhouse boards
2) Workable public widget API
3) Lever postings API
4) Fallback HTML link extraction
"""
from __future__ import annotations

import html
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

UNRESOLVED_LOG_PATH = Path("apps/careers_crawler/output/unresolved_careers_pages.log")


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
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
    tokens = [
        "python",
        "java",
        "javascript",
        "react",
        "node",
        "sql",
        "aws",
        "kubernetes",
        "docker",
        "golang",
        "c++",
        "machine learning",
        "sales",
    ]
    out: List[str] = []
    for token in tokens:
        if token in pool:
            label = token.upper() if token in {"aws", "sql", "c++"} else token.title()
            out.append(label)
        if len(out) >= 3:
            break
    return out


def _extract_city_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r"\bLocation(?:s)?\s*[:\-]\s*([A-Za-z .\-]+)",
        r"\bBased in\s+([A-Za-z .\-]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue
        raw = (match.group(1) or "").strip(" .,-")
        if not raw:
            continue
        return raw.split(",")[0].strip() or None
    return None


def _parse_location(raw: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not raw:
        return None, None, None
    parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    if not parts:
        return None, None, None
    city = parts[0]
    state = parts[-2] if len(parts) >= 3 else None
    country = parts[-1] if len(parts) >= 2 else None
    return city, state, country


def _detect_greenhouse_board(html_text: str) -> Optional[str]:
    if not html_text:
        return None
    match = re.search(r"boards\.greenhouse\.io/embed/job_board/js\?for=([a-z0-9_-]+)", html_text, flags=re.I)
    if match:
        return match.group(1)
    match = re.search(r"boards-api\.greenhouse\.io/v1/boards/([a-z0-9_-]+)/jobs", html_text, flags=re.I)
    if match:
        return match.group(1)
    return None


def _guess_workable_account(url: str) -> Optional[str]:
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.strip("/")
    if "apply.workable.com" in host and path:
        return path.split("/")[0]
    return None


def _guess_greenhouse_board(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")
    if "job-boards.greenhouse.io" in host and path:
        return path.split("/")[0]
    if "boards.greenhouse.io" in host and path:
        return path.split("/")[0]
    return None


def _guess_lever_company(url: str, html_text: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.strip("/")
    if "jobs.lever.co" in host and path:
        return path.split("/")[0]
    m = re.search(r"https?://jobs\.lever\.co/([a-z0-9_-]+)", html_text or "", flags=re.I)
    if m:
        return m.group(1)
    return None


def _detect_workday_jobs_api(careers_url: str, html_text: str) -> Optional[str]:
    if not html_text:
        return None
    absolute = re.search(
        r"https?://[^\"'\s]+/wday/cxs/[^\"'\s]+/jobs(?:\?[^\"'\s]*)?",
        html_text,
        flags=re.I,
    )
    if absolute:
        return absolute.group(0)

    relative = re.search(
        r"(/wday/cxs/[^\"'\s]+/jobs(?:\?[^\"'\s]*)?)",
        html_text,
        flags=re.I,
    )
    if relative:
        return urljoin(careers_url, relative.group(1))

    return None


def _log_unresolved(company_label: str, careers_url: str, reason: str) -> None:
    UNRESOLVED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    now_iso = datetime.utcnow().isoformat()
    line = f"{now_iso}\t{company_label}\t{careers_url}\t{reason}\n"
    with UNRESOLVED_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line)


def _fetch_html(url: str) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


def _fetch_workday(api_url: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    limit = 20

    while True:
        resp = requests.post(
            api_url,
            json={"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""},
            headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        if not isinstance(payload, dict):
            break

        rows = payload.get("jobPostings")
        if not isinstance(rows, list) or not rows:
            break

        out.extend([r for r in rows if isinstance(r, dict)])
        total = int(payload.get("total") or 0)
        offset += len(rows)
        if offset >= total or len(rows) < limit:
            break

    return out


def _fetch_greenhouse(board: str) -> List[Dict[str, Any]]:
    api = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    payload = requests.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).json()
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    return jobs if isinstance(jobs, list) else []


def _fetch_workable(account: str) -> List[Dict[str, Any]]:
    api = f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true"
    payload = requests.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).json()
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    return jobs if isinstance(jobs, list) else []


def _fetch_lever(company_slug: str) -> List[Dict[str, Any]]:
    api = f"https://api.lever.co/v0/postings/{company_slug}?mode=json"
    payload = requests.get(api, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).json()
    return payload if isinstance(payload, list) else []


def _extract_html_jobs(careers_url: str, html_text: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not html_text:
        return out
    for href, title in re.findall(
        r"<a[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
        html_text,
        flags=re.I | re.S,
    ):
        clean_title = _clean_text(title)
        full = urljoin(careers_url, html.unescape(href))
        low = full.lower()
        if not clean_title:
            continue
        if not any(k in low for k in ["job", "career", "opening", "position", "workable", "greenhouse", "lever"]):
            continue
        if len(clean_title) < 3 or len(clean_title) > 140:
            continue
        out.append({"id": full, "title": clean_title, "url": full})
        if len(out) >= 300:
            break
    return out


def _build_role(
    company: str,
    source_type: str,
    now_iso: str,
    job_id: str,
    title: str,
    apply_link: str,
    page_text: str = "",
    city: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    category_hint: Optional[str] = None,
) -> RoleDetail:
    skills = _extract_skills(page_text)
    min_yoe, max_yoe = _extract_yoe(f"{title} {page_text}")
    category = match_category(
        title=title,
        department=category_hint,
        skills=skills,
        page_text=page_text,
        category_hint=category_hint,
    )
    return RoleDetail(
        job_hash=generate_job_hash(company, job_id),
        job_id=job_id,
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
        workplace_type=None,
        skills=skills,
        apply_link=apply_link,
        created_at=now_iso,
        updated_at=None,
    )


def _enrich_role_from_detail_page(role: RoleDetail) -> None:
    needs_city = not role.city
    needs_skills = not role.skills
    needs_yoe = role.min_yoe is None and role.max_yoe is None
    if not (needs_city or needs_skills or needs_yoe):
        return
    if not role.apply_link:
        return

    try:
        detail_html = _fetch_html(role.apply_link)
    except Exception:
        return

    detail_text = _clean_text(detail_html)
    if not detail_text:
        return

    if needs_city:
        role.city = normalize_city(_extract_city_from_text(detail_text))

    if needs_skills:
        role.skills = _extract_skills(detail_text)

    if needs_yoe:
        min_yoe, max_yoe = _extract_yoe(detail_text)
        if min_yoe is not None:
            role.min_yoe = min_yoe
        if max_yoe is not None:
            role.max_yoe = max_yoe


def _save_roles_sorted(
    roles: Iterable[RoleDetail],
    checker: MongoJobHashChecker,
    company_label: str,
    max_saved: int,
    total_saved: int,
) -> Tuple[int, bool]:
    for role in roles:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            return total_saved, True

        if OUTPUT_DESTINATION == "MONGO" and checker.exists(role.job_hash):
            print(
                f"{company_label}: existing job_hash found for job_id={role.job_id}, "
                "stopping subsequent jobs (sorted feed)."
            )
            return total_saved, True

        _enrich_role_from_detail_page(role)

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(role.job_hash)
            print(f"{company_label}: saved job_id={role.job_id}")

        if stop_fetch:
            print(
                f"{company_label}: duplicate detected while saving job_id={role.job_id}, stopping."
            )
            return total_saved, True

    return total_saved, False


def fetch_company_jobs(
    source_cfg: Dict[str, Any],
    *,
    default_company: str,
    default_careers_url: str,
    default_source_type: str = "HTML",
    greenhouse_board: Optional[str] = None,
    workable_account: Optional[str] = None,
    lever_company: Optional[str] = None,
) -> int:
    company = source_cfg.get("company") or default_company
    company_label = company
    source_type = (source_cfg.get("source_type") or default_source_type).upper()
    careers_url = source_cfg.get("careers_url") or default_careers_url
    if not careers_url:
        print(f"{company_label}: missing careers_url, skipping.")
        return 0

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

    # Direct provider routing from URL/config (avoid dependency on fetching landing HTML first).
    direct_workday_api = source_cfg.get("workday_jobs_api")
    if direct_workday_api:
        print(f"{company_label}: using configured Workday jobs api={direct_workday_api}")
        try:
            jobs = _fetch_workday(direct_workday_api)
        except Exception as exc:
            print(f"{company_label}: configured Workday api fetch failed: {exc}")
            _log_unresolved(company_label, careers_url, f"configured_workday_api_failed: {exc}")
            jobs = []

        roles: List[RoleDetail] = []
        for job in jobs:
            title = str(job.get("title") or "").strip()
            external_path = str(job.get("externalPath") or "").strip()
            if not title or not external_path:
                continue

            location_text = str(job.get("locationsText") or "").strip()
            if not location_text:
                fields = job.get("bulletFields") if isinstance(job.get("bulletFields"), list) else []
                if len(fields) > 1:
                    location_text = str(fields[1] or "").strip()
            city, state, country = _parse_location(location_text)
            apply_link = urljoin(careers_url, external_path)
            page_text = _clean_text(
                " ".join(
                    str(job.get(k) or "")
                    for k in ("title", "postedOn", "timeType", "jobReqId")
                )
            )
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=external_path,
                    title=title,
                    apply_link=apply_link,
                    page_text=page_text,
                    city=city,
                    state=state,
                    country=country,
                )
            )

        if not roles:
            _log_unresolved(company_label, careers_url, "configured_workday_api_no_roles")
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    direct_greenhouse = source_cfg.get("greenhouse_board") or greenhouse_board or _guess_greenhouse_board(careers_url)
    if direct_greenhouse:
        print(f"{company_label}: using direct greenhouse board={direct_greenhouse}")
        try:
            jobs = _fetch_greenhouse(direct_greenhouse)
        except Exception as exc:
            print(f"{company_label}: direct greenhouse fetch failed: {exc}")
            _log_unresolved(company_label, careers_url, f"direct_greenhouse_fetch_failed: {exc}")
            jobs = []
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("id") or "")
            title = str(job.get("title") or "").strip()
            apply_link = str(job.get("absolute_url") or "").strip()
            if not job_id or not title or not apply_link:
                continue
            departments = job.get("departments")
            category_hint = None
            if isinstance(departments, list) and departments and isinstance(departments[0], dict):
                category_hint = departments[0].get("name")
            offices = job.get("offices")
            office_loc = None
            if isinstance(offices, list) and offices and isinstance(offices[0], dict):
                office_loc = offices[0].get("location") or offices[0].get("name")
            city, state, country = _parse_location(office_loc)
            content = _clean_text(job.get("content"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=job_id,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=category_hint,
                )
            )
        if not roles:
            _log_unresolved(company_label, careers_url, "direct_greenhouse_no_roles")
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    direct_workable = source_cfg.get("workable_account") or workable_account or _guess_workable_account(careers_url)
    if direct_workable:
        print(f"{company_label}: using direct workable account={direct_workable}")
        try:
            jobs = _fetch_workable(direct_workable)
        except Exception as exc:
            print(f"{company_label}: direct workable fetch failed: {exc}")
            _log_unresolved(company_label, careers_url, f"direct_workable_fetch_failed: {exc}")
            jobs = []
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            shortcode = str(job.get("shortcode") or job.get("id") or "").strip()
            title = str(job.get("title") or "").strip()
            apply_link = str(job.get("url") or "").strip()
            if not shortcode or not title or not apply_link:
                continue
            location = job.get("location")
            location_name = location.get("location_str") if isinstance(location, dict) else None
            city, state, country = _parse_location(location_name)
            content = _clean_text(job.get("description"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=shortcode,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=job.get("department"),
                )
            )
        if not roles:
            _log_unresolved(company_label, careers_url, "direct_workable_no_roles")
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    direct_lever = source_cfg.get("lever_company") or lever_company or _guess_lever_company(careers_url, "")
    if direct_lever:
        print(f"{company_label}: using direct lever company={direct_lever}")
        try:
            jobs = _fetch_lever(direct_lever)
        except Exception as exc:
            print(f"{company_label}: direct lever fetch failed: {exc}")
            _log_unresolved(company_label, careers_url, f"direct_lever_fetch_failed: {exc}")
            jobs = []
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("id") or "").strip()
            title = str(job.get("text") or "").strip()
            apply_link = str(job.get("hostedUrl") or "").strip()
            if not job_id or not title or not apply_link:
                continue
            categories = job.get("categories") if isinstance(job.get("categories"), dict) else {}
            city, state, country = _parse_location(categories.get("location"))
            content = _clean_text(job.get("descriptionPlain") or job.get("description"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=job_id,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=categories.get("team"),
                )
            )
        if not roles:
            _log_unresolved(company_label, careers_url, "direct_lever_no_roles")
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    print(f"{company_label}: fetching careers page {careers_url}")
    try:
        page_html = _fetch_html(careers_url)
    except Exception as exc:
        print(f"{company_label}: failed to fetch careers_url: {exc}")
        _log_unresolved(company_label, careers_url, f"careers_page_fetch_failed: {exc}")
        return 0

    # Provider 0: Workday
    workday_jobs_api = source_cfg.get("workday_jobs_api") or _detect_workday_jobs_api(careers_url, page_html)
    if workday_jobs_api:
        print(f"{company_label}: detected Workday jobs api={workday_jobs_api}")
        try:
            jobs = _fetch_workday(workday_jobs_api)
        except Exception as exc:
            print(f"{company_label}: workday api fetch failed: {exc}")
            _log_unresolved(company_label, careers_url, f"workday_api_fetch_failed: {exc}")
            jobs = []

        roles: List[RoleDetail] = []
        for job in jobs:
            title = str(job.get("title") or "").strip()
            external_path = str(job.get("externalPath") or "").strip()
            job_id = external_path
            if not title or not external_path:
                continue

            apply_link = urljoin(careers_url, external_path)

            location_text = str(job.get("locationsText") or "").strip()
            if not location_text:
                fields = job.get("bulletFields") if isinstance(job.get("bulletFields"), list) else []
                if len(fields) > 1:
                    location_text = str(fields[1] or "").strip()
            city, state, country = _parse_location(location_text)
            page_text = _clean_text(
                " ".join(
                    str(job.get(k) or "")
                    for k in ("title", "postedOn", "timeType", "jobReqId")
                )
            )

            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=job_id,
                    title=title,
                    apply_link=apply_link,
                    page_text=page_text,
                    city=city,
                    state=state,
                    country=country,
                )
            )

        if not roles:
            _log_unresolved(company_label, careers_url, "workday_detected_but_no_roles")
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    # Provider 1: Greenhouse
    board = source_cfg.get("greenhouse_board") or greenhouse_board or _detect_greenhouse_board(page_html)
    if board:
        print(f"{company_label}: detected greenhouse board={board}")
        jobs = _fetch_greenhouse(board)
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("id") or "")
            title = str(job.get("title") or "").strip()
            apply_link = str(job.get("absolute_url") or "").strip()
            if not job_id or not title or not apply_link:
                continue
            departments = job.get("departments")
            category_hint = None
            if isinstance(departments, list) and departments and isinstance(departments[0], dict):
                category_hint = departments[0].get("name")
            offices = job.get("offices")
            office_loc = None
            if isinstance(offices, list) and offices and isinstance(offices[0], dict):
                office_loc = offices[0].get("location") or offices[0].get("name")
            city, state, country = _parse_location(office_loc)
            content = _clean_text(job.get("content"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=job_id,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=category_hint,
                )
            )
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    # Provider 2: Workable
    workable = source_cfg.get("workable_account") or workable_account or _guess_workable_account(careers_url)
    if workable:
        print(f"{company_label}: detected workable account={workable}")
        jobs = _fetch_workable(workable)
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            shortcode = str(job.get("shortcode") or job.get("id") or "").strip()
            title = str(job.get("title") or "").strip()
            apply_link = str(job.get("url") or "").strip()
            if not shortcode or not title or not apply_link:
                continue
            location = job.get("location")
            location_name = location.get("location_str") if isinstance(location, dict) else None
            city, state, country = _parse_location(location_name)
            content = _clean_text(job.get("description"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=shortcode,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=job.get("department"),
                )
            )
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    # Provider 3: Lever
    lever = source_cfg.get("lever_company") or lever_company or _guess_lever_company(careers_url, page_html)
    if lever:
        print(f"{company_label}: detected lever company={lever}")
        jobs = _fetch_lever(lever)
        roles: List[RoleDetail] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("id") or "").strip()
            title = str(job.get("text") or "").strip()
            apply_link = str(job.get("hostedUrl") or "").strip()
            if not job_id or not title or not apply_link:
                continue
            categories = job.get("categories") if isinstance(job.get("categories"), dict) else {}
            city, state, country = _parse_location(categories.get("location"))
            content = _clean_text(job.get("descriptionPlain") or job.get("description"))
            roles.append(
                _build_role(
                    company=company,
                    source_type="API",
                    now_iso=now_iso,
                    job_id=job_id,
                    title=title,
                    apply_link=apply_link,
                    page_text=content,
                    city=city,
                    state=state,
                    country=country,
                    category_hint=categories.get("team"),
                )
            )
        total_saved, _ = _save_roles_sorted(roles, checker, company_label, max_saved, total_saved)
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    # Fallback: HTML anchor extraction (unsorted, so do not stop on first duplicate).
    print(f"{company_label}: using HTML fallback parsing.")
    html_jobs = _extract_html_jobs(careers_url, page_html)
    if not html_jobs:
        _log_unresolved(
            company_label,
            careers_url,
            "no_provider_detected_and_no_html_jobs_extracted",
        )
    for row in html_jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break
        job_id = row["id"]
        job_hash = generate_job_hash(company, job_id)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            continue
        role = _build_role(
            company=company,
            source_type=source_type,
            now_iso=now_iso,
            job_id=job_id,
            title=row["title"],
            apply_link=row["url"],
            page_text=row["title"],
        )
        _enrich_role_from_detail_page(role)
        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company_label}: saved job_id={job_id}")
        if stop_fetch:
            print(f"{company_label}: duplicate detected while saving; continuing (unsorted fallback).")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

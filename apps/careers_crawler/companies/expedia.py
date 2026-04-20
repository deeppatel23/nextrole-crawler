"""
Workday API (preferred; avoids 403/WAF that may block `careers.expediagroup.com` endpoints).

List API (POST JSON):
https://expedia.wd108.myworkdayjobs.com/wday/cxs/expedia/search/jobs

Body:
{
  "appliedFacets": {},
  "limit": 20,
  "offset": 0,
  "searchText": ""
}

List response shape (sample):
{
  "total": 1234,
  "jobPostings": [
    {
      "title": "Senior Software Development Engineer",
      "externalPath": "/job/India---Gurgaon/Senior-Software-Development-Engineer_R-101761",
      "locationsText": "India - Gurgaon"
    }
  ]
}

Detail API (GET JSON):
Take `externalPath` from list and call:
https://expedia.wd108.myworkdayjobs.com/wday/cxs/expedia/search{externalPath}

Detail response contains `jobPostingInfo` with fields like:
- jobReqId (e.g. R-101761)
- title
- jobDescription
- location (e.g. "India - Haryāna - Gurgaon")
- externalUrl (apply link)

Mapping with role_details
    job_hash: str = generate_job_hash("Expediagroup", job_id)

    # identity
    job_id: str = jobPostingInfo.jobReqId (fallback: parse from externalPath)
    company: str = Expediagroup

    # core job info
    title: str = list.title (or detail.jobPostingInfo.title)
    role: Optional[str] = None
    category: Optional[str] = get_enrichment with extra_text = title + jobDescription

    # experience
    min_yoe: Optional[int] = get_enrichment with extra_text = title + jobDescription
    max_yoe: Optional[int] = get_enrichment with extra_text = title + jobDescription

    # location
    country: Optional[str] = parse from jobPostingInfo.location (first token)
    state: Optional[str] = parse from jobPostingInfo.location (middle tokens)
    city: Optional[str] = parse from jobPostingInfo.location (last token)
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = get_enrichment(...).skills (top 3)

    # links
    apply_link: Optional[str] = jobPostingInfo.externalUrl
    source_type: Optional[str] = API

    # metadata
    created_at: Optional[str] = None = datetime.now().isoformat().date()
    updated_at: Optional[str] = None

You will not get all the jobs in one API call:
- Increment `offset` by `limit` until offset >= total or no jobs returned.
- Since Workday facet ids are opaque, filter to the configured country using `locationsText`/`location`.

The response is not guaranteed to be sorted, so if we are saving it to mongo and a job exists,
(if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then skip that job and continue.

Add this company details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""

from __future__ import annotations

import html
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import get_by_path, normalize_city, normalize_region
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

COMPANY = "Expediagroup"
SOURCE_TYPE = "API"
WORKDAY_LIST_API_URL = "https://expedia.wd108.myworkdayjobs.com/wday/cxs/expedia/search/jobs"
WORKDAY_DETAIL_API_PREFIX = "https://expedia.wd108.myworkdayjobs.com/wday/cxs/expedia/search"
JOBS_PAGE_URL = "https://careers.expediagroup.com/jobs/"
CALC_RESULTS_URL = "https://careers.expediagroup.com/calc-results/"
SITE_BASE_URL = "https://careers.expediagroup.com"
DEFAULT_COUNTRY_FILTER = "India"
DEFAULT_MAX_PAGES = 50
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _resolve_max_saved(source_cfg: Dict[str, Any], site_key: str) -> int:
    raw = source_cfg.get("max_saved_jobs", 9999)
    if isinstance(raw, dict):
        value = raw.get(site_key, raw.get("default", 9999))
    else:
        value = raw
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else 0
    except (TypeError, ValueError):
        return 9999


def _build_headers(source_cfg: Dict[str, Any], *, referer: Optional[str], xhr: bool) -> Dict[str, str]:
    headers: Dict[str, str] = {
        "user-agent": str(source_cfg.get("user_agent") or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" if not xhr else "text/html, */*; q=0.1",
        "accept-language": str(source_cfg.get("accept_language") or "en-US,en;q=0.9").strip()
        or "en-US,en;q=0.9",
        # Keep this conservative (requests may not decode br unless brotli is installed).
        "accept-encoding": "gzip, deflate",
        "connection": "keep-alive",
        "upgrade-insecure-requests": "1" if not xhr else "0",
    }
    if referer:
        headers["referer"] = referer
    if xhr:
        headers["x-requested-with"] = "XMLHttpRequest"
        headers["sec-fetch-dest"] = "empty"
        headers["sec-fetch-mode"] = "cors"
        headers["sec-fetch-site"] = "same-origin"
    else:
        headers["sec-fetch-dest"] = "document"
        headers["sec-fetch-mode"] = "navigate"
        headers["sec-fetch-site"] = "same-origin"
        headers["sec-fetch-user"] = "?1"

    cookie = source_cfg.get("cookie")
    if isinstance(cookie, str) and cookie.strip():
        headers["cookie"] = cookie.strip()
    return headers


def _prime_session(session: requests.Session, source_cfg: Dict[str, Any]) -> None:
    # Some WAFs block direct hits to "calc-results" unless a browsing session exists.
    try:
        session.get(
            JOBS_PAGE_URL,
            headers=_build_headers(source_cfg, referer=None, xhr=False),
            timeout=30,
        )
    except Exception:
        return


def _fetch_list_html(session: requests.Session, source_cfg: Dict[str, Any], country: str, mypage: int) -> str:
    # Prefer the main jobs page (often less protected than the calc-results endpoint).
    # Fall back to calc-results if needed.
    params = {"mypage": str(mypage)}
    if country:
        params["filter[country]"] = country
    referer = f"{SITE_BASE_URL}/jobs/"

    resp = session.get(
        JOBS_PAGE_URL,
        params=params,
        headers=_build_headers(source_cfg, referer=referer, xhr=False),
        timeout=30,
    )
    if resp.status_code == 403:
        _prime_session(session, source_cfg)
        resp = session.get(
            JOBS_PAGE_URL,
            params=params,
            headers=_build_headers(source_cfg, referer=referer, xhr=False),
            timeout=30,
        )
    if resp.status_code == 403:
        # As a last resort, try the legacy calc-results endpoint.
        resp = session.get(
            CALC_RESULTS_URL,
            params=params,
            headers=_build_headers(source_cfg, referer=referer, xhr=True),
            timeout=30,
        )
        if resp.status_code == 403:
            _prime_session(session, source_cfg)
            resp = session.get(
                CALC_RESULTS_URL,
                params=params,
                headers=_build_headers(source_cfg, referer=referer, xhr=True),
                timeout=30,
            )
    if resp.status_code == 403:
        raise RuntimeError(
            "403 Forbidden from Expedia careers site (likely bot protection). "
            "Try setting `cookie:` (and optionally `user_agent:`) for Expedia in careers_sources.yaml "
            "from a real browser session."
        )
    resp.raise_for_status()
    return resp.text


_LISTING_RE = re.compile(
    r'<a[^>]+href=[\"\'](?P<href>/job/[^\"\']+/R-\d+/)[\"\'][^>]*class=[\"\'][^\"\']*view-job-button[^\"\']*[\"\'][^>]*>'
    r"(?P<body>[\s\S]*?)</a>",
    flags=re.I,
)


def _extract_listings(list_html: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for match in _LISTING_RE.finditer(list_html or ""):
        href = match.group("href")
        if not href:
            continue
        href = html.unescape(href).strip()
        if href in seen:
            continue
        seen.add(href)
        body = match.group("body") or ""
        title_match = re.search(
            r"<h3[^>]*Results__list__title[^>]*>(?P<title>[\s\S]*?)</h3>",
            body,
            flags=re.I,
        )
        loc_match = re.search(
            r"<h4[^>]*Results__list__location[^>]*>(?P<loc>[\s\S]*?)</h4>",
            body,
            flags=re.I,
        )
        title = _clean_text(title_match.group("title")) if title_match else ""
        location = _clean_text(loc_match.group("loc")) if loc_match else ""
        out.append({"href": href, "title": title, "location": location})
    return out


def _location_matches_country(location: str, country: str) -> bool:
    if not country:
        return True
    if not location:
        return False
    loc = location.strip().lower()
    c = country.strip().lower()
    if not c:
        return True
    # Common formats:
    # - "India - Bangalore"
    # - "India - Haryāna - Gurgaon"
    # - "Gurgaon, India"
    head = loc.split(" - ", 1)[0].strip()
    return (
        head == c
        or loc.startswith(f"{c} -")
        or loc.endswith(f", {c}")
        or loc.endswith(f" {c}")
        or loc == c
    )


def _build_workday_list_body(offset: int, limit: int) -> Dict[str, Any]:
    # Not applying facets (facet ids are opaque). Filter by locationsText later.
    return {"appliedFacets": {}, "limit": limit, "offset": offset, "searchText": ""}


def _extract_workday_list(payload: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
    total = payload.get("total")
    postings = payload.get("jobPostings")
    out_total = total if isinstance(total, int) and total >= 0 else 0
    if not isinstance(postings, list):
        return out_total, []
    return out_total, [p for p in postings if isinstance(p, dict)]


def _build_workday_detail_url(external_path: Optional[str]) -> Optional[str]:
    if not isinstance(external_path, str):
        return None
    path = external_path.strip()
    if not path:
        return None
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{WORKDAY_DETAIL_API_PREFIX}{path}"


def _extract_workday_job_info(payload: Dict[str, Any]) -> Dict[str, Any]:
    info = payload.get("jobPostingInfo")
    return info if isinstance(info, dict) else {}


def _extract_location_parts(location: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(location, str) or not location.strip():
        return None, None, None
    parts = [part.strip() for part in location.split(" - ") if part.strip()]
    if not parts:
        return None, None, None
    if len(parts) == 1:
        return parts[0], None, None
    if len(parts) == 2:
        return parts[0], None, parts[1]
    country = parts[0]
    state = " - ".join(parts[1:-1]) if len(parts) > 2 else None
    city = parts[-1]
    return country, state, city


def _extract_job_id_from_href(href: str) -> Optional[str]:
    if not isinstance(href, str) or not href:
        return None
    match = re.search(r"(R-\d+)", href)
    if match:
        return match.group(1)
    return None


def _fetch_detail_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _extract_jsonld_objects(detail_html: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for script in re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>([\s\S]*?)</script>",
        detail_html or "",
        flags=re.I,
    ):
        raw = (script or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if isinstance(data, dict):
            out.append(data)
        elif isinstance(data, list):
            out.extend([item for item in data if isinstance(item, dict)])
    return out


def _pick_jobposting_jsonld(objs: List[Dict[str, Any]]) -> Dict[str, Any]:
    for obj in objs:
        typ = obj.get("@type")
        if typ == "JobPosting":
            return obj
    return objs[0] if objs else {}


def _extract_skills_from_jsonld(jobposting: Dict[str, Any]) -> List[str]:
    raw = jobposting.get("skills")
    if isinstance(raw, list):
        out = [str(s).strip() for s in raw if s]
        return [s for s in out if s][:3]
    if isinstance(raw, str) and raw.strip():
        tokens = [t.strip() for t in re.split(r"[,;|\n]+", raw) if t.strip()]
        return tokens[:3]
    return []


def _extract_location_from_jsonld(jobposting: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    city = get_by_path(jobposting, "jobLocation.address.addressLocality")
    country = get_by_path(jobposting, "jobLocation.address.addressCountry")
    if isinstance(country, dict):
        country = country.get("name") or country.get("@id") or country.get("addressCountry")
    out_city = str(city).strip() if city else None
    out_country = str(country).strip() if country else None
    return out_city, out_country


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company_label = source_cfg.get("company") or COMPANY
    site_key = str(source_cfg.get("site_number") or "default")
    max_saved = _resolve_max_saved(source_cfg, site_key)

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

    country = str(source_cfg.get("country") or DEFAULT_COUNTRY_FILTER).strip() or DEFAULT_COUNTRY_FILTER
    max_pages = int(source_cfg.get("max_pages") or DEFAULT_MAX_PAGES)
    if max_pages <= 0:
        max_pages = DEFAULT_MAX_PAGES

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0
    seen_job_ids: Set[str] = set()

    # Primary path: Workday JSON APIs (more reliable than careers.expediagroup.com, which often returns 403 to scripts).
    offset = 0
    limit = int(source_cfg.get("limit") or 20)
    if limit <= 0:
        limit = 20

    while True:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping.")
            break

        print(f"{company_label}: fetching workday offset={offset} limit={limit}")
        try:
            payload = call_api(
                method="POST",
                url=WORKDAY_LIST_API_URL,
                headers={"User-Agent": DEFAULT_USER_AGENT, "Content-Type": "application/json"},
                body=_build_workday_list_body(offset=offset, limit=limit),
            )
        except Exception as exc:
            print(f"{company_label}: failed to fetch workday list offset={offset}: {exc}")
            break

        total, postings = _extract_workday_list(payload if isinstance(payload, dict) else {})
        if not postings:
            print(f"{company_label}: no more postings (offset={offset}), stopping.")
            break

        offset += len(postings)

        for post in postings:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping.")
                break

            title = str(post.get("title") or "").strip()
            external_path = post.get("externalPath")
            locations_text = str(post.get("locationsText") or "").strip()
            if country and locations_text and not _location_matches_country(locations_text, country):
                continue
            if not title:
                continue

            detail_url = _build_workday_detail_url(external_path)
            if not detail_url:
                continue

            try:
                detail_payload = call_api(
                    method="GET",
                    url=detail_url,
                    headers={"User-Agent": DEFAULT_USER_AGENT, "accept": "application/json"},
                    params=None,
                    body=None,
                )
            except Exception as exc:
                print(f"{company_label}: failed to fetch workday detail url={detail_url}: {exc}")
                continue

            info = _extract_workday_job_info(detail_payload if isinstance(detail_payload, dict) else {})
            job_id = info.get("jobReqId") or info.get("jobRequisitionId")
            if not isinstance(job_id, str) or not job_id.strip():
                job_id = _extract_job_id_from_href(str(external_path or ""))
            job_id = str(job_id).strip() if job_id else None
            if not job_id:
                continue

            if job_id in seen_job_ids:
                continue
            seen_job_ids.add(job_id)

            job_hash = generate_job_hash(COMPANY, job_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(f"{company_label}: duplicate job_hash job_id={job_id}, skipping.")
                continue

            description = info.get("jobDescription") if isinstance(info.get("jobDescription"), str) else ""
            location_str = info.get("location") if isinstance(info.get("location"), str) else locations_text
            loc_country, loc_state, loc_city = _extract_location_parts(location_str)
            if country and loc_country and not _location_matches_country(loc_country, country):
                continue
            apply_link = info.get("externalUrl") if isinstance(info.get("externalUrl"), str) else None

            extra_text = f"{title}\n\n{_clean_text(description)}".strip()
            enrichment = get_enrichment(title=title, apply_link=apply_link, extra_text=extra_text)

            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_id,
                company=COMPANY,
                source_type=SOURCE_TYPE,
                title=title,
                role=None,
                category=enrichment.get("category"),
                min_yoe=enrichment.get("min_yoe"),
                max_yoe=enrichment.get("max_yoe"),
                city=normalize_city(loc_city),
                state=normalize_region(loc_state),
                country=loc_country,
                workplace_type=None,
                skills=enrichment.get("skills") or [],
                apply_link=apply_link,
                created_at=created_at,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
                print(f"{company_label}: saved job_id={job_id}")
            elif stop_fetch:
                print(f"{company_label}: job already exists job_id={job_id}, skipping.")

        if total and offset >= total:
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

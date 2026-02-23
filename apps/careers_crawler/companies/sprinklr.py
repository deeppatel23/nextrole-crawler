"""
POST API for Sprinklr company careers page.
https://sprinklr.wd1.myworkdayjobs.com/wday/cxs/sprinklr/careers/jobs

Body:
{
    "appliedFacets": {
        "locationCountry": [
            "c4f78be1a8f14da0ab49ce1162348a5e"
        ]
    },
    "limit": 20,
    "offset": 0,
    "searchText": ""
}

Sample Response of main POST API:
{
    "total": 48,
    "jobPostings": [
        {
            "title": "Lead Software Engineer",
            "externalPath": "/job/India---Haryana---Gurgaon/Lead-Software-Engineer_112655-JOB",
            "locationsText": "India - Haryana - Gurgaon",
            "postedOn": "Posted Today",
            "bulletFields": [
                "112655-JOB"
            ]
        }
    ]
}

Job details GET API: 
https://sprinklr.wd1.myworkdayjobs.com/wday/cxs/sprinklr/careers/{externalPath}

Job details sample response:
{
    "jobPostingInfo": {
        "id": "b7c839335a9f1000f69c9fcefc660000",
        "title": "Lead Software Engineer",
        "jobDescription": "",
        "location": "India - Haryana - Gurgaon",
        "postedOn": "Posted Today",
        "startDate": "2026-02-17",
        "timeType": "Full time",
        "jobReqId": "112655-JOB",
        "jobPostingId": "Lead-Software-Engineer_112655-JOB",
        "jobPostingSiteId": "careers",
        "country": {
            "descriptor": "India",
            "id": "c4f78be1a8f14da0ab49ce1162348a5e"
        },
        "canApply": true,
        "posted": true,
        "includeResumeParsing": true,
        "jobRequisitionLocation": {
            "descriptor": "India - Haryana - Gurgaon",
            "country": {
                "descriptor": "India",
                "id": "c4f78be1a8f14da0ab49ce1162348a5e",
                "alpha2Code": "IN"
            }
        },
        "externalUrl": "https://sprinklr.wd1.myworkdayjobs.com/careers/job/India---Haryana---Gurgaon/Lead-Software-Engineer_112655-JOB",
        "questionnaireId": "f4c2353e95de1000aebf50f3f5610000"
    },
    "hiringOrganization": {
        "name": "Sprinklr India Private Limited",
        "url": ""
    },
    "similarJobs": [],
    "userAuthenticated": false
}

Mapping to RoleDetail with job details API response:
    job_hash: str = hash of id + "Sprinklr"
    
    # identity
    job_id: str = id
    company: str = "Sprinklr"

    # core job info
    title: str = title
    role: Optional[str] = None
    category: Optional[str] = None = get_enrichment with extra_text = fetch title + jobDescription 

    # experience
    min_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 
    max_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 

    # location
    city: Optional[str] = None = split location by " - " and take last part as city
    state: Optional[str] = None = split location by " - " and take middle part as state
    country: Optional[str] = None = split location by " - " and take first part as country
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = get_enrichment with extra_text = fetch title + jobDescription 

    # links
    apply_link: Optional[str] = None = externalUrl
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = date.now().isoformat()
    updated_at: Optional[str] = None

Loop the main API first. You will get at max limit (20) in one batch. Change the offset parameter to get the next batch of jobs. 
For each job, call the job details API to get the job description and other details to build the role_detail object.
We main total parameter in main API response to know how many jobs are there in total and when to stop calling the main API after total/limit number of times.

Main API response is not sorted, so for each job details, check if it already exists in mongo (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) if exists then skip saving and move to next job. This will ensure that we are not saving duplicate jobs.

Add this company details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

LIST_API_URL = "https://sprinklr.wd1.myworkdayjobs.com/wday/cxs/sprinklr/careers/jobs"
DETAIL_API_PREFIX = "https://sprinklr.wd1.myworkdayjobs.com/wday/cxs/sprinklr/careers"
DEFAULT_LIMIT = 20
DEFAULT_LOCATION_COUNTRY_ID = "c4f78be1a8f14da0ab49ce1162348a5e"


def _build_list_body(
    offset: int,
    limit: int,
    location_country_id: str,
    search_text: str,
) -> Dict[str, Any]:
    return {
        "appliedFacets": {"locationCountry": [location_country_id]},
        "limit": limit,
        "offset": offset,
        "searchText": search_text,
    }


def _extract_listing(payload: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
    total = payload.get("total")
    postings = payload.get("jobPostings")

    out_total = total if isinstance(total, int) and total >= 0 else 0
    if not isinstance(postings, list):
        return out_total, []

    out_jobs = [job for job in postings if isinstance(job, dict)]
    return out_total, out_jobs


def _build_detail_url(external_path: Optional[str]) -> Optional[str]:
    if not isinstance(external_path, str):
        return None
    path = external_path.strip()
    if not path:
        return None
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{DETAIL_API_PREFIX}{path}"


def _extract_job_posting_info(payload: Dict[str, Any]) -> Dict[str, Any]:
    info = payload.get("jobPostingInfo")
    if isinstance(info, dict):
        return info
    return {}


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


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Sprinklr"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    site_key = str(source_cfg.get("site_number") or "default")
    max_saved = _resolve_max_saved(source_cfg, site_key)
    last_saved = source_cfg.get("last_saved")

    limit = source_cfg.get("page_size", source_cfg.get("limit", DEFAULT_LIMIT))
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT
    if limit <= 0:
        limit = DEFAULT_LIMIT

    location_country_id = str(
        source_cfg.get("location_country_id") or DEFAULT_LOCATION_COUNTRY_ID
    )
    search_text = str(source_cfg.get("search_text") or "")

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
    created_at = today.isoformat()
    saved_for_site = 0
    total_saved = 0
    page_index = 0
    offset = 0
    total_jobs = None
    total_pages = None

    while True:
        if saved_for_site >= max_saved:
            print(
                f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping."
            )
            break

        body = _build_list_body(
            offset=offset,
            limit=limit,
            location_country_id=location_country_id,
            search_text=search_text,
        )
        print(
            f"{company_label}: start iteration {page_index + 1} calling {LIST_API_URL} with offset={offset} limit={limit}"
        )
        try:
            payload = call_api(
                method="POST",
                url=LIST_API_URL,
                headers={"Content-Type": "application/json"},
                body=body,
            )
        except Exception as exc:
            print(f"{company_label}: listing API failed at offset={offset}: {exc}")
            break

        current_total, jobs = _extract_listing(payload)
        if total_jobs is None:
            total_jobs = current_total
            total_pages = math.ceil(total_jobs / limit) if total_jobs > 0 else 0
            print(
                f"{company_label}: total jobs={total_jobs}, limit={limit}, total_pages={total_pages}"
            )

        if not jobs:
            print(f"{company_label}: no jobs returned at offset={offset}, stopping.")
            break

        print(f"{company_label}: iteration {page_index + 1} fetched {len(jobs)} jobs.")

        for job in jobs:
            if saved_for_site >= max_saved:
                print(
                    f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping."
                )
                return total_saved

            detail_url = _build_detail_url(job.get("externalPath"))
            if not detail_url:
                print(f"{company_label}: missing externalPath in listing item, skipping.")
                continue

            detail_info: Dict[str, Any] = {}
            try:
                detail_payload = call_api(method="GET", url=detail_url)
                detail_info = _extract_job_posting_info(detail_payload)
            except Exception as exc:
                print(f"{company_label}: detail API failed for {detail_url}: {exc}")
                continue

            job_id = str(detail_info.get("id") or "").strip()
            title = str(detail_info.get("title") or job.get("title") or "").strip()
            if not job_id or not title:
                print(f"{company_label}: missing id/title from detail payload, skipping.")
                continue

            job_hash = generate_job_hash(company, job_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(f"{company_label}: existing job_hash for job_id={job_id}, skipping.")
                continue

            job_description = str(detail_info.get("jobDescription") or "").strip()
            enrichment_text = f"{title} {job_description}".strip()
            apply_link = detail_info.get("externalUrl")
            apply_link = str(apply_link).strip() if isinstance(apply_link, str) else None
            if not apply_link:
                print(f"{company_label}: missing externalUrl for job_id={job_id}, skipping.")
                continue

            enrichment = get_enrichment(
                title=title,
                apply_link=apply_link,
                extra_text=enrichment_text or None,
            )

            location = (
                detail_info.get("location")
                or job.get("locationsText")
                or (
                    detail_info.get("jobRequisitionLocation", {}).get("descriptor")
                    if isinstance(detail_info.get("jobRequisitionLocation"), dict)
                    else None
                )
            )
            country, state, city = _extract_location_parts(location)

            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_id,
                company=company,
                title=title,
                role=None,
                category=enrichment["category"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                city=normalize_city(city),
                state=state,
                country=country,
                workplace_type=None,
                skills=enrichment["skills"],
                apply_link=apply_link,
                source_type=source_type,
                created_at=created_at,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                saved_for_site += saved_count
                total_saved += saved_count
                checker.record(job_hash)
                print(
                    f"{company_label}: saved job_id={job_id} (saved={total_saved}, site_saved={saved_for_site})."
                )
            elif stop_fetch:
                print(
                    f"{company_label}: duplicate detected while saving job_id={job_id}, skipping (unsorted listing)."
                )

        page_index += 1
        offset += limit

        if total_jobs is not None and offset >= total_jobs:
            print(
                f"{company_label}: reached end of listing at offset={offset} with total={total_jobs}, stopping."
            )
            break
        if total_pages is not None and page_index >= total_pages:
            print(f"{company_label}: processed total_pages={total_pages}, stopping.")
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

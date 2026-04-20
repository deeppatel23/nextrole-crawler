"""
POST API:
curl --location 'https://www.uber.com/api/loadSearchJobsResults?localeCode=en' \
--header 'x-csrf-token: x' \
--header 'Content-Type: application/json' \
--header 'Cookie: __cf_bm=YccdL8bV038kkMqyVf6GUGdHOnhOV8Z6wiu4jQsmmZQ-1776696043-1.0.1.1-_cyhCaP6rsVNPnl8BqW9rcj3_qwaivbuJGoICEn_pm0qO3tvqphalpJhmIwwG8q.D3rysSfznUEKGgW0hiUKmCQziF8kzOEg4vLOmIFAlc4; _ua={"session_id":"9a494c7a-f9ee-4510-a920-ea872c6434a9","session_time_ms":1776696120644}; jwt-session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InNsYXRlLWV4cGlyZXMtYXQiOjE3NzY2OTc5MjA2NTF9LCJpYXQiOjE3NzY2OTYxMjAsImV4cCI6MTc3Njc4MjUyMH0._XxhJh1upoQsCIFlTmoTnEfgf8FYIr8plnfFBrEVCuc; user_city_ids=761' \
--data '{
    "params": {
        "department": [],
        "lineOfBusinessName": [],
        "location": [
            {
                "country": "IND"
            }
        ],
        "programAndPlatform": [],
        "team": []
    }
}'

Sample Response:
{
    "data": {
        "results": [
            {
                "id": 155708,
                "title": "Staff Technical Program Manager",
                "description": "",
                "department": "Engineering",
                "type": "",
                "programAndPlatform": "",
                "location": {
                    "country": "IND",
                    "region": "Karnātaka",
                    "city": "Bangalore",
                    "countryName": "India"
                },
                "featured": false,
                "level": "5B",
                "creationDate": "2026-02-16T20:44:00.000Z",
                "otherLevels": null,
                "team": "Technical Program Manager",
                "portalID": "EXTERNAL",
                "isPipeline": false,
                "statusID": "D31001",
                "statusName": "Approved",
                "updatedDate": "2026-04-20T14:21:00.000Z",
                "uniqueSkills": "",
                "timeType": "Full-Time",
                "allLocations": [
                    {
                        "country": "IND",
                        "region": "Karnātaka",
                        "city": "Bangalore",
                        "countryName": "India"
                    }
                ]
            }
        ]
    }
}

Mapping with role_details
    job_hash: str = hash of data.results[index].id + Uber
    
    # identity
    job_id: str = id
    company: str = Uber

    # core job info
    title: str = title
    role: Optional[str] = None
    category: Optional[str] = get_enrichment with extra_text = fetch title + jobDescription and get the caegory

    # experience
    min_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 
    max_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 

    # location
    city: Optional[str] = None = location.city
    state: Optional[str] = None
    country: Optional[str] = None = location.country
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = get_enrichment with extra_text = fetch title + jobDescription 

    # links
    apply_link: Optional[str] = None = "https://www.uber.com/global/en/careers/list/" + id
    source_type: Optional[str] = API

    # metadata
    created_at: Optional[str] = None = datetime.now().isoformat().date()
    updated_at: Optional[str] = None

You will get all the jobs response in one API call. Loop until we have iterated through all the jobs or reached max_saved_jobs, total parameter is there in response and size parameter is the query parameter we are sending in the API request.

The response of the API is not sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then skip that particular jobs and go to next job

Add this comapny details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.


"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import normalize_city, normalize_region
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://www.uber.com/api/loadSearchJobsResults"
DEFAULT_LOCALE_CODE = "en"
DEFAULT_LIMIT = 2000


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


def _build_headers(source_cfg: Dict[str, Any]) -> Dict[str, str]:
    headers = {
        "x-csrf-token": "x",
        "Content-Type": "application/json",
        "accept": "application/json, text/plain, */*",
        "origin": "https://www.uber.com",
        "referer": "https://www.uber.com/global/en/careers/list/",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    cookie = source_cfg.get("cookie")
    if isinstance(cookie, str) and cookie.strip():
        headers["Cookie"] = cookie.strip()

    return headers


def _build_body(source_cfg: Dict[str, Any]) -> Dict[str, Any]:
    country = str(source_cfg.get("country") or "IND").strip() or "IND"

    # Uber appears to accept a single request with filters and returns all results.
    # We include an optional "limit" to play nicely if the API supports it.
    return {
        "params": {
            "department": source_cfg.get("department") or [],
            "lineOfBusinessName": source_cfg.get("lineOfBusinessName") or [],
            "location": [{"country": country}],
            "programAndPlatform": source_cfg.get("programAndPlatform") or [],
            "team": source_cfg.get("team") or [],
        },
        "limit": int(source_cfg.get("limit") or DEFAULT_LIMIT),
    }


def _extract_results(payload: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return 0, []
    results = data.get("results")
    total = data.get("total")

    out_jobs = [job for job in results if isinstance(job, dict)] if isinstance(results, list) else []
    if isinstance(total, int) and total >= 0:
        return total, out_jobs
    return len(out_jobs), out_jobs


def _extract_location(job: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    location = job.get("location")
    if not isinstance(location, dict):
        return None, None, None, None

    city = location.get("city")
    state = location.get("region")
    country_name = location.get("countryName")
    country_code = location.get("country")

    out_city = str(city).strip() if city else None
    out_state = str(state).strip() if state else None
    out_country = str(country_name).strip() if country_name else (str(country_code).strip() if country_code else None)
    return out_city, out_state, out_country, str(country_code).strip() if country_code else None


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Uber"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
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

    locale_code = str(source_cfg.get("locale_code") or DEFAULT_LOCALE_CODE).strip() or DEFAULT_LOCALE_CODE
    params = {"localeCode": locale_code}

    print(f"{company_label}: fetching jobs from Uber careers API locale={locale_code}")
    try:
        payload = call_api(
            method="POST",
            url=API_URL,
            headers=_build_headers(source_cfg),
            params=params,
            body=_build_body(source_cfg),
        )
    except Exception as exc:
        print(f"{company_label}: failed to fetch jobs from API: {exc}")
        return 0

    total, jobs = _extract_results(payload)
    print(f"{company_label}: total={total} received={len(jobs)}")
    if not jobs:
        print(f"{company_label}: no jobs found.")
        return 0

    created_at = today.isoformat()
    checker = MongoJobHashChecker()

    total_saved = 0
    apply_base = str(source_cfg.get("apply_base_url") or "https://www.uber.com/global/en/careers/list/").strip()
    if apply_base and not apply_base.endswith("/"):
        apply_base += "/"

    for index, job in enumerate(jobs):
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping.")
            break

        job_id = job.get("id")
        title = job.get("title")
        description = job.get("description") or ""
        if job_id is None or title is None:
            continue

        job_id_str = str(job_id).strip()
        if not job_id_str:
            continue

        job_hash = generate_job_hash(company, job_id_str)
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(f"{company_label}: duplicate job_hash job_id={job_id_str}, skipping.")
            continue

        apply_link = f"{apply_base}{job_id_str}" if apply_base else None
        extra_text = f"{title}\n\n{description}".strip()
        enrichment = get_enrichment(title=str(title), apply_link=apply_link, extra_text=extra_text)

        city, state, country, _country_code = _extract_location(job)

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id_str,
            company=company,
            source_type=source_type,
            title=str(title),
            role=None,
            category=enrichment.get("category"),
            min_yoe=enrichment.get("min_yoe"),
            max_yoe=enrichment.get("max_yoe"),
            city=normalize_city(city),
            state=normalize_region(state),
            country=country,
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
            print(f"{company_label}: saved job_id={job_id_str} index={index}")
        elif stop_fetch:
            print(f"{company_label}: job already exists job_id={job_id_str} index={index}, skipping.")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

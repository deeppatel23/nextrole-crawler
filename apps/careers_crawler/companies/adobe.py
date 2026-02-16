"""
POST API Curl command to get job details for adobe
curl --location 'https://careers.adobe.com/widgets' \
--header 'Content-Type: application/json' \
--header 'Cookie: PHPPPE_ACT=f867f195-67e3-4278-a8ac-c60817e16001; PLAY_SESSION=eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7IkpTRVNTSU9OSUQiOiJmODY3ZjE5NS02N2UzLTQyNzgtYThhYy1jNjA4MTdlMTYwMDEifSwibmJmIjoxNzcxMjYyNjc0LCJpYXQiOjE3NzEyNjI2NzR9.Cf4M_Z8lUu18GhkUQqRzQzEgMQ2obl0evqJwg6OOhcQ' \
--data '{
    "lang": "en_us",
    "deviceType": "desktop",
    "country": "us",
    "pageName": "search-results",
    "ddoKey": "refineSearch",
    "sortBy": "Most recent",
    "subsearch": "",
    "from": 0,
    "irs": false,
    "jobs": true,
    "counts": true,
    "all_fields": [
        "remote",
        "country",
        "state",
        "city",
        "experienceLevel",
        "category",
        "profession",
        "employmentType",
        "jobLevel"
    ],
    "size": 10,
    "clearAll": false,
    "jdsource": "facets",
    "isSliderEnable": false,
    "pageId": "page15-ds",
    "siteType": "external",
    "keywords": "",
    "global": true,
    "selected_fields": {
        "country": [
            "India"
        ]
    },
    "sort": {
        "order": "desc",
        "field": "postedDate"
    },
    "locationData": {}
}'

Sample response structure:
{
    "refineSearch": {
        "status": 200,
        "hits": 10,
        "totalHits": 185,
        "data": {
            "jobs": [
                {
                    "jobPostingEndDate": "",
                    "ml_skills": [
                        "saas delivery management",
                        "cloud platforms",
                        "enterprise software delivery",
                        "digital transformation",
                        "delivery strategy development",
                        "operational reporting",
                        "forecasting",
                        "cost management",
                        "deal structuring",
                        "contract negotiation",
                        "customer management",
                        "talent development",
                        "stakeholder management",
                        "executive presence",
                        "conflict resolution",
                        "crm",
                        "web analytics",
                        "campaign management",
                        "digital marketing technologies",
                        "matrixed environment leadership"
                    ],
                    "type": "Full time",
                    "descriptionTeaser": "Join our team as Senior Director, North America Delivery and lead regional GDC delivery excellence across the Americas. Drive strategy, execution, and growth in a global environment, leveraging your expertise in SaaS, cloud platforms, and enterprise software delivery. Shape the future of digital experiences with Adobe and empower high-performing teams to deliver outstanding results.",
                    "state": "Karnātaka",
                    "siteType": "external",
                    "multi_category": [
                        "Sales"
                    ],
                    "reqId": "R165385",
                    "city": "Bangalore",
                    "latitude": "12.97194",
                    "multi_location": [
                        "Bangalore, Karnātaka, India"
                    ],
                    "experienceLevel": "Experienced",
                    "address": "Bangalore, Bangalore- 560 029, Karnātaka, India",
                    "applyUrl": "https://adobe.wd5.myworkdayjobs.com/external_experienced/job/Bangalore/Senior-Director--North-America-Delivery_R165385-1/apply",
                    "ml_job_parser": {
                        "descriptionTeaser": "Join our team as Senior Director, North America Delivery and lead regional GDC delivery excellence across the Americas. Drive strategy, execution, and growth in a global environment, leveraging your expertise in SaaS, cloud platforms, and enterprise software delivery. Shape the future of digital experiences with Adobe and empower high-performing teams to deliver outstanding results.",
                        "descriptionTeaser_first200": "Our Company. Changing the world through digital experiences is what Adobe’s all about. We give everyone—from emerging artists to global brands—everything they need to design and deliver exceptional...",
                        "descriptionTeaser_keyword": "Our Company. Changing the world through digital experiences is what Adobe’s all about. We give everyone—from emerging artists to global brands—everything they need to design and deliver exceptional di...",
                        "descriptionTeaser_ats": "Our CompanyChanging the world through digital experiences is what Adobe’s all about. We give everyonefrom emerging artists to global brandseverything they need to design and deliver exceptional digi"
                    },
                    "externalApply": false,
                    "cityState": "Bangalore, Karnātaka",
                    "country": "India",
                    "visibilityType": "External",
                    "longitude": "77.59369",
                    "jobId": "R165385",
                    "locale": "en_US",
                    "title": "Senior Director, North America Delivery",
                    "jobSeqNo": "ADOBUSR165385EXTERNALENUS",
                    "postedDate": "2026-02-16T00:00:00.000+0000",
                    "dateCreated": "2026-02-11T08:30:31.017+0000",
                    "cityStateCountry": "Bangalore, Karnātaka, India",
                    "jobVisibility": [
                        "external"
                    ],
                    "location": "Bangalore, Karnātaka, India",
                    "category": "Sales",
                    "isMultiLocation": true,
                    "multi_location_array": [
                        {
                            "latlong": {
                                "lon": 77.59369,
                                "lat": 12.97194
                            },
                            "location": "Bangalore, Karnātaka, India"
                        }
                    ],
                    "isMultiCategory": true,
                    "multi_category_array": [
                        {
                            "category": "Sales"
                        }
                    ],
                    "badge": ""
                }
            ]
        }
    }
}

Detailed Job page API call example:
GET Request URL: https://adobe.wd5.myworkdayjobs.com/wday/cxs/adobe/external_experienced/job/Bangalore/Senior-Director--North-America-Delivery_R165385-1
fetch external_experienced/job/Bangalore/Senior-Director--North-America-Delivery_R165385-1 this part from applyUrl and then append it to https://adobe.wd5.myworkdayjobs.com/wday/cxs/adobe/ to get the detailed job page URL. This page contains more details about the job which can be used for enrichment.

Detailed Job page response structure:
{
  "jobPostingInfo": {
    "id": "792ac55f6a87100142391bc5baf40000",
    "title": "Senior Director, North America Delivery",
    "jobDescription":""
    }
}


Mapping with role_details
    job_hash: str = generate_job_hash(company, jobId)
    
    # identity
    job_id: str = jobId
    company: str = "Adobe"

    # core job info
    title: str = title
    role: Optional[str] = None
    category: Optional[str] = None = Make and call job detail API and try to extract category from the response. If not found, use the category field from the listing API response. If that is also not found, use the match_category function with title, skills and page text to get the category. 

    # experience
    min_yoe: Optional[int] = None = Make and cal job detail api, and try to extract min_yoe from the api response jobDescription. If not found, leave it as None. Make LLM call with the page text and ask it to identify the minimum years of experience required for the job. If LLM returns a number, use it. Otherwise, leave it as None.
    max_yoe: Optional[int] = None = Make and cal job detail api, and try to extract max_yoe from the api response jobDescription. If not found, leave it as None. Make LLM call with the page text and ask it to identify the maximum years of experience required for the job. If LLM returns a number, use it. Otherwise, leave it as None.

    # location
    city: Optional[str] = None = city
    state: Optional[str] = None = state
    country: Optional[str] = None = India
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = top 3 ml_skills

    # links
    apply_link: Optional[str] = None = applyUrl remove "/apply" from the end
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = today().date().isoformat()
    updated_at: Optional[str] = None

Check for one job at a time from a response. Loop in totalHits/hits and page number until we have iterated through all the jobs or reached max_saved_jobs, total parameter is there in response and size parameter is the query parameter we are sending in the API request. 
The from parameter in the API request will be page_number * hits count.

The response if the API for a particular siteNumber is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well for that siteNumber since they will be duplicates.

Add this company details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://careers.adobe.com/widgets"
DETAIL_API_PREFIX = "https://adobe.wd5.myworkdayjobs.com/wday/cxs/adobe/"
DEFAULT_SIZE = 10

BASE_REQUEST_BODY: Dict[str, Any] = {
    "lang": "en_us",
    "deviceType": "desktop",
    "country": "us",
    "pageName": "search-results",
    "ddoKey": "refineSearch",
    "sortBy": "Most recent",
    "subsearch": "",
    "from": 0,
    "irs": False,
    "jobs": True,
    "counts": True,
    "all_fields": [
        "remote",
        "country",
        "state",
        "city",
        "experienceLevel",
        "category",
        "profession",
        "employmentType",
        "jobLevel",
    ],
    "size": DEFAULT_SIZE,
    "clearAll": False,
    "jdsource": "facets",
    "isSliderEnable": False,
    "pageId": "page15-ds",
    "siteType": "external",
    "keywords": "",
    "global": True,
    "selected_fields": {"country": ["India"]},
    "sort": {"order": "desc", "field": "postedDate"},
    "locationData": {},
}


def _build_request_body(offset: int, size: int) -> Dict[str, Any]:
    body = dict(BASE_REQUEST_BODY)
    body["from"] = offset
    body["size"] = size
    return body


def _extract_jobs(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    refine = payload.get("refineSearch")
    if not isinstance(refine, dict):
        return []
    data = refine.get("data")
    if not isinstance(data, dict):
        return []
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        return []
    return [job for job in jobs if isinstance(job, dict)]


def _extract_counts(payload: Dict[str, Any], size: int) -> tuple[int, int]:
    refine = payload.get("refineSearch")
    if not isinstance(refine, dict):
        return 0, size

    total_hits = refine.get("totalHits")
    page_hits = refine.get("hits")
    out_total = total_hits if isinstance(total_hits, int) and total_hits >= 0 else 0
    out_hits = page_hits if isinstance(page_hits, int) and page_hits > 0 else size
    return out_total, out_hits


def _extract_top_skills(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []

    out: List[str] = []
    for item in raw:
        if not item:
            continue
        skill = str(item).strip()
        if not skill:
            continue
        out.append(skill)
        if len(out) >= 3:
            break
    return out


def _normalize_apply_link(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    link = str(raw).strip()
    if not link:
        return None
    if link.endswith("/apply"):
        return link[:-6]
    return link


def _build_detail_api_url(apply_url: Optional[str]) -> Optional[str]:
    if not apply_url:
        return None
    parsed = urlparse(apply_url)
    path = parsed.path.lstrip("/")
    if not path:
        return None
    return f"{DETAIL_API_PREFIX}{path}"


def _fetch_detail_payload(detail_api_url: Optional[str]) -> Dict[str, Any]:
    if not detail_api_url:
        return {}
    try:
        resp = call_api(method="GET", url=detail_api_url)
        if isinstance(resp, dict):
            return resp
    except Exception:
        return {}
    return {}


def _extract_detail_text(detail_payload: Dict[str, Any]) -> str:
    job_info = detail_payload.get("jobPostingInfo")
    if not isinstance(job_info, dict):
        return ""
    desc = job_info.get("jobDescription")
    return str(desc or "").strip()


def _fetch_page(offset: int, size: int) -> Dict[str, Any]:
    return call_api(
        method="POST",
        url=API_URL,
        headers={"Content-Type": "application/json"},
        body=_build_request_body(offset, size),
    )


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Adobe"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")
    size = int(source_cfg.get("page_size", DEFAULT_SIZE))
    if size <= 0:
        size = DEFAULT_SIZE

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

    print(f"{company_label}: start iteration 1 calling {API_URL}")
    first_payload = _fetch_page(0, size)
    first_jobs = _extract_jobs(first_payload)
    if not first_jobs:
        print(f"{company_label}: no jobs found.")
        return 0

    total_hits, page_hits = _extract_counts(first_payload, size)
    total_pages = max(1, math.ceil(total_hits / page_hits)) if total_hits else 1
    print(
        f"{company_label}: total_hits={total_hits} page_hits={page_hits} pages={total_pages}"
    )

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0

    def _save_jobs(jobs: List[Dict[str, Any]], iteration: int) -> bool:
        nonlocal total_saved
        for job in jobs:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                return True

            job_id = job.get("jobId")
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
                return True

            skills = _extract_top_skills(job.get("ml_skills"))
            raw_apply_url = job.get("applyUrl")
            apply_link = _normalize_apply_link(raw_apply_url)
            detail_payload = _fetch_detail_payload(_build_detail_api_url(apply_link))
            detail_text = _extract_detail_text(detail_payload)

            text_chunks = [
                detail_text,
                str(job.get("descriptionTeaser") or ""),
                str(job.get("category") or ""),
                ", ".join(skills),
            ]
            enrichment_text = " ".join(chunk for chunk in text_chunks if chunk).strip()
            enrichment = get_enrichment(
                str(title),
                apply_link,
                enrichment_text,
            )

            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_id_str,
                company=company,
                source_type=source_type,
                title=str(title),
                role=None,
                category=enrichment["category"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                city=job.get("city"),
                state=job.get("state"),
                country=job.get("country") or "India",
                workplace_type=None,
                skills=skills,
                apply_link=apply_link,
                created_at=created_at,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
            if stop_fetch:
                print(
                    f"{company_label}: duplicate detected while saving job_id={job_id_str}, stopping further fetch."
                )
                return True

        print(f"{company_label}: iteration {iteration} processed {len(jobs)} jobs")
        return False

    if _save_jobs(first_jobs, 1):
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    for page_index in range(1, total_pages):
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break
        offset = page_index * page_hits
        print(f"{company_label}: start iteration {page_index + 1} calling {API_URL}")
        payload = _fetch_page(offset, size)
        jobs = _extract_jobs(payload)
        if not jobs:
            print(f"{company_label}: iteration {page_index + 1} returned no jobs.")
            break
        if _save_jobs(jobs, page_index + 1):
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

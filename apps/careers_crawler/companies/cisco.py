"""
POST Request:
https://careers.cisco.com/widgets

JSON Request Body:
{
    "sortBy": "Most recent",
    "subsearch": "",
    "from": 0,
    "jobs": true,
    "counts": true,
    "all_fields": [
        "category",
        "raasJobRequisitionType",
        "country",
        "state",
        "city",
        "type",
        "RemoteType"
    ],
    "pageName": "Sales Careers in India - Cisco Careers",
    "pageType": "landingPage",
    "size": 999,
    "rk": "",
    "clearAll": false,
    "jdsource": "facets",
    "isSliderEnable": false,
    "pageId": "page529-prod",
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
    "rkstatus": true,
    "s": "1",
    "lang": "en_global",
    "deviceType": "desktop",
    "country": "global",
    "refNum": "CISCISGLOBAL",
    "ddoKey": "eagerLoadRefineSearch"
}

API Response:
{
    "eagerLoadRefineSearch": {
        "status": 200,
        "hits": 10,
        "totalHits": 195,
        "data": {
            "jobs": [
                {
                    "ml_skills": [
                        "c++",
                        "embedded development",
                        "networking technologies",
                        "software development",
                        "api development",
                        "firmware development",
                        "python",
                        "communication skills",
                        "strategic planning",
                        "problem solving",
                        "interdisciplinary collaboration",
                        "execution focus"
                    ],
                    "type": "Full time",
                    "descriptionTeaser": "Join our team as a C++ Software Engineer and develop core software technologies for next-generation infrastructure solutions. If you have a strong background in C++, a passion for problem-solving, and excellent communication skills, we want to hear from you!",
                    "state": "",
                    "siteType": "external",
                    "RemoteType": "Hybrid",
                    "multi_category": [
                        "Product and Engineering"
                    ],
                    "reqId": "2007643",
                    "city": "Bangalore",
                    "latitude": "12.97194",
                    "multi_location": [
                        "Bangalore, India"
                    ],
                    "address": "., Bangalore- 560103, India",
                    "applyUrl": "https://cisco.wd5.myworkdayjobs.com/Cisco_Careers/job/Bangalore-India/Software-Engineer_2007643/apply",
                    "ml_job_parser": {
                        "descriptionTeaser": "Join our team as a C++ Software Engineer and develop core software technologies for next-generation infrastructure solutions. If you have a strong background in C++, a passion for problem-solving, and excellent communication skills, we want to hear from you!",
                        "descriptionTeaser_ats": "Meet the Team We are thrilled to announce an exciting opportunity joining Silicon One as a C++ SW Engineer. Join our rapidly growing SDK Silicon One team at Cisco.The Cisco Silicon-One team develops b",
                        "descriptionTeaser_keyword": "Meet the Team. We are thrilled to announce an exciting opportunity joining Silicon One as a C++ SW Engineer. Join our rapidly growing SDK Silicon One team at Cisco. The Cisco Silicon-One team develops...",
                        "descriptionTeaser_first200": "Meet the Team. We are thrilled to announce an exciting opportunity joining Silicon One as a C++ SW Engineer. Join our rapidly growing SDK Silicon One team at Cisco. The Cisco Silicon-One team devel..."
                    },
                    "externalApply": false,
                    "cityState": "Bangalore",
                    "country": "India",
                    "visibilityType": "External",
                    "longitude": "77.59369",
                    "jobId": "2007643",
                    "locale": "en_GLOBAL",
                    "title": "Software Engineer",
                    "jobSeqNo": "CISCISGLOBAL2007643EXTERNALENGLOBAL",
                    "postedDate": "2026-02-12T00:00:00.000+0000",
                    "dateCreated": "2026-01-29T00:51:16.558+0000",
                    "cityStateCountry": "Bangalore, India",
                    "department": "SDK - Peripherals (Daniel Kaminsky)",
                    "jobVisibility": [
                        "external",
                        "internal"
                    ],
                    "location": "Bangalore, India",
                    "category": "Product and Engineering",
                    "isMultiLocation": true,
                    "multi_location_array": [
                        {
                            "latlong": {
                                "lon": 77.59369,
                                "lat": 12.97194
                            },
                            "location": "Bangalore, India"
                        }
                    ],
                    "isMultiCategory": true,
                    "multi_category_array": [
                        {
                            "category": "Product and Engineering"
                        }
                    ],
                    "badge": ""
                }
            ]
        }
    }
}

Mapping with our model:
job_hash: str = hash of jobId + Cisco
    
    # identity
    job_id: str = data.jobId
    company: str = "Cisco"

    # core job info
    title: str = data.title
    role: Optional[str] = None
    category: Optional[str] = data.category

    # experience
    min_yoe: Optional[int] = None = Open https://careers.cisco.com/global/en/job/{jobId} and check if experience is mentioned in the description or title, if yes then extract it. e.g. "2-4 years", "5+ years" etc. We can use regex to extract the numbers and then take the min and max. LLM call is required
    max_yoe: Optional[int] = None = Open https://careers.cisco.com/global/en/job/{jobId} and check if experience is mentioned in the description or title, if yes then extract it. e.g. "2-4 years", "5+ years" etc. We can use regex to extract the numbers and then take the min and max. LLM call is required

    # location
    city: Optional[str] = data.cityState
    state: Optional[str] = None
    country: Optional[str] = None = India
    workplace_type: Optional[str] = None  # onsite / remote / hybrid = data.RemoteType

    # content
    skills: List[str] = field(default_factory=list) = take top 3 skills [] max, from ml_skills list

    # links
    apply_link: Optional[str] = None = data.applyUrl
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = data.now date only
    updated_at: Optional[str] = None

Check for one job at a time from a response[].

The response if the API is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well since they will be duplicates.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop).

Try to add print statement wherever necessary.

Consider this for building the role_detail object.

"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://careers.cisco.com/widgets"

REQUEST_BODY: Dict[str, Any] = {
    "sortBy": "Most recent",
    "subsearch": "",
    "from": 0,
    "jobs": True,
    "counts": True,
    "all_fields": [
        "category",
        "raasJobRequisitionType",
        "country",
        "state",
        "city",
        "type",
        "RemoteType",
    ],
    "pageName": "Sales Careers in India - Cisco Careers",
    "pageType": "landingPage",
    "size": 999,
    "rk": "",
    "clearAll": False,
    "jdsource": "facets",
    "isSliderEnable": False,
    "pageId": "page529-prod",
    "siteType": "external",
    "keywords": "",
    "global": True,
    "selected_fields": {"country": ["India"]},
    "sort": {"order": "desc", "field": "postedDate"},
    "rkstatus": True,
    "s": "1",
    "lang": "en_global",
    "deviceType": "desktop",
    "country": "global",
    "refNum": "CISCISGLOBAL",
    "ddoKey": "eagerLoadRefineSearch",
}


def _fetch_jobs() -> List[Dict[str, Any]]:
    response = call_api(method="POST", url=API_URL, body=REQUEST_BODY)
    if isinstance(response, dict):
        eager_load = response.get("eagerLoadRefineSearch", {})
        data = eager_load.get("data", {})
        if isinstance(data, dict):
            jobs = data.get("jobs", [])
            return jobs if isinstance(jobs, list) else []
        if isinstance(data, list):
            return data
    return []


def _extract_skills(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    skills: List[str] = []
    for item in raw:
        if not item:
            continue
        skills.append(str(item))
        if len(skills) >= 3:
            break
    return skills


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Cisco"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
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

    print(f"{company_label}: fetching jobs from {API_URL}")
    jobs = _fetch_jobs()
    if not jobs:
        print(f"{company_label}: no jobs found.")
        return 0

    checker = MongoJobHashChecker()
    total_saved = 0
    today_str = today.isoformat()

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        if not isinstance(job, dict):
            continue

        job_id = job.get("jobId")
        if not job_id:
            continue

        job_id_str = str(job_id)
        job_hash = generate_job_hash(company, job_id_str)

        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(
                f"{company_label}: existing job_hash found for job_id={job_id_str}, "
                "stopping subsequent jobs (API assumed sorted)."
            )
            break

        title = job.get("title")
        category = job.get("category")
        city = job.get("cityState")
        workplace_type = job.get("RemoteType")
        apply_link = job.get("applyUrl")
        detail_link = f"https://careers.cisco.com/global/en/job/{job_id_str}"

        enrichment = get_enrichment(title, detail_link)

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id_str,
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=category,
            min_yoe=enrichment["min_yoe"],
            max_yoe=enrichment["max_yoe"],
            city=city,
            state=None,
            country="India",
            workplace_type=workplace_type,
            skills=_extract_skills(job.get("ml_skills")),
            apply_link=apply_link,
            created_at=today_str,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
            print(f"{company_label}: saved job_id={job_id_str}")

        if stop_fetch:
            print(
                f"{company_label}: duplicate detected while saving job_id={job_id_str}, stopping further fetch."
            )
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

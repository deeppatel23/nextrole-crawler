"""
GET API for Morgan Stanley: https://morganstanley.eightfold.ai/api/pcsx/search?domain=morganstanley.com&query=&location=&start=0&sort_by=timestamp&filter_country=India

Sample response:
{
    "status": 200,
    "error": {
        "message": "",
        "body": ""
    },
    "data": {
        "positions": [
            {
                "id": 549795501880,
                "displayJobId": "PT-JR028074",
                "name": "Dir, P3, Data & Analytics Eng III : Job Level - Director",
                "locations": [
                    "Mumbai, Maharashtra, India"
                ],
                "standardizedLocations": [
                    "Mumbai, MH, IN"
                ],
                "postedTs": 1771286400,
                "solrScore": null,
                "stars": 0,
                "department": "Data & Analytics Engineering",
                "creationTs": 1768867200,
                "isHot": 0,
                "workLocationOption": "onsite",
                "locationFlexibility": null,
                "atsJobId": "PT-JR028074",
                "positionUrl": "/careers/job/549795501880"
            }
        ]
    }
}

Get job details API: 
https://morganstanley.eightfold.ai/api/pcsx/position_details?position_id={id}&domain=morganstanley.com&hl=en

Job details response:
{
    "status": 200,
    "error": {
        "message": "",
        "body": ""
    },
    "data": {
        "id": 549795501880,
        "displayJobId": "PT-JR028074",
        "name": "Dir, P3, Data & Analytics Eng III : Job Level - Director",
        "locations": [
            "Mumbai, Maharashtra, India"
        ],
        "standardizedLocations": [
            "Mumbai, MH, IN"
        ],
        "postedTs": 1771286400,
        "stars": 0,
        "department": "Data & Analytics Engineering",
        "creationTs": 1768867200,
        "jobDescription": "***Pre-id - Vennila Rajagopal<br><br>Position Overview: <br> <br>AIDT under Management Technology provides consistent, trusted and integrated business information across all platforms and delivery channels within Morgan Stanley Wealth Management (MSWM) businesses. AIDT areas of expertise include but are not limited to Enterprise Data Warehouse (EDW) and Datalake (DL), cross-business-unit Information Management, Business Intelligence and more. AIDT team synthesizes and provisions a multitude of business-critical metrics and underlying detailed information in all Core areas of MSWM. Examples of essential data domains and metrics include Assets, Revenue, Account Demographics, Financial Advisor (FA) information, Securities and Cash Transactions, and so on. <br><br> <br>As an integral part of AIDT,?Data Integration & Delivery team?is tasked with providing insightful and accurate business intelligence on Strategic Client Reporting (SCR), Account Performance metrics, Investment Insights, Trade Surveillance, Anti-Money Laundering and many other aspects of Wealth Management business. The candidate may be assigned to a single Legal and Compliance project or several projects. The role requires credibility, confidence and active participation with leadership teams, business units and technology groups across the project lifecycle. Specific assignments will depend on the size and complexity of the project? <br><br>Roles and Responsibilities <br><br>The candidate is expected to <br><br>Undertaking end-to-end project delivery (from inception to post-implementation support), including review and finalization of business requirements, creation of functional specifications and/or system designs, and ensuring that end-solution meets business needs and expectations. <br><br>Develop strategy and methodology to drive process and people optimizations. <br><br>Analysis of existing designs and interfaces and applying design modifications or enhancements <br><br>Providing business insights and analysis findings for ad-hoc data requests <br><br>Providing reporting-line transparency through periodic updates on project or task status; Education:??Bachelor's/Master's Degree in Engineering, preferably Computer Science/Engineering <br><br> Primary Skills / Must have <br><br>5+ years of experience with the technical analysis and design, development and implementation of data warehousing / Data Lake solutions; <br><br>Strong communication skills to be able to put forth his opinion in global meetings <br><br>Good proficiency in Microsoft Excel, Microsoft Power point and other tools <br><br>5+ years relational database experience (experience with Teradata is a big plus); <br><br>Process oriented, focused on standardization, streamlining, and implementation of best practices delivery approach; <br><br>Excellent problem solving and analytical skills; <br><br>Excellent verbal and written communication skills; <br><br>Experience in optimizing large data loads; <br><br>? <br><br>Secondary Skills / Desired skills <br><br>Should be a good Team player; <br><br>Exposure to an Agile Development environment would be a plus. <br><br>Should have good knowledge of AI usage.<p></p><p><b>WHAT YOU CAN EXPECT FROM MORGAN STANLEY:</b> </p><p></p><p> At Morgan Stanley, we raise, manage and allocate capital for our clients \u2013 helping them reach their goals. We do it in a way that\u2019s differentiated \u2013 and we\u2019ve done that for 90 years. \u00a0Our values - putting clients first, doing the right thing, leading with exceptional ideas, committing to diversity and inclusion, and giving back - aren\u2019t just beliefs, they guide the decisions we make every day to do what's best for our clients, communities and more than 80,000 employees in 1,200 offices across 42 countries. At Morgan Stanley, you\u2019ll find an opportunity to work alongside the best and the brightest, in an environment where you are supported and empowered. Our teams are relentless collaborators and creative thinkers, fueled by their diverse backgrounds and experiences. We are proud to support our employees and their families at every point along their work-life journey, offering some of the most attractive and comprehensive employee benefits and perks in the industry. There\u2019s also ample opportunity to move about the business for those who show passion and grit in their work. </p><p></p><p>To learn more about our offices across the globe, please copy and paste https://www.morganstanley.com/about-us/global-offices\u200b into your browser.</p><p></p><p style=\"text-align:inherit\"></p><p style=\"text-align:left\"><span>Morgan Stanley is an equal opportunities employer. We work to provide a supportive and inclusive environment where all individuals can maximize their full potential. Our skilled and creative workforce is comprised of individuals drawn from a broad cross section of the global communities in which we operate and who reflect a variety of backgrounds, talents, perspectives, and experiences. Our strong commitment to a culture of inclusion is evident through our constant focus on recruiting, developing, and advancing individuals based on their skills and talents.</span></p><p style=\"text-align:inherit\"></p>",
        "location": "Mumbai, Maharashtra, India",
        "isHot": 0,
        "workLocationOption": "onsite",
        "locationFlexibility": null,
        "atsJobId": "PT-JR028074",
        "positionUrl": "/careers/job/549795501880",
        "publicUrl": "https://morganstanley.eightfold.ai/careers/job/549795501880",
        "efcustomTextTextTimeType": [
            "Full time"
        ],
        "efcustomTextPcsPostingJobLevel": [
            "Professional"
        ],
        "jdHighlight": [],
        "positionExtraDetails": {
            "videos": [],
            "blogs": [],
            "perks": [],
            "pymww": {
                "people": [],
                "count": 0
            },
            "customContent": [
                {
                    "body": "",
                    "title": ""
                }
            ]
        },
        "positionUserActions": {
            "isSaved": false,
            "applyAction": {
                "status": "link_off",
                "applyUrl": "https://ms.wd5.myworkdayjobs.com/External/job/Mumbai-India/Dir--P3--Data---Analytics-Eng-III---Job-Level---Director_PT-JR028074-1/apply?src=Eightfold"
            }
        }
    },
    "metadata": null
}

Mapping with role_detail fields with job details response:
    job_hash: str = hash of id + "Morgan Stanley"
    
    # identity
    job_id: str = id
    company: str = "Morgan Stanley"

    # core job info
    title: str = name
    role: Optional[str] = None
    category: Optional[str] = None = get_enrichment with extra_text = fetch department + jobDescription 

    # experience
    min_yoe: Optional[int] = None = get_enrichment with extra_text = fetch department + jobDescription 
    max_yoe: Optional[int] = None = get_enrichment with extra_text = fetch department + jobDescription 

    # location
    city: Optional[str] = None = location.split(",")[0].strip() if location else None
    state: Optional[str] = None
    country: Optional[str] = None = location.split(",")[1].strip() if location else None
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = get_enrichment with extra_text = fetch department + jobDescription 

    # links
    apply_link: Optional[str] = None = f"https://morganstanley.eightfold.ai/careers/job/{id}"
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = date.now().isoformat()
    updated_at: Optional[str] = None

Loop the main API first. You will get at max 10 in one batch. Change the start parameter to get the next batch of jobs. 
If it does not exist, call the job details API to get the job description and other details to build the role_detail object.
If in any batch the main API returns an empty list of positions, stop the loop as well since it means there are no more jobs to fetch.
If in any batch, the main API returns a list of positions of size < 10, it means it is the last batch, so we can stop the loop after processing that batch.

Main API response is sorted by time, so for each job details, check if it already exists in mongo (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well for that siteNumber since they will be duplicates. 

Add this company details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.

"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

SEARCH_API_URL = "https://morganstanley.eightfold.ai/api/pcsx/search"
DETAIL_API_URL = "https://morganstanley.eightfold.ai/api/pcsx/position_details"
CAREERS_JOB_URL_PREFIX = "https://morganstanley.eightfold.ai/careers/job/"
DEFAULT_PAGE_SIZE = 10


def _build_search_url(start: int, country: str) -> str:
    params = {
        "domain": "morganstanley.com",
        "query": "",
        "location": "",
        "start": str(start),
        "sort_by": "timestamp",
        "filter_country": country,
    }
    return f"{SEARCH_API_URL}?{urlencode(params)}"


def _build_detail_url(position_id: str) -> str:
    params = {
        "position_id": position_id,
        "domain": "morganstanley.com",
        "hl": "en",
    }
    return f"{DETAIL_API_URL}?{urlencode(params)}"


def _extract_positions(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    positions = data.get("positions")
    if not isinstance(positions, list):
        return []
    return [position for position in positions if isinstance(position, dict)]


def _extract_detail_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return {}


def _extract_location(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(location, str) or not location.strip():
        return None, None
    parts = [part.strip() for part in location.split(",") if part.strip()]
    city = parts[0] if parts else None
    country = parts[1] if len(parts) > 1 else None
    return city, country


def _get_location_text(position: Dict[str, Any], detail: Dict[str, Any]) -> Optional[str]:
    location = detail.get("location")
    if isinstance(location, str) and location.strip():
        return location

    detail_locations = detail.get("locations")
    if isinstance(detail_locations, list) and detail_locations:
        first = detail_locations[0]
        if isinstance(first, str) and first.strip():
            return first

    listing_locations = position.get("locations")
    if isinstance(listing_locations, list) and listing_locations:
        first = listing_locations[0]
        if isinstance(first, str) and first.strip():
            return first

    return None


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
    company = "Morgan Stanley"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    country = source_cfg.get("filter_country") or "India"
    page_size = source_cfg.get("page_size", DEFAULT_PAGE_SIZE)
    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = DEFAULT_PAGE_SIZE
    if page_size <= 0:
        page_size = DEFAULT_PAGE_SIZE

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

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0
    saved_for_site = 0
    start = 0
    iteration = 1

    while True:
        if saved_for_site >= max_saved:
            print(
                f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping."
            )
            break

        search_url = _build_search_url(start=start, country=str(country))
        print(f"{company_label}: start iteration {iteration} calling {search_url}")
        try:
            payload = call_api(method="GET", url=search_url)
        except Exception as exc:
            print(f"{company_label}: failed search API call at start={start}: {exc}")
            break

        positions = _extract_positions(payload)
        if not positions:
            print(f"{company_label}: iteration {iteration} returned no positions, stopping.")
            break

        print(f"{company_label}: iteration {iteration} fetched {len(positions)} positions.")

        for position in positions:
            if saved_for_site >= max_saved:
                print(
                    f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping."
                )
                return total_saved

            raw_id = position.get("id")
            position_id = str(raw_id).strip() if raw_id is not None else ""
            title = str(position.get("name") or "").strip()
            if not position_id or not title:
                continue

            job_hash = generate_job_hash(company, position_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(
                    f"{company_label}: existing job_hash found for position_id={position_id}, "
                    "stopping all subsequent jobs because API is sorted."
                )
                return total_saved

            detail_data: Dict[str, Any] = {}
            detail_url = _build_detail_url(position_id)
            try:
                detail_payload = call_api(method="GET", url=detail_url)
                detail_data = _extract_detail_data(detail_payload)
            except Exception as exc:
                print(
                    f"{company_label}: detail API failed for position_id={position_id}: {exc}. "
                    "Using listing data."
                )

            department = str(
                detail_data.get("department") or position.get("department") or ""
            ).strip()
            job_description = str(detail_data.get("jobDescription") or "").strip()
            enrichment_text = " ".join(
                text for text in [department, job_description] if text
            ).strip()

            apply_link = f"{CAREERS_JOB_URL_PREFIX}{position_id}"
            enrichment = get_enrichment(
                title=title,
                apply_link=apply_link,
                extra_text=enrichment_text or None,
            )

            location_text = _get_location_text(position, detail_data)
            city, country_value = _extract_location(location_text)
            workplace_type = (
                detail_data.get("workLocationOption") or position.get("workLocationOption")
            )
            if isinstance(workplace_type, str):
                workplace_type = workplace_type.strip() or None
            else:
                workplace_type = None

            role = RoleDetail(
                job_hash=job_hash,
                job_id=position_id,
                company=company,
                title=title,
                role=None,
                category=enrichment["category"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                city=city,
                state=None,
                country=country_value,
                workplace_type=workplace_type,
                skills=enrichment["skills"],
                apply_link=apply_link,
                source_type=source_type,
                created_at=created_at,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                saved_for_site += saved_count
                checker.record(job_hash)
                print(
                    f"{company_label}: saved position_id={position_id} "
                    f"(saved={total_saved}, site_saved={saved_for_site})."
                )
            if stop_fetch:
                print(
                    f"{company_label}: duplicate detected while saving position_id={position_id}, "
                    "stopping further fetch."
                )
                return total_saved

        if len(positions) < page_size:
            print(
                f"{company_label}: iteration {iteration} had {len(positions)} positions (<{page_size}), stopping."
            )
            break

        start += page_size
        iteration += 1

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

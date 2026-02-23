"""

GET API curl command
curl --location 'https://io.spire2grow.com/ies/v1/p/requisition/_search?page=1&size=6&selectedSortOrder=desc&selectedSortField=postedOn' \
--header 'accept: */*' \
--header 'accept-language: en-GB,en-US;q=0.9,en;q=0.8' \
--header 'content-type: application/json' \
--header 'language: en' \
--header 'origin: https://jobs.myntra.com' \
--header 'referer: https://jobs.myntra.com/' \
--header 'workflowid: WFU_1771257105860000' \
--header 'workspaceid: MYNTRA-93as3' \
--header 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'


Sample response
{
    "entities": [
        {
            "id": "r_ea628a1d7bfe3920a666959b119bbe78",
            "displayId": "6592513870",
            "workspaceId": "MYNTRA-93as3",
            "jobTitle": "Technical Lead - Data Platform",
            "requiredExperienceInMonths": {
                "from": 72,
                "to": 96
            },
            "skills": [
                {
                    "skill": "data platform",
                    "isMandatory": true,
                    "reqId": "r_ea628a1d7bfe3920a666959b119bbe78"
                },
                {
                    "skill": "Java OR python OR scala",
                    "isMandatory": true,
                    "reqId": "r_ea628a1d7bfe3920a666959b119bbe78"
                }
            ],
            "jobLocation": [
                {
                    "city": "Bengaluru",
                    "state": "Karnataka",
                    "country": "India",
                    "fqLocationName": "Bengaluru, Karnataka, India",
                    "cityCode": "Bangalore",
                    "stateCode": "Karnataka",
                    "countryCode": "India",
                    "geoCode": {
                        "lat": 12.9715987,
                        "lon": 77.5945627
                    },
                    "originalCityName": "MYNHQ",
                    "originalStatename": "Karnataka",
                    "originalCountryName": "India"
                }
            ],
            "requiredEducation": [],
            "hotJob": {
                "isHot": false,
                "markedOn": 1771227040000
            },
            "departmentName": "Analytics (F1205)",
            "employmentType": "FULL_TIME",
            "jobPosting": {
                "startDate": 1771227040000,
                "endDate": 1776411040000,
                "status": "ACTIVE"
            },
            "createdOn": 1771227399000,
            "updatedOn": 1771227399000,
            "jobDescription": "to be added ML platform",
            "aboutCompany": "<b>Who are we?</b><br><br> <p>Myntra is Indias leading fashion and lifestyle platform, where technology meets creativity. As pioneers in fashion e-commerce, we’ve always believed in disrupting the ordinary.</p> <p>We thrive on a shared passion for fashion, a drive to innovate to lead, and an environment that empowers each one of us to pave our own way. We’re bold in our thinking, agile in our execution, and collaborative in spirit.<br>Here, we create MAGIC by inspiring vibrant and joyous self-expression and expanding fashion possibilities for India, while staying true to what we believe in.</p> <p>We believe in taking bold bets and changing the fashion landscape of India. We are a company that is constantly evolving into newer and better forms and we look for people who are ready to evolve with us.<br>From our humble beginnings as a customization company in 2007 to being technology and fashion pioneers today, Myntra is going places and we want you to take part in this journey with us.</p> <p>Working at Myntra is challenging but fun - we are a young and dynamic team, firm believers in meritocracy, believe in equal opportunity, encourage intellectual curiosity and empower our teams with the right tools, space, and opportunities.</p>\n",
            "jobStatus": {
                "statusDisplay": "Open",
                "statusCode": "OPEN"
            },
            "fcpr": {
                "id": null,
                "roleSkill": null,
                "jobFamily": null
            },
            "recruiter": {
                "fullName": "Madhu Kiran M V",
                "emailId": "madhu.kiran@myntra.com"
            }
        }
    ],
    "total": 15
}

Mapping with role_details
    job_hash: str = hash of job_id + Myntra
    
    # identity
    job_id: str = id
    company: str = Myntra

    # core job info
    title: str = jobTitle
    role: Optional[str] = None
    category: Optional[str] = call category_enricher(jobTitle, departmentName, skills text comma separated)

    # experience
    min_yoe: Optional[int] = None = requiredExperienceInMonths.from / 12
    max_yoe: Optional[int] = None = requiredExperienceInMonths.to / 12

    # location
    city: Optional[str] = None = jobLocation[0].city
    state: Optional[str] = None
    country: Optional[str] = None = jobLocation[0].country
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = top 3 [skill.skill for skill in skills]

    # links
    apply_link: Optional[str] = None = "https://jobs.myntra.com/jobs/" + displayId + "?ref=job-share-internal-link"
    source_type: Optional[str] = None

    # metadata
    created_at: Optional[str] = None = datetime.now().isoformat().date()
    updated_at: Optional[str] = None

Check for one job at a time from a response. Loop in total/size and page number until we have iterated through all the jobs or reached max_saved_jobs, total parameter is there in response and size parameter is the query parameter we are sending in the API request.

The response if the API for a particular siteNumber is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well for that siteNumber since they will be duplicates.

Add this comapny details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

API_URL = "https://io.spire2grow.com/ies/v1/p/requisition/_search"
DEFAULT_SIZE = 6
DEFAULT_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "language": "en",
    "origin": "https://jobs.myntra.com",
    "referer": "https://jobs.myntra.com/",
    "workflowid": "WFU_1771257105860000",
    "workspaceid": "MYNTRA-93as3",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    ),
}


def _to_years(raw_months: Any) -> Optional[int]:
    if raw_months is None:
        return None
    try:
        months = int(raw_months)
    except (TypeError, ValueError):
        return None
    if months < 0:
        return None
    return months // 12


def _extract_skills(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []

    skills: List[str] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        skill = item.get("skill")
        if not skill:
            continue
        skill_text = str(skill).strip()
        if not skill_text:
            continue
        skills.append(skill_text)
        if len(skills) >= 3:
            break
    return skills


def _extract_location(entity: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    locations = entity.get("jobLocation")
    if not isinstance(locations, list) or not locations:
        return None, None
    first = locations[0]
    if not isinstance(first, dict):
        return None, None
    city = first.get("city")
    country = first.get("country")
    return (
        str(city).strip() if city else None,
        str(country).strip() if country else None,
    )


def _fetch_page(page: int, size: int) -> Dict[str, Any]:
    params = {
        "page": page,
        "size": size,
        "selectedSortOrder": "desc",
        "selectedSortField": "postedOn",
    }
    return call_api(
        method="GET",
        url=API_URL,
        headers=DEFAULT_HEADERS,
        params=params,
    )


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Myntra"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "API").upper()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")
    page_size = int(source_cfg.get("page_size", DEFAULT_SIZE))
    if page_size <= 0:
        page_size = DEFAULT_SIZE

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

    print(f"{company_label}: fetching page=1 size={page_size}")
    first_payload = _fetch_page(1, page_size)
    first_entities = first_payload.get("entities")
    total = first_payload.get("total")

    if not isinstance(first_entities, list) or not first_entities:
        print(f"{company_label}: no jobs found.")
        return 0

    if not isinstance(total, int) or total < 0:
        total = len(first_entities)
    total_pages = max(1, math.ceil(total / page_size))
    print(f"{company_label}: total={total} page_size={page_size} pages={total_pages}")

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0

    def _process_entities(entities: List[Dict[str, Any]], page_number: int) -> bool:
        nonlocal total_saved
        for entity in entities:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                return True
            if not isinstance(entity, dict):
                continue

            job_id = entity.get("id")
            title = entity.get("jobTitle")
            display_id = entity.get("displayId")
            if not job_id or not title:
                continue

            job_hash = generate_job_hash(company, str(job_id))
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(
                    f"{company_label}: duplicate job_hash for job_id={job_id} on page={page_number}; "
                    "stopping subsequent jobs."
                )
                return True

            skills = _extract_skills(entity.get("skills"))
            skills_text = ", ".join(skills)
            category = match_category(
                title=str(title),
                department=entity.get("departmentName"),
                skills=skills,
                page_text=skills_text,
                category_hint=entity.get("departmentName"),
            )

            req_exp = entity.get("requiredExperienceInMonths")
            req_from = req_exp.get("from") if isinstance(req_exp, dict) else None
            req_to = req_exp.get("to") if isinstance(req_exp, dict) else None
            min_yoe = _to_years(req_from)
            max_yoe = _to_years(req_to)

            city, country = _extract_location(entity)
            apply_link = None
            if display_id:
                apply_link = f"https://jobs.myntra.com/jobs/{display_id}?ref=job-share-internal-link"

            role = RoleDetail(
                job_hash=job_hash,
                job_id=str(job_id),
                company=company,
                source_type=source_type,
                title=str(title),
                role=None,
                category=category,
                min_yoe=min_yoe,
                max_yoe=max_yoe,
                city=normalize_city(city),
                state=None,
                country=country,
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
                print(f"{company_label}: saved job_id={job_id}")

            if stop_fetch:
                print(
                    f"{company_label}: duplicate detected while saving job_id={job_id}; stopping further fetch."
                )
                return True
        return False

    should_stop = _process_entities(first_entities, 1)
    if should_stop:
        print(f"{company_label}: total saved {total_saved} jobs.")
        return total_saved

    for page in range(2, total_pages + 1):
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break
        print(f"{company_label}: fetching page={page} size={page_size}")
        payload = _fetch_page(page, page_size)
        entities = payload.get("entities")
        if not isinstance(entities, list) or not entities:
            print(f"{company_label}: page={page} returned no jobs, stopping.")
            break
        if _process_entities(entities, page):
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

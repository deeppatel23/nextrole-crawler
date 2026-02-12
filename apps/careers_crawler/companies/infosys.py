"""
This is for Infosys Limited, Infosys BPM is not included here since it has a different API and response structure.
Make one call to this GET API : https://intapgateway.infosysapps.com/careersci/search/intapjbsrch/getCareerSearchJobs?sourceId=1,21&searchText=ALL
Response:
[
    {
        "postingTitle": "Senior Analyst - Privacy & Data Protection",
        "createdOn": "2026-02-10T11:35:53.994",
        "roleDesignation": "Senior Analyst - Privacy & Data Protection",
        "unit": "Data Privacy Office",
        "location": "BANGALORE",
        "skills": null,
        "postingDescription": "",
        "technicalRequirement": "\tConduct privacy compliance checks for clients, internal solutions and processes\n\tReview and respond to client privacy requirements \n\tImplement and monitor the data privacy policies and associated processes across functions and business units.",
        "additionalResponsibility": "\tLearnability\n\tBusiness Communication & Articulation Skills\n\tAdaptability to interact and work with people from different nations & culture",
        "postingId": 236690,
        "requisitionId": 238477,
        "referenceCode": "INFSYS-EXTERNAL-238477",
        "sourceId": 1,
        "minExperienceLevel": 3,
        "maxExperienceLevel": 6,
        "rolesResponsibilities": "\tConduct Privacy impact assessments for new technology solution or process such as BYOD, Biometric Authentication for Physical Security, Smartphone app based solutions, Web applications\n\tAnalyze and Recommend Privacy by Design in new products and platforms \n\tIdentify and Mitigate risks related to PII \n\tInvestigate any data privacy breaches including root cause analysis, corrective and preventive actions.\n\tParticipate in strategic improvement initiatives",
        "company": "Infosys Limited",
        "country": "India",
        "preferredSkills": "Foundational ->Data privacy->Privacy by design",
        "genericSkills": "",
        "educationalRequirement": "Bachelor of Engineering,Bachelor of Laws",
        "expiryDate": "2026-05-21T00:00:00",
        "publicationId": 1034348,
        "hotjob": "N",
        "companyHiringTypeId": 1,
        "functionalArea": "Data Privacy Office"
    }
]
Mapping with our model:
    job_hash: str = hash of postingId + requisitionId + Infosys

    # identity
    job_id: str = postingId - requisitionId
    company: str = "Infosys"

    # core job info
    title: str = postingTitle
    role: Optional[str] = None
    category: Optional[str] = None = functionalArea

    # experience
    min_yoe: Optional[int] = minExperienceLevel
    max_yoe: Optional[int] = maxExperienceLevel

    # location
    city: Optional[str] = location
    state: Optional[str] = None
    country: Optional[str] = country
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = take top 3 skills [] max, from preferredSkills.split(","), and then each split by "->" and take the last part as the skill name.

    # links
    apply_link: Optional[str] = f"https://career.infosys.com/jobdesc?jobReferenceCode={referenceCode}"
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = now()
    updated_at: Optional[str] = None


Check for one job at a time from a response[].

The response if the API is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well since they will be duplicates.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop).

Try to add print statement wherever necessary.

No enrichment with LLM API call is needed, just direct mapping and saving to DB.

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

API_URL = (
    "https://intapgateway.infosysapps.com/careersci/search/intapjbsrch/"
    "getCareerSearchJobs?sourceId=1,21&searchText=ALL"
)


def _fetch_jobs() -> List[Dict[str, Any]]:
    response = call_api(method="GET", url=API_URL)
    return response if isinstance(response, list) else []


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_skills(preferred_skills: Any) -> List[str]:
    if not isinstance(preferred_skills, str) or not preferred_skills.strip():
        return []

    skills: List[str] = []
    for raw_chunk in preferred_skills.split(","):
        chunk = raw_chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("->") if p.strip()]
        skill = parts[-1] if parts else None
        if not skill:
            continue
        skills.append(skill)
        if len(skills) >= 3:
            break

    return skills


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Infosys"
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
    now_iso = datetime.utcnow().date().isoformat()

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        if not isinstance(job, dict):
            continue

        posting_id = job.get("postingId")
        requisition_id = job.get("requisitionId")
        if posting_id is None or requisition_id is None:
            print(f"{company_label}: skipping job with missing postingId/requisitionId.")
            continue

        job_id = f"{posting_id}-{requisition_id}"
        job_hash = generate_job_hash(company, job_id)

        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(
                f"{company_label}: existing job_hash found for job_id={job_id}, "
                "stopping subsequent jobs (API assumed sorted)."
            )
            break

        reference_code = job.get("referenceCode")
        apply_link = None
        if reference_code:
            apply_link = (
                "https://career.infosys.com/jobdesc?jobReferenceCode="
                f"{reference_code}"
            )

        role = RoleDetail(
            job_hash=job_hash,
            job_id=job_id,
            company=company,
            source_type=source_type,
            title=job.get("postingTitle"),
            role=None,
            category=job.get("functionalArea"),
            min_yoe=_to_int(job.get("minExperienceLevel")),
            max_yoe=_to_int(job.get("maxExperienceLevel")),
            city=job.get("location"),
            state=None,
            country=job.get("country"),
            workplace_type=None,
            skills=_extract_skills(job.get("preferredSkills")),
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
            print(f"{company_label}: duplicate detected while saving, stopping further fetch.")
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

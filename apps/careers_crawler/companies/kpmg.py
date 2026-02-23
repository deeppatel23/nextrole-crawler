"""
GET Kpmg1: https://ejgk.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_1,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=25,sortBy=POSTING_DATES_DESC
Kpmg1 has siteNumber=CX_1

GET Kpmg2: https://ejgk.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_3,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=25,sortBy=POSTING_DATES_DESC
Kpmg2 has siteNumber=CX_3

GET Kpmg3: https://ejgk.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_3001,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=25,sortBy=POSTING_DATES_DESC
Kpmg3 has siteNumber=CX_3001

Sample response structure is common for each siteNumber
{
    "items": [
        {
            "SearchId": 1,
            "Keyword": null,
            "CorrectedKeyword": null,
            "UseExactKeywordFlag": false,
            "SuggestedKeyword": null,
            "ExecuteSpellCheckFlag": true,
            "Location": null,
            "LocationId": null,
            "Radius": 0,
            "RadiusUnit": "MI",
            "SelectedTitlesFacet": null,
            "SelectedCategoriesFacet": null,
            "SelectedPostingDatesFacet": null,
            "SelectedLocationsFacet": null,
            "LastSelectedFacet": null,
            "Facets": "LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS",
            "Offset": 0,
            "Limit": 25,
            "SortBy": "POSTING_DATES_DESC",
            "TotalJobsCount": 255,
            "Latitude": null,
            "Longitude": null,
            "SiteNumber": "CX_3001",
            "JobFamilyId": null,
            "PostingStartDate": null,
            "PostingEndDate": null,
            "SelectedWorkLocationsFacet": null,
            "RequisitionId": null,
            "CandidateNumber": null,
            "WorkLocationZipCode": null,
            "WorkLocationCountryCode": null,
            "SelectedFlexFieldsFacets": null,
            "OrganizationId": null,
            "SelectedOrganizationsFacet": null,
            "UserTargetFacetName": null,
            "UserTargetFacetInputTerm": null,
            "HotJobFlag": null,
            "WorkplaceType": null,
            "SelectedWorkplaceTypesFacet": null,
            "BotQRShortCode": null,
            "requisitionList": [
                {
                    "Id": "30036316",
                    "Title": "KDNI - Automation/Performance Testing - (CON)",
                    "PostedDate": "2026-02-13",
                    "PostingEndDate": null,
                    "Language": "US",
                    "PrimaryLocationCountry": "IN",
                    "GeographyId": 300002240961169,
                    "HotJobFlag": false,
                    "WorkplaceTypeCode": null,
                    "JobFamily": null,
                    "JobFunction": null,
                    "WorkerType": null,
                    "ContractType": null,
                    "ManagerLevel": null,
                    "JobSchedule": null,
                    "JobShift": null,
                    "JobType": null,
                    "StudyLevel": null,
                    "DomesticTravelRequired": null,
                    "InternationalTravelRequired": null,
                    "WorkDurationYears": null,
                    "WorkDurationMonths": null,
                    "WorkHours": null,
                    "WorkDays": null,
                    "LegalEmployer": null,
                    "BusinessUnit": null,
                    "Department": null,
                    "Organization": null,
                    "MediaThumbURL": null,
                    "ShortDescriptionStr": "",
                    "PrimaryLocation": "Bangalore, Karnataka, India",
                    "Distance": 1.7709408E+12,
                    "TrendingFlag": false,
                    "BeFirstToApplyFlag": false,
                    "Relevancy": 1E+1,
                    "WorkplaceType": "",
                    "ExternalQualificationsStr": null,
                    "ExternalResponsibilitiesStr": null,
                    "secondaryLocations": [],
                    "otherWorkLocations": [],
                    "workLocation": [
                        {
                            "LocationId": 300000007677415,
                            "LocationName": "Bangalore One",
                            "AddressLine1": "One",
                            "AddressLine2": null,
                            "AddressLine3": null,
                            "AddressLine4": null,
                            "Building": null,
                            "TownOrCity": "Bangalore",
                            "PostalCode": "560103",
                            "Country": "IN",
                            "Region1": null,
                            "Region2": "Karnataka",
                            "Region3": null,
                            "Latitude": 12.9307,
                            "Longitude": 77.6854
                        }
                    ],
                    "requisitionFlexFields": []
                }
            ]
        }
    ]
}

Job details UI page link format: https://ejgk.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{siteNumber}/jobs/preview/{job_id}

For each siteNumber, TotalJobsCount/Limit gives the total number of pages to crawl for that siteNumber. For example, for siteNumber=CX_3001, TotalJobsCount is 255 and Limit is 25, so we need to crawl 11 pages (0-10) for that siteNumber. Add offset=page_number*limit to the query params to get the next page of results. For example, for page_number=1, offset=25, for page_number=2, offset=50, and so on.

Mapping with role_detail fields:
    job_hash: str = generate_job_hash(KPMG, siteNumber, job_id)
    
    # identity
    job_id: str = requisitionList.Id + siteNumber
    company: str = KPMG

    # core job info
    title: str = requisitionList.Title
    role: Optional[str] = None
    category: Optional[str] = None = Open job detail page, and try to extract category from the page content. If not found, leave it as None. Make LLM call with the page text and ask it to identify the most relevant category for the job. If LLM returns a category, use it. Otherwise, leave it as None.

    # experience
    min_yoe: Optional[int] = None = Open job detail page, and try to extract min_yoe from the page content. If not found, leave it as None. Make LLM call with the page text and ask it to identify the minimum years of experience required for the job. If LLM returns a number, use it. Otherwise, leave it as None.
    max_yoe: Optional[int] = None = Open job detail page, and try to extract max_yoe from the page content. If not found, leave it as None. Make LLM call with the page text and ask it to identify the maximum years of experience required for the job. If LLM returns a number, use it. Otherwise, leave it as None.

    # location
    city: Optional[str] = None = requisitionList.PrimaryLocation[0].townOrCity
    state: Optional[str] = None
    country: Optional[str] = None = India
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = Open job detail page, and try to extract skills from the page content. If not found, leave it as empty list. Make LLM call with the page text and ask it to identify the top 5 relevant skills for the job. If LLM returns a list of skills, use it. Otherwise, leave it as empty list.

    # links
    apply_link: Optional[str] = None = https://ejgk.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{siteNumber}/jobs/preview/{job_id}
    source_type: Optional[str] = None = API

    # metadata
    created_at: Optional[str] = None = today's date in ISO format
    updated_at: Optional[str] = None

Check for one job at a time from a response.

The response if the API for a particular siteNumber is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well for that siteNumber since they will be duplicates.

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

API_URL = (
    "https://ejgk.fa.em2.oraclecloud.com/hcmRestApi/resources/latest/"
    "recruitingCEJobRequisitions"
)
SITE_NUMBERS = ("CX_1", "CX_3", "CX_3001")
DEFAULT_LIMIT = 25


def _build_params(site_number: str, limit: int, offset: int) -> Dict[str, str]:
    return {
        "onlyData": "true",
        "expand": (
            "requisitionList.workLocation,requisitionList.otherWorkLocations,"
            "requisitionList.secondaryLocations,flexFieldsFacet.values,"
            "requisitionList.requisitionFlexFields"
        ),
        "finder": (
            "findReqs;"
            f"siteNumber={site_number},"
            "facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3B"
            "CATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,"
            f"limit={limit},sortBy=POSTING_DATES_DESC,offset={offset}"
        ),
    }


def _extract_locations(job: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    work_locations = job.get("workLocation")
    if isinstance(work_locations, list) and work_locations:
        first = work_locations[0] if isinstance(work_locations[0], dict) else {}
        city = first.get("TownOrCity")
        state = first.get("Region2")
        return city, state

    primary_location = job.get("PrimaryLocation")
    if isinstance(primary_location, str):
        parts = [part.strip() for part in primary_location.split(",") if part.strip()]
        city = parts[0] if len(parts) > 0 else None
        state = parts[1] if len(parts) > 1 else None
        return city, state

    return None, None


def _get_job_lists(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int, int]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return [], 0, DEFAULT_LIMIT

    first_item = items[0] if isinstance(items[0], dict) else {}
    total_jobs = first_item.get("TotalJobsCount")
    limit = first_item.get("Limit")
    requisitions = first_item.get("requisitionList")

    if not isinstance(total_jobs, int):
        total_jobs = 0
    if not isinstance(limit, int) or limit <= 0:
        limit = DEFAULT_LIMIT
    if not isinstance(requisitions, list):
        requisitions = []
    requisitions = [r for r in requisitions if isinstance(r, dict)]
    return requisitions, total_jobs, limit


def _get_workplace_type(job: Dict[str, Any]) -> Optional[str]:
    workplace_type = job.get("WorkplaceType")
    if isinstance(workplace_type, str) and workplace_type.strip():
        return workplace_type.strip()
    return None


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "KPMG"
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

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0

    print(f"{company_label}: starting crawl for {len(SITE_NUMBERS)} site numbers.")

    for site_number in SITE_NUMBERS:
        print(f"{company_label}: crawling site_number={site_number}")
        site_saved = 0
        first_payload = call_api(
            method="GET",
            url=API_URL,
            params=_build_params(site_number, DEFAULT_LIMIT, 0),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        requisitions, total_jobs, limit = _get_job_lists(first_payload)
        total_pages = max(1, math.ceil(total_jobs / limit)) if total_jobs else 1
        print(
            f"{company_label}: site_number={site_number} total_jobs={total_jobs} limit={limit} pages={total_pages}"
        )

        stop_site = False
        for page_number in range(total_pages):
            if stop_site:
                break
            if site_saved >= max_saved:
                print(
                    f"{company_label}: site_number={site_number} reached max_saved_jobs={max_saved}, moving to next site."
                )
                break

            if page_number == 0:
                page_requisitions = requisitions
            else:
                offset = page_number * limit
                page_payload = call_api(
                    method="GET",
                    url=API_URL,
                    params=_build_params(site_number, limit, offset),
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                page_requisitions, _, _ = _get_job_lists(page_payload)

            print(
                f"{company_label}: site_number={site_number} page={page_number} jobs={len(page_requisitions)}"
            )

            for job in page_requisitions:
                if site_saved >= max_saved:
                    print(
                        f"{company_label}: site_number={site_number} reached max_saved_jobs={max_saved}, moving to next site."
                    )
                    break

                raw_job_id = job.get("Id")
                title = job.get("Title")
                if not raw_job_id or not title:
                    continue

                raw_job_id = str(raw_job_id)
                job_id = f"{raw_job_id}_{site_number}"
                job_hash = generate_job_hash(company, job_id)

                if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                    print(
                        f"{company_label}: duplicate job_hash for site_number={site_number}, job_id={raw_job_id}; stopping this site."
                    )
                    stop_site = True
                    break

                apply_link = (
                    "https://ejgk.fa.em2.oraclecloud.com/hcmUI/CandidateExperience/"
                    f"en/sites/{site_number}/jobs/preview/{raw_job_id}"
                )
                city, state = _extract_locations(job)
                enrichment = get_enrichment(
                    str(title),
                    apply_link,
                )

                role = RoleDetail(
                    job_hash=job_hash,
                    job_id=job_id,
                    company=company,
                    source_type=source_type,
                    title=str(title),
                    role=None,
                    category=enrichment.get("category"),
                    min_yoe=enrichment.get("min_yoe"),
                    max_yoe=enrichment.get("max_yoe"),
                    city=normalize_city(city),
                    state=state,
                    country="India",
                    workplace_type=_get_workplace_type(job),
                    skills=enrichment.get("skills", []),
                    apply_link=apply_link,
                    created_at=created_at,
                    updated_at=None,
                )

                saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
                if saved_count:
                    total_saved += saved_count
                    site_saved += saved_count
                    checker.record(job_hash)
                    print(
                        f"{company_label}: saved site_number={site_number} job_id={raw_job_id}"
                    )

                if stop_fetch:
                    print(
                        f"{company_label}: duplicate detected while saving for site_number={site_number}; stopping this site."
                    )
                    stop_site = True
                    break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

"""
JP Morgan & Chase careers parser.
Backend API: Oracle Cloud recruiting API with paging + per-job detail API for extra text.
Ordering: API uses RELEVANCY sort (not explicitly date-descending).
De-dupe: if job_hash exists (mongo), append_roles returns stop_fetch and the crawler stops early.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from clients.http_client import call_api
from config.config import OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

BASE_URL = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
DETAIL_URL = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
BASE_APPLY_URL = "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job"
LIMIT = 10
LOCATION_ID = 300000000289360  # India

DEFAULT_QUERY = {
    "onlyData": "true",
    "expand": (
        "requisitionList.workLocation,"
        "requisitionList.otherWorkLocations,"
        "requisitionList.secondaryLocations,"
        "flexFieldsFacet.values,"
        "requisitionList.requisitionFlexFields"
    ),
}

FINDER_BASE = (
    "findReqs;"
    "siteNumber=CX_1001,"
    "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS,"
    f"limit={LIMIT},"
    "keyword=\"Technology\","
    f"locationId={LOCATION_ID},"
    "sortBy=RELEVANCY"
)


def _build_url(offset: int) -> str:
    parsed = urlparse(BASE_URL)
    query = dict(parse_qsl(parsed.query))
    query.update(DEFAULT_QUERY)
    finder = f"{FINDER_BASE},offset={offset}"
    query["finder"] = finder
    new_query = urlencode(query)
    return urlunparse(parsed._replace(query=new_query))


def _build_detail_url(job_id: str) -> str:
    parsed = urlparse(DETAIL_URL)
    query = dict(parse_qsl(parsed.query))
    query["expand"] = "all"
    query["onlyData"] = "true"
    query["finder"] = f'ById;Id="{job_id}",siteNumber=CX_1001'
    new_query = urlencode(query)
    return urlunparse(parsed._replace(query=new_query))


def _fetch_external_description(job_id: str) -> Optional[str]:
    try:
        resp = call_api(
            method="GET",
            url=_build_detail_url(job_id),
        )
    except Exception:
        return None

    items = resp.get("items")
    if isinstance(items, list) and items:
        if isinstance(items[0], dict):
            return items[0].get("ExternalDescriptionStr")
    elif isinstance(items, dict):
        return items.get("ExternalDescriptionStr")

    return None


def _get_items(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    items = response.get("items")
    if isinstance(items, dict):
        return items
    if isinstance(items, list) and items:
        if isinstance(items[0], dict):
            return items[0]
    return None


def _iter_requisitions(response: Dict[str, Any]):
    items = _get_items(response)
    if not items:
        return

    requisitions = items.get("requisitionList", [])
    if not isinstance(requisitions, list):
        return

    for req in requisitions:
        if isinstance(req, dict):
            yield req


def _extract_city_state_country(primary_location: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # Example: "Bengaluru, Karnataka, India"
    if not primary_location:
        return None, None, None
    parts = [p.strip() for p in primary_location.split(",") if p.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], None, parts[1]
    if len(parts) == 1:
        return parts[0], None, None
    return None, None, None


def _get_total_count(response: Dict[str, Any]) -> int:
    items = _get_items(response)
    if not items:
        return 0

    facets = items.get("locationsFacet", [])
    if not isinstance(facets, list):
        return 0

    for facet in facets:
        if not isinstance(facet, dict):
            continue
        if facet.get("Id") == LOCATION_ID or facet.get("Name") == "India":
            count = facet.get("TotalCount")
            return count if isinstance(count, int) else 0

    return 0


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    today = datetime.utcnow().date().isoformat()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    print(f"JP Morgan & Chase: start iteration 1 calling {_build_url(0)}")
    first_response = call_api(
        method="GET",
        url=_build_url(0),
    )

    total_count = _get_total_count(first_response)
    total_pages = max(1, math.ceil(total_count / LIMIT))

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")

    def _build_batch(resp: Dict[str, Any]) -> List[RoleDetail]:
        batch: List[RoleDetail] = []
        for req in _iter_requisitions(resp):
            job_id = req.get("Id")
            if not job_id:
                continue

            city, state, country = _extract_city_state_country(req.get("PrimaryLocation"))

            apply_link = f"{BASE_APPLY_URL}/{job_id}"

            external_desc = _fetch_external_description(str(job_id))
            enrichment = get_enrichment(
                req.get("Title"),
                apply_link,
                external_desc,
            )

            role = RoleDetail(
                job_hash=generate_job_hash(company, str(job_id)),
                job_id=str(job_id),
                company=company,
                source_type=source_type,
                title=req.get("Title"),
                role=req.get("JobFamily"),
                category=enrichment["category"],
                city=normalize_city(city),
                state=state,
                country=country or req.get("PrimaryLocationCountry"),
                workplace_type=req.get("WorkplaceType"),
                apply_link=apply_link,
                skills=enrichment["skills"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                created_at=today,
                updated_at=req.get("PostingEndDate"),
            )
            batch.append(role)
        return batch

    total_saved = 0

    first_batch = _build_batch(first_response)
    saved_count, stop_fetch = append_roles(OUTPUT_FILE, first_batch)
    total_saved += saved_count
    print(f"JP Morgan & Chase: iteration 1 saved {saved_count} jobs")
    if stop_fetch:
        print("JP Morgan & Chase: existing job_hash found, stopping further fetch.")
        return total_saved
    if total_saved >= max_saved:
        print(f"JP Morgan & Chase: reached max_saved_jobs={max_saved}, stopping.")
        return total_saved

    for page_index in range(1, total_pages):
        if total_saved >= max_saved:
            print(f"JP Morgan & Chase: reached max_saved_jobs={max_saved}, stopping.")
            break
        offset = page_index * LIMIT
        print(f"JP Morgan & Chase: start iteration {page_index + 1} calling {_build_url(offset)}")
        page_response = call_api(
            method="GET",
            url=_build_url(offset),
        )
        batch = _build_batch(page_response)
        saved_count, stop_fetch = append_roles(OUTPUT_FILE, batch)
        total_saved += saved_count
        print(f"JP Morgan & Chase: iteration {page_index + 1} saved {saved_count} jobs")
        if stop_fetch:
            print("JP Morgan & Chase: existing job_hash found, stopping further fetch.")
            return total_saved

    return total_saved

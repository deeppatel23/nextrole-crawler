import math
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from clients.http_client import call_api
from config.config import OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles

BASE_URL = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
BASE_APPLY_URL = "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job"
LIMIT = 25
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

            role = RoleDetail(
                job_hash=generate_job_hash(company, str(job_id)),
                job_id=str(job_id),
                company=company,
                source_type=source_type,
                title=req.get("Title"),
                role=req.get("JobFunction"),
                category=req.get("JobFamily"),
                city=city,
                state=state,
                country=country or req.get("PrimaryLocationCountry"),
                workplace_type=req.get("WorkplaceType"),
                description=req.get("ShortDescriptionStr"),
                apply_link=apply_link,
                created_at=req.get("PostedDate"),
                updated_at=req.get("PostingEndDate"),
                # raw=req,
            )
            batch.append(role)
        return batch

    total_saved = 0

    first_batch = _build_batch(first_response)
    append_roles(OUTPUT_FILE, first_batch)
    total_saved += len(first_batch)
    print(f"JP Morgan & Chase: iteration 1 saved {len(first_batch)} jobs")

    for page_index in range(1, total_pages):
        offset = page_index * LIMIT
        print(f"JP Morgan & Chase: start iteration {page_index + 1} calling {_build_url(offset)}")
        page_response = call_api(
            method="GET",
            url=_build_url(offset),
        )
        batch = _build_batch(page_response)
        append_roles(OUTPUT_FILE, batch)
        total_saved += len(batch)
        print(f"JP Morgan & Chase: iteration {page_index + 1} saved {len(batch)} jobs")

    return total_saved

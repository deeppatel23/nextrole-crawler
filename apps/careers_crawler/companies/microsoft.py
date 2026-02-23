"""
Microsoft careers parser.
Backend API: Microsoft PCS search API with pagination + detail API for extra text.
Ordering: API sort_by=timestamp (newest-first), but API controls final order.
De-dupe: if job_hash exists (mongo), append_roles returns stop_fetch and the crawler stops early.
"""
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

from clients.http_client import call_api
from models.role_detail import RoleDetail
from config.config import OUTPUT_FILE
from utils.extract_utils import get_by_path, normalize_city
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

BASE_URL = "https://apply.careers.microsoft.com/api/pcsx/search"
DETAIL_URL = "https://apply.careers.microsoft.com/api/pcsx/position_details"
PAGE_SIZE = 10

DEFAULT_QUERY = {
    "domain": "microsoft.com",
    "query": "",
    "location": "india",
    "start": "0",
    "sort_by": "timestamp",
}

MAPPING = {
    "job_id": "id",
    "title": "name",
    "role": "department",
    "category": "department",
    "workplace_type": "workLocationOption",
    "apply_link": "positionUrl",
    "updated_at": "postedTs",
}


def _build_url(start: int) -> str:
    parsed = urlparse(BASE_URL)
    query = dict(parse_qsl(parsed.query))
    query.update(DEFAULT_QUERY)
    query["start"] = str(start)
    new_query = urlencode(query)
    return urlunparse(parsed._replace(query=new_query))


def _iter_positions(response: Dict[str, Any]):
    data = response.get("data")
    if not isinstance(data, dict):
        return

    positions = data.get("positions", [])
    if not isinstance(positions, list):
        return

    for position in positions:
        if isinstance(position, dict):
            yield position


def _parse_standardized_location(value: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], None, parts[1]
    if len(parts) == 1:
        return parts[0], None, None
    return None, None, None


def _parse_full_location(value: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    parts = [p.strip() for p in value.split(",") if p.strip()]
    if len(parts) == 3:
        return parts[2], parts[1], parts[0]
    if len(parts) == 2:
        return parts[1], None, parts[0]
    if len(parts) == 1:
        return parts[0], None, None
    return None, None, None


def _extract_location(position: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    standardized = position.get("standardizedLocations")
    if isinstance(standardized, list) and standardized:
        if isinstance(standardized[0], str):
            return _parse_standardized_location(standardized[0])

    locations = position.get("locations")
    if isinstance(locations, list) and locations:
        if isinstance(locations[0], str):
            return _parse_full_location(locations[0])

    return None, None, None


def _normalize_apply_link(position_url: Optional[str]) -> Optional[str]:
    if not position_url:
        return None
    if position_url.startswith("http://") or position_url.startswith("https://"):
        return position_url
    return f"https://apply.careers.microsoft.com{position_url}"


def _build_detail_url(job_id: str) -> str:
    return f"{DETAIL_URL}?position_id={job_id}&domain=microsoft.com&hl=en"


def _fetch_job_description(job_id: str) -> Optional[str]:
    try:
        resp = call_api(
            method="GET",
            url=_build_detail_url(job_id),
        )
    except Exception:
        return None

    data = resp.get("data")
    if isinstance(data, dict):
        desc = data.get("jobDescription")
        return desc if isinstance(desc, str) else None
    return None


def _get_total_count(response: Dict[str, Any]) -> int:
    data = response.get("data")
    if not isinstance(data, dict):
        return 0
    count = data.get("count")
    if isinstance(count, int):
        return count
    return 0


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    today = datetime.utcnow().date().isoformat()
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    print(f"Microsoft: start iteration 1 calling {_build_url(0)}")
    first_response = call_api(
        method="GET",
        url=_build_url(0),
    )

    total_count = _get_total_count(first_response)
    total_pages = max(1, math.ceil(total_count / PAGE_SIZE))

    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")

    total_saved = 0

    def _build_batch(resp: Dict[str, Any]) -> List[RoleDetail]:
        batch: List[RoleDetail] = []
        for position in _iter_positions(resp):
            mapped = {
                field: get_by_path(position, path)
                for field, path in MAPPING.items()
            }

            job_id = mapped.get("job_id")
            if not job_id:
                continue

            mapped.pop("job_id", None)
            mapped["apply_link"] = _normalize_apply_link(mapped.get("apply_link"))

            city, state, country = _extract_location(position)
            if city and not mapped.get("city"):
                mapped["city"] = city
            mapped["city"] = normalize_city(mapped.get("city"))
            if state and not mapped.get("state"):
                mapped["state"] = state
            if country and not mapped.get("country"):
                mapped["country"] = country

            detail_desc = _fetch_job_description(str(job_id))
            enrichment = get_enrichment(
                mapped.get("title"),
                mapped.get("apply_link"),
                detail_desc,
            )
            mapped["category"] = enrichment["category"]

            role = RoleDetail(
                job_hash=generate_job_hash(company, str(job_id)),
                job_id=str(job_id),
                company=company,
                source_type=source_type,
                skills=enrichment["skills"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                created_at=today,
                **mapped,
            )

            batch.append(role)
        return batch

    first_batch = _build_batch(first_response)
    saved_count, stop_fetch = append_roles(OUTPUT_FILE, first_batch)
    total_saved += saved_count
    print(f"Microsoft: iteration 1 saved {saved_count} jobs")
    if stop_fetch:
        print("Microsoft: existing job_hash found, stopping further fetch.")
        return total_saved
    if total_saved >= max_saved:
        print(f"Microsoft: reached max_saved_jobs={max_saved}, stopping.")
        return total_saved

    for page_index in range(1, total_pages):
        if total_saved >= max_saved:
            print(f"Microsoft: reached max_saved_jobs={max_saved}, stopping.")
            break
        start = page_index * PAGE_SIZE
        print(f"Microsoft: start iteration {page_index + 1} calling {_build_url(start)}")
        page_response = call_api(
            method="GET",
            url=_build_url(start),
        )
        batch = _build_batch(page_response)
        saved_count, stop_fetch = append_roles(OUTPUT_FILE, batch)
        total_saved += saved_count
        print(f"Microsoft: iteration {page_index + 1} saved {saved_count} jobs")
        if stop_fetch:
            print("Microsoft: existing job_hash found, stopping further fetch.")
            return total_saved

    return total_saved

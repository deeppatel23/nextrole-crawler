"""
Goldman Sachs careers parser.
Uses the GS GraphQL API to fetch India roles in pages of 20 (DESC order), then fetches per-role details.
Skips all saving if today <= last_saved, stops at max_saved_jobs, and stops on first existing job_hash.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from clients.http_client import call_api
from config.config import OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://api-higher.gs.com/gateway/api/v1/graphql"
PAGE_SIZE = 20

SEARCH_QUERY = """
query GetRoles($searchQueryInput: RoleSearchQueryInput!) {
  roleSearch(searchQueryInput: $searchQueryInput) {
    totalCount
    items {
      roleId
      corporateTitle
      jobTitle
      jobFunction
      locations {
        primary
        state
        country
        city
        __typename
      }
      status
      division
      skills
      jobType {
        code
        description
        __typename
      }
      externalSource {
        sourceId
        __typename
      }
      __typename
    }
    __typename
  }
}
""".strip()

DETAIL_QUERY = """
query GetRoleById($externalSourceId: String!, $externalSourceFetch: Boolean) {
  role(
    externalSourceId: $externalSourceId
    externalSourceFetch: $externalSourceFetch
  ) {
    roleId
    corporateTitle
    jobTitle
    jobFunction
    locations {
      primary
      state
      country
      city
      __typename
    }
    division
    descriptionHtml
    jobType {
      code
      description
      __typename
    }
    skillset
    compensation {
      minSalary
      maxSalary
      currency
      __typename
    }
    applyActive
    status
    externalSource {
      externalApplicationUrl
      applyInExternalSource
      sourceId
      secondarySourceId
      __typename
    }
    __typename
  }
}
""".strip()

ROLE_MAPPING = {
    "job_id": "roleId",
    "title": "jobTitle",
    "role": "corporateTitle",
    "category": "jobFunction",
    "division": "division",
    "skills": "skills",
    "locations": "locations",
    "job_type": "jobType.description",
    "source_id": "externalSource.sourceId",
}

DETAIL_MAPPING = {
    "job_id": "roleId",
    "title": "jobTitle",
    "role": "corporateTitle",
    "category": "jobFunction",
    "division": "division",
    "locations": "locations",
    "job_type": "jobType.description",
    "description_html": "descriptionHtml",
    "skillset": "skillset",
    "apply_link": "externalSource.externalApplicationUrl",
    "source_id": "externalSource.sourceId",
}


def _build_payload(page_number: int) -> Dict[str, Any]:
    return {
        "operationName": "GetRoles",
        "variables": {
            "searchQueryInput": {
                "page": {"pageSize": PAGE_SIZE, "pageNumber": page_number},
                "sort": {"sortStrategy": "RELEVANCE", "sortOrder": "DESC"},
                "filters": [
                    {
                        "filterCategoryType": "LOCATION",
                        "filters": [
                            {
                                "filter": "India",
                                "subFilters": [
                                    {
                                        "filter": "Karnataka",
                                        "subFilters": [{"filter": "Bengaluru", "subFilters": []}],
                                    },
                                    {
                                        "filter": "Maharashtra",
                                        "subFilters": [{"filter": "Mumbai", "subFilters": []}],
                                    },
                                    {
                                        "filter": "Telangana",
                                        "subFilters": [{"filter": "Hyderabad", "subFilters": []}],
                                    },
                                ],
                            }
                        ],
                    }
                ],
                "experiences": ["EARLY_CAREER", "PROFESSIONAL"],
                "searchTerm": "",
            }
        },
        "query": SEARCH_QUERY,
    }


def _get_by_path(obj: Dict[str, Any], path: str) -> Any:
    current: Any = obj
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _extract_primary_location(locations: Any) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(locations, list):
        return None, None, None
    primary = None
    for loc in locations:
        if isinstance(loc, dict) and loc.get("primary"):
            primary = loc
            break
    if primary is None and locations:
        primary = locations[0] if isinstance(locations[0], dict) else None
    if not isinstance(primary, dict):
        return None, None, None
    return primary.get("city"), primary.get("state"), primary.get("country")


def _build_apply_link(role_id: Optional[str], source_id: Optional[str]) -> Optional[str]:
    token = source_id or role_id
    if not token:
        return None
    return f"https://higher.gs.com/roles/{token}"


def _iter_items(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict):
        return []
    role_search = data.get("roleSearch")
    if not isinstance(role_search, dict):
        return []
    items = role_search.get("items")
    if isinstance(items, list):
        return [item for item in items if isinstance(item, dict)]
    return []


def _get_total_count(response: Dict[str, Any]) -> int:
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict):
        return 0
    role_search = data.get("roleSearch")
    if not isinstance(role_search, dict):
        return 0
    count = role_search.get("totalCount")
    return count if isinstance(count, int) else 0


def _fetch_role_detail(external_source_id: str) -> Optional[Dict[str, Any]]:
    try:
        resp = call_api(
            method="POST",
            url=API_URL,
            headers={"Content-Type": "application/json"},
            body={
                "operationName": "GetRoleById",
                "variables": {
                    "externalSourceId": external_source_id,
                    "externalSourceFetch": True,
                },
                "query": DETAIL_QUERY,
            },
        )
    except Exception:
        return None

    data = resp.get("data") if isinstance(resp, dict) else None
    if not isinstance(data, dict):
        return None
    role = data.get("role")
    return role if isinstance(role, dict) else None


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(str(value).split())


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = source_cfg.get("company")
    source_type = source_cfg.get("source_type")
    max_saved = source_cfg.get("max_saved_jobs", 9999)
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(
                    f"Goldman Sachs: last_saved={last_saved} is >= today={today.isoformat()}, skipping."
                )
                return 0
        except ValueError:
            print(f"Goldman Sachs: invalid last_saved={last_saved}, continuing.")

    print(f"Goldman Sachs: start iteration 1 calling {API_URL}")
    first_response = call_api(
        method="POST",
        url=API_URL,
        headers={"Content-Type": "application/json"},
        body=_build_payload(0),
    )

    total_count = _get_total_count(first_response)
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)

    total_saved = 0

    def _build_batch(items: List[Dict[str, Any]]) -> List[RoleDetail]:
        batch: List[RoleDetail] = []
        for item in items:
            mapped = {field: _get_by_path(item, path) for field, path in ROLE_MAPPING.items()}
            role_id = mapped.get("job_id")
            if not role_id:
                continue
            source_id = mapped.get("source_id")
            if not source_id:
                continue

            detail = _fetch_role_detail(str(source_id))
            if not detail:
                print(f"Goldman Sachs: failed to fetch detail for source_id={source_id}")
                continue

            detail_mapped = {
                field: _get_by_path(detail, path) for field, path in DETAIL_MAPPING.items()
            }

            city, state, country = _extract_primary_location(detail_mapped.get("locations"))
            apply_link = detail_mapped.get("apply_link") or _build_apply_link(
                role_id, source_id
            )

            skillset = detail_mapped.get("skillset")
            if isinstance(skillset, list):
                skills_text = " ".join(str(s) for s in skillset if s)
            else:
                skills_text = str(skillset or "")

            description_html = detail_mapped.get("description_html")
            extra_text = _clean_text(description_html)
            if skills_text:
                extra_text = f"{extra_text} {skills_text}".strip()

            enrichment = get_enrichment(
                detail_mapped.get("title"),
                apply_link,
                extra_text,
            )

            role = RoleDetail(
                job_hash=generate_job_hash(company, str(role_id)),
                job_id=str(role_id),
                company=company,
                source_type=source_type,
                title=detail_mapped.get("title"),
                role=detail_mapped.get("role"),
                category=enrichment["category"],
                city=normalize_city(city),
                state=state,
                country=country,
                workplace_type=detail_mapped.get("job_type"),
                apply_link=apply_link,
                skills=enrichment["skills"],
                min_yoe=enrichment["min_yoe"],
                max_yoe=enrichment["max_yoe"],
                created_at=today.isoformat(),
            )
            batch.append(role)
        return batch

    def _save_batch(batch: List[RoleDetail]) -> bool:
        nonlocal total_saved
        if not batch:
            return False

        remaining = max_saved - total_saved
        if remaining <= 0:
            print(f"Goldman Sachs: reached max_saved_jobs={max_saved}, stopping.")
            return True

        saved_in_batch = 0
        for role in batch:
            if total_saved >= max_saved:
                print(f"Goldman Sachs: reached max_saved_jobs={max_saved}, stopping.")
                return True
            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            total_saved += saved_count
            saved_in_batch += saved_count
            if stop_fetch:
                print("Goldman Sachs: existing job_hash found, stopping further fetch.")
                return True

        print(f"Goldman Sachs: saved {saved_in_batch}/{len(batch)} jobs in this batch")
        return False

    first_items = _iter_items(first_response)
    if _save_batch(_build_batch(first_items)):
        return total_saved

    for page_number in range(1, total_pages):
        if total_saved >= max_saved:
            print(f"Goldman Sachs: reached max_saved_jobs={max_saved}, stopping.")
            break
        print(f"Goldman Sachs: start iteration {page_number + 1} calling {API_URL}")
        response = call_api(
            method="POST",
            url=API_URL,
            headers={"Content-Type": "application/json"},
            body=_build_payload(page_number),
        )
        items = _iter_items(response)
        if _save_batch(_build_batch(items)):
            break

    return total_saved

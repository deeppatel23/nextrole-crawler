"""
Call GET API once: https://jobs.intuit.com/search-jobs/results?ActiveFacetID=0&CurrentPage=1&RecordsPerPage=999&TotalContentResults=&Distance=50&RadiusUnitType=0&Keywords=&Location=&ShowRadius=False&IsPagination=False&CustomFacetName=&FacetTerm=1269750&FacetType=2&FacetFilters%5B0%5D.ID=1269750&FacetFilters%5B0%5D.FacetType=2&FacetFilters%5B0%5D.Count=105&FacetFilters%5B0%5D.Display=India&FacetFilters%5B0%5D.IsApplied=true&FacetFilters%5B0%5D.FieldName=&SearchResultsModuleName=Search+Results&SearchFiltersModuleName=Search+Filters&SortCriteria=0&SortDirection=0&SearchType=3&OrganizationIds=27595&PostalCode=&ResultsType=0&fc=&fl=&fcf=&afc=&afl=&afcf=&TotalContentPages=NaN

This API response has only one parameter "filters" that contains all the jobs. 
in "flters" string, we have <ul class="search-list"> which has <li> for each job. 
Each <li> is similar to this structure:

<li data-remote="19498" data-count="0" data-intuit-jobid="19498" data-category-id="68357" data-category='Software Engineering' data-orig-location="">
    <a href="/job/bengaluru/software-engineer-1-backend/27595/91583618112" data-job-id="19498" class="sr-item" data-wa-link="" data-object="content" data-action="interacted" data-ui-object="link" data-ui_object_detail="Software Engineer 1 Backend" data-ui-action="clicked" data-object-detail="job-tile" data-title="Software Engineer 1 Backend">
        <h2>Software Engineer 1 Backend</h2>
                <span class="job-location">Bangalore, India</span>
    </a>
    <button type="button" class="js-save-job-btn" data-object="content" data-action="interacted" data-ui-object="button" data-ui-action="clicked" data-object-detail="liked-job" data-ui_object_detail="Software Engineer 1 Backend" data-job-id="91583618112" data-org-id="27595"><span class="wai">Save </span></button> 
</li>

job_hash: str = hash of company + job_id (which is data-intuit-jobid in the html)

# identity
job_id: str = data-intuit-jobid
company: str = "Intuit"

# core job info
title: str = h2 text
role: Optional[str] = None
category: Optional[str] = None = data-category in the html

# experience
min_yoe: Optional[int] = None = Open href link and look for experience requirements in the job description, make LLM call to extract min and max yoe
max_yoe: Optional[int] = None = Open href link and look for experience requirements in the job description, make LLM call to extract min and max yoe

# location
city: Optional[str] = None = Extract city from span with class "job-location", split by comma and take first part
state: Optional[str] = None
country: Optional[str] = None = India
workplace_type: Optional[str] = None  # onsite / remote / hybrid

# content
skills: List[str] = field(default_factory=list) = = Open href link and look for experience requirements in the job description, make LLM call to extract skills

# links
apply_link: Optional[str] = None = href link in the html
source_type: Optional[str] = None = html

# metadata
created_at: Optional[str] = None = today
updated_at: Optional[str] = None

Call the API once and fetch the data as per above. 
For each job (<li>) first check if it exists in mongo db when CAREERS_OUTPUT_DESTINATION = MONGO. If not then proceed to build the role_detail object and save.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop).

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""
from __future__ import annotations

from datetime import datetime
from html.parser import HTMLParser
from html import unescape as html_unescape
import re
from typing import Any, Dict, List, Optional

import requests

from config.config import OUTPUT_FILE, OUTPUT_DESTINATION
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = (
    "https://jobs.intuit.com/search-jobs/results?ActiveFacetID=0&CurrentPage=1&"
    "RecordsPerPage=999&TotalContentResults=&Distance=50&RadiusUnitType=0&"
    "Keywords=&Location=&ShowRadius=False&IsPagination=False&CustomFacetName=&"
    "FacetTerm=1269750&FacetType=2&FacetFilters%5B0%5D.ID=1269750&"
    "FacetFilters%5B0%5D.FacetType=2&FacetFilters%5B0%5D.Count=105&"
    "FacetFilters%5B0%5D.Display=India&FacetFilters%5B0%5D.IsApplied=true&"
    "FacetFilters%5B0%5D.FieldName=&SearchResultsModuleName=Search+Results&"
    "SearchFiltersModuleName=Search+Filters&SortCriteria=0&SortDirection=0&"
    "SearchType=3&OrganizationIds=27595&PostalCode=&ResultsType=0&fc=&fl=&"
    "fcf=&afc=&afl=&afcf=&TotalContentPages=NaN"
)
BASE_URL = "https://jobs.intuit.com"


class _SearchListParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.jobs: List[Dict[str, Optional[str]]] = []
        self._in_search_list = False
        self._ul_depth = 0
        self._in_li = False
        self._in_h2 = False
        self._in_location = False
        self._current: Optional[Dict[str, Optional[str]]] = None
        self._title_parts: List[str] = []
        self._location_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {k: v for k, v in attrs}
        if tag == "ul":
            class_attr = attrs_dict.get("class") or ""
            classes = class_attr.split()
            if "search-list" in classes:
                self._in_search_list = True
            if self._in_search_list:
                self._ul_depth += 1
            return

        if not self._in_search_list:
            return

        if tag == "li":
            self._in_li = True
            self._current = {
                "job_id": attrs_dict.get("data-intuit-jobid"),
                "category": attrs_dict.get("data-category"),
            }
            self._title_parts = []
            self._location_parts = []
            return

        if not self._in_li:
            return

        if tag == "a":
            href = attrs_dict.get("href")
            if href and self._current is not None:
                self._current.setdefault("apply_link", href)
        elif tag == "h2":
            self._in_h2 = True
        elif tag == "span":
            class_attr = attrs_dict.get("class") or ""
            if "job-location" in class_attr.split():
                self._in_location = True

    def handle_endtag(self, tag: str) -> None:
        if not self._in_search_list:
            return

        if tag == "ul":
            self._ul_depth -= 1
            if self._ul_depth <= 0:
                self._in_search_list = False
            return

        if not self._in_li:
            return

        if tag == "h2":
            self._in_h2 = False
            return

        if tag == "span" and self._in_location:
            self._in_location = False
            return

        if tag == "li":
            if self._current is not None:
                title = "".join(self._title_parts).strip()
                location = "".join(self._location_parts).strip()
                if title:
                    self._current["title"] = title
                if location:
                    self._current["location"] = location
                self.jobs.append(self._current)
            self._current = None
            self._in_li = False
            self._title_parts = []
            self._location_parts = []

    def handle_data(self, data: str) -> None:
        if not self._in_li:
            return
        if self._in_h2:
            self._title_parts.append(data)
        if self._in_location:
            self._location_parts.append(data)


def _fetch_search_payload() -> Optional[Dict[str, Any]]:
    resp = requests.get(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    print(f"Intuit: API status={resp.status_code} content_type={resp.headers.get('content-type')}")
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError:
        print("Intuit: response was not valid JSON.")
        return None
    if not isinstance(data, dict):
        print(f"Intuit: unexpected JSON type: {type(data)}")
        return None
    print(f"Intuit: response keys={list(data.keys())}")
    return data


def _parse_jobs(raw_html: str) -> List[Dict[str, Optional[str]]]:
    html_text = html_unescape(raw_html)
    print(f"Intuit: contains data-intuit-jobid={('data-intuit-jobid' in html_text)}")
    parser = _SearchListParser()
    parser.feed(html_text)
    print(f"Intuit: HTMLParser jobs={len(parser.jobs)}")
    if parser.jobs:
        print(f"Intuit: sample job={parser.jobs[0]}")
        return parser.jobs
    print("Intuit: HTMLParser found no jobs, trying regex fallback.")
    jobs = _parse_jobs_regex(html_text)
    print(f"Intuit: regex jobs={len(jobs)}")
    if jobs:
        print(f"Intuit: regex sample job={jobs[0]}")
    return jobs


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value).strip()


def _parse_jobs_regex(filters_html: str) -> List[Dict[str, Optional[str]]]:
    jobs: List[Dict[str, Optional[str]]] = []
    li_pattern = re.compile(
        r"(<li[^>]*data-intuit-jobid=(?P<quote>['\"]?)(?P<jobid>[^'\">\\s]+)(?P=quote)[^>]*>)(?P<body>.*?)</li>",
        flags=re.I | re.S,
    )
    print("Intuit: regex scanning for <li data-intuit-jobid=...>")
    for match in li_pattern.finditer(filters_html):
        li_tag = match.group(1)
        body = match.group("body")
        job_id = match.group("jobid")
        category_match = re.search(r"data-category=['\"]([^'\"]+)['\"]", li_tag, flags=re.I)
        category = category_match.group(1) if category_match else None
        href_match = re.search(r"<a[^>]*href=['\"]([^'\"]+)['\"]", body, flags=re.I)
        title_match = re.search(r"<h2[^>]*>(.*?)</h2>", body, flags=re.I | re.S)
        location_match = re.search(
            r"<span[^>]*class=['\"][^'\"]*job-location[^'\"]*['\"][^>]*>(.*?)</span>",
            body,
            flags=re.I | re.S,
        )
        title = _strip_tags(title_match.group(1)) if title_match else None
        location = _strip_tags(location_match.group(1)) if location_match else None
        apply_link = href_match.group(1) if href_match else None
        jobs.append(
            {
                "job_id": job_id,
                "category": category,
                "title": title,
                "location": location,
                "apply_link": apply_link,
            }
        )
    return jobs


def _normalize_apply_link(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"{BASE_URL}{href}"


def _extract_city(location: Optional[str]) -> Optional[str]:
    if not location:
        return None
    parts = [p.strip() for p in location.split(",") if p.strip()]
    return parts[0] if parts else None


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Intuit"
    company_label = source_cfg.get("company") or company
    source_type = (source_cfg.get("source_type") or "HTML").upper()
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

    print(f"{company_label}: fetching jobs from Intuit API")
    payload = _fetch_search_payload()
    if not payload:
        print(f"{company_label}: no payload returned.")
        return 0

    html_candidates: List[tuple[str, str]] = []
    for key in ("filters", "results"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            html_candidates.append((key, value))

    if not html_candidates:
        print(f"{company_label}: no HTML candidates found in payload.")
        return 0

    jobs: List[Dict[str, Optional[str]]] = []
    for key, html in html_candidates:
        print(f"{company_label}: parsing HTML from '{key}', length={len(html)}")
        print(f"{company_label}: {key} head={html[:300]!r}")
        jobs = _parse_jobs(html)
        if jobs:
            break

    if not jobs:
        print(f"{company_label}: no jobs parsed from payload HTML.")
        return 0

    checker = MongoJobHashChecker()
    today_str = today.isoformat()
    total_saved = 0

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        job_id = job.get("job_id")
        if not job_id:
            continue

        job_hash = generate_job_hash(company, str(job_id))
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(f"{company_label}: existing job_hash found, skipping.")
            continue

        apply_link = _normalize_apply_link(job.get("apply_link"))
        title = job.get("title")
        location = job.get("location")
        city = _extract_city(location)

        enrichment = get_enrichment(title, apply_link)

        role = RoleDetail(
            job_hash=job_hash,
            job_id=str(job_id),
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=enrichment["category"],
            city=city,
            state=None,
            country="India",
            workplace_type=None,
            apply_link=apply_link,
            skills=enrichment["skills"],
            min_yoe=enrichment["min_yoe"],
            max_yoe=enrichment["max_yoe"],
            created_at=today_str,
            updated_at=None,
        )

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
        if saved_count:
            total_saved += saved_count
            checker.record(job_hash)
        if stop_fetch:
            print(f"{company_label}: existing job_hash found, stopping further fetch.")
            break

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

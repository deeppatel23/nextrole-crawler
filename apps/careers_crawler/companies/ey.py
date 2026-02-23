"""
https://careers.ey.com/ey/search/?q=&sortColumn=referencedate&sortDirection=desc&optionsFacetsDD_country=IN&startrow=0
startrow = 0, 25, 50...

Sample Response which we are concerned with:

<div class="searchResultsShell">
    <table id="searchresults" class="searchResults full table table-striped table-hover" cellpadding="0" cellspacing="0" aria-label="Search results for India. Page 1 of 108, Results 1 to 25 of 2684">
        <thead>
        </thead>
        <tbody>

                <tr class="data-row">
                    <td class="colTitle" headers="hdrTitle">
                        <span class="jobTitle hidden-phone">
                            <a href="/ey/job/Mumbai-Associate-SaT-CHS-SaT-TCF-M&amp;A-Advisory-Mumbai-MH-400028/1293039001/" class="jobTitle-link">Associate - SaT - CHS - SaT - TCF - M&amp;A Advisory - Mumbai</a>
                        </span>
                        <div class="jobdetail-phone visible-phone">
                                        <span class="jobTitle visible-phone">
                                            <a class="jobTitle-link" href="/ey/job/Mumbai-Associate-SaT-CHS-SaT-TCF-M&amp;A-Advisory-Mumbai-MH-400028/1293039001/">Associate - SaT - CHS - SaT - TCF - M&amp;A Advisory - Mumbai</a>
                                        </span>
                                        <span class="jobState visible-phone">MH</span>
                        </div>
                    </td>
                <td class="colLocation hidden-phone" headers="hdrLocation">

                <span class="jobLocation">
                    Mumbai, MH, IN, 400028
                    
                </span>
				</td>
				<td class="hidden-phone"></td>
            </tr>
        </tbody>
    </table>
</div>
Sample apply_link = https://careers.ey.com/{href from the job title a tag}


Iterate these Category wise links:
Assurance: https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Assurance&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=
CBS: https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=CBS&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=
Consulting: https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=ConsultingS&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=
Strategy and Transactions = https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Strategy%20and%20Transactions&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=
Tax: https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Tax&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=

Data model:
    job_hash: str = hash of (job_id + company) to ensure deduplication
    
    # identity
    job_id: str = extracted from the job link, e.g. 1293039001
    company: str = EY

    # core job info
    title: str = jobTitle-link text
    role: Optional[str] = None
    category: Optional[str] = None = from the category link we are iterating, e.g. Assurance, CBS, Consulting, Strategy and Transactions, Tax

    # experience
    min_yoe: Optional[int] = None = open the job link and try to extract from the job description, e.g. "2-4 years", "5+ years" etc. We can use regex to extract the numbers and then take the min and max. LLM call is required
    max_yoe: Optional[int] = None = = open the job link and try to extract from the job description, e.g. "2-4 years", "5+ years" etc. We can use regex to extract the numbers and then take the min and max. LLM call is required

    # location
    city: Optional[str] = None = jobLocation text split by comma and take 0th index, e.g. Mumbai
    state: Optional[str] = None = jobLocation text split by comma and take 1st index, e.g. MH
    country: Optional[str] = None = India
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = field(default_factory=list) = open the job link and try to extract skills from the job description. LLM call is required

    # links
    apply_link: Optional[str] = None = https://careers.ey.com/{href from the job title a tag}
    source_type: Optional[str] = None = HTML

    # metadata
    created_at: Optional[str] = None = current date in ISO format
    updated_at: Optional[str] = None


Check for one job at a time from a response[].

The response if the API is already sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then stop saving all the subsequent jobs as well since they will be duplicates.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop).

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""
from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

BASE_URL = "https://careers.ey.com"
PAGE_SIZE = 25
CATEGORY_URLS: Dict[str, str] = {
    # "Assurance": "https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Assurance&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=",
    "CBS": "https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=CBS&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=",
    "Consulting": "https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=ConsultingS&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=",
    "Strategy and Transactions": "https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Strategy%20and%20Transactions&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=",
    "Tax": "https://careers.ey.com/ey/search/?createNewAlert=false&q=&optionsFacetsDD_customfield1=Tax&optionsFacetsDD_country=IN&sortColumn=referencedate&optionsFacetsDD_city=",
}


def _clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_job_id(href: str) -> Optional[str]:
    match = re.search(r"/(\d{6,})/?(?:\?|$)", href)
    if match:
        return match.group(1)
    return None


def _parse_location(raw_location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw_location:
        return None, None
    parts = [part.strip() for part in raw_location.split(",")]
    city = parts[0] if len(parts) > 0 and parts[0] else None
    state = parts[1] if len(parts) > 1 and parts[1] else None
    return city, state


def _fetch_html(url: str, start_row: int) -> str:
    resp = requests.get(
        url,
        params={"startrow": start_row},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text


def _parse_rows(page_html: str) -> List[Dict[str, Optional[str]]]:
    rows: List[Dict[str, Optional[str]]] = []
    row_matches = re.findall(
        r"<tr[^>]*class=['\"]data-row['\"][^>]*>(.*?)</tr>",
        page_html,
        flags=re.I | re.S,
    )

    for row_html in row_matches:
        link_match = re.search(
            r"<a[^>]*class=['\"]jobTitle-link['\"][^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
            row_html,
            flags=re.I | re.S,
        )
        if not link_match:
            continue

        href = html.unescape(link_match.group(1).strip())
        title = _clean_text(link_match.group(2))
        if not href or not title:
            continue

        location_match = re.search(
            r"<span[^>]*class=['\"]jobLocation['\"][^>]*>(.*?)</span>",
            row_html,
            flags=re.I | re.S,
        )
        location_text = _clean_text(location_match.group(1)) if location_match else None

        rows.append(
            {
                "href": href,
                "title": title,
                "location": location_text,
            }
        )

    return rows


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "EY"
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

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0

    print(f"{company_label}: starting category crawl.")
    should_stop_all = False

    for category, category_url in CATEGORY_URLS.items():
        print(f"{company_label}: crawling category={category}")
        start_row = 0

        while not should_stop_all:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                should_stop_all = True
                break

            try:
                page_html = _fetch_html(category_url, start_row)
            except Exception as exc:
                print(
                    f"{company_label}: failed to fetch category={category} startrow={start_row}: {exc}"
                )
                break

            rows = _parse_rows(page_html)
            if not rows:
                print(
                    f"{company_label}: no rows found for category={category} startrow={start_row}, moving on."
                )
                break

            print(
                f"{company_label}: category={category} startrow={start_row} rows={len(rows)}"
            )

            for row in rows:
                if total_saved >= max_saved:
                    print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                    should_stop_all = True
                    break

                href = row.get("href")
                title = row.get("title")
                if not href or not title:
                    continue

                apply_link = urljoin(BASE_URL, href)
                job_id = _extract_job_id(href)
                if not job_id:
                    print(
                        f"{company_label}: unable to extract job_id from href={href}, skipping."
                    )
                    continue

                job_hash = generate_job_hash(company, job_id)
                if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                    print(
                        f"{company_label}: existing job_hash for job_id={job_id}, stopping subsequent jobs."
                    )
                    should_stop_all = True
                    break

                city, state = _parse_location(row.get("location"))
                enrichment = get_enrichment(title, apply_link)

                role = RoleDetail(
                    job_hash=job_hash,
                    job_id=job_id,
                    company=company,
                    source_type=source_type,
                    title=title,
                    role=None,
                    category=enrichment["category"],
                    min_yoe=enrichment["min_yoe"],
                    max_yoe=enrichment["max_yoe"],
                    city=normalize_city(city),
                    state=state,
                    country="India",
                    workplace_type=None,
                    skills=enrichment["skills"],
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
                        f"{company_label}: duplicate detected while saving job_id={job_id}, stopping subsequent jobs."
                    )
                    should_stop_all = True
                    break

            start_row += PAGE_SIZE

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

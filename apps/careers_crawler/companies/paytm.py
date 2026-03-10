"""
Paytm careers via RippleHire candidate APIs.

Landing page:
https://paytm.com/careers/

Jobs listing:
POST https://paytm.ripplehire.com/candidate/candidatejobsearch
form-data:
  - careerSiteUrlParams: JSON string
  - lang: en

Job detail:
GET https://paytm.ripplehire.com/candidate/candidatejobdetail?token=...&jobSeq=...&source=CAREERSITE&lang=en
"""
from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

LIST_API_URL = "https://paytm.ripplehire.com/candidate/candidatejobsearch"
DETAIL_API_URL = "https://paytm.ripplehire.com/candidate/candidatejobdetail"
TOKEN = "Jrn4GUz6HCYtOdlkVCzo"
SOURCE = "CAREERSITE"
PAGE_SIZE = 50


def _clean_html_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = html.unescape(html.unescape(str(raw)))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_yoe(text: str) -> Tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    source = text.lower()

    min_vals: List[int] = []
    max_vals: List[int] = []

    for lo, hi in re.findall(
        r"(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\s*(?:\+)?\s*(?:years|year|yrs|yr)",
        source,
    ):
        lo_i, hi_i = int(lo), int(hi)
        min_vals.append(min(lo_i, hi_i))
        max_vals.append(max(lo_i, hi_i))

    for lo in re.findall(r"(\d{1,2})\s*\+\s*(?:years|year|yrs|yr)", source):
        min_vals.append(int(lo))

    if not min_vals and not max_vals:
        return None, None

    return (min(min_vals) if min_vals else None, max(max_vals) if max_vals else None)


def _extract_skills(text: str) -> List[str]:
    if not text:
        return []
    pool = text.lower()
    candidates = [
        "python",
        "java",
        "javascript",
        "sql",
        "aws",
        "kubernetes",
        "docker",
        "react",
        "node",
        "android",
        "ios",
        "sales",
        "negotiation",
        "communication",
        "excel",
    ]
    out: List[str] = []
    for skill in candidates:
        if skill in pool:
            out.append(skill.upper() if skill in {"aws", "ios", "sql"} else skill.title())
        if len(out) >= 3:
            break
    return out


def _extract_xml_root(xml_text: str, root_tag: str) -> Optional[ET.Element]:
    if not xml_text:
        return None
    match = re.search(rf"<{root_tag}[\s\S]*?</{root_tag}>", xml_text)
    if not match:
        return None
    try:
        return ET.fromstring(match.group(0))
    except ET.ParseError:
        return None


def _fetch_list_page(page: int, page_size: int) -> Optional[ET.Element]:
    params = {
        "page": page,
        "search": "*:*",
        "campaignSeq": "",
        "token": TOKEN,
        "source": SOURCE,
        "pagesize": page_size,
    }
    resp = requests.post(
        LIST_API_URL,
        data={"careerSiteUrlParams": str(params).replace("'", '"'), "lang": "en"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    return _extract_xml_root(resp.text, "JobPageVO")


def _fetch_job_detail(job_seq: str) -> Optional[ET.Element]:
    resp = requests.get(
        DETAIL_API_URL,
        params={"token": TOKEN, "jobSeq": job_seq, "source": SOURCE, "lang": "en"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    return _extract_xml_root(resp.text, "CandidateJobVO")


def _first_text(node: Optional[ET.Element], path: str) -> Optional[str]:
    if node is None:
        return None
    child = node.find(path)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Paytm"
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
    now_iso = today.isoformat()
    total_saved = 0

    print(f"{company_label}: fetching jobs from RippleHire API.")
    page = 0

    while True:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        page_root = _fetch_list_page(page, PAGE_SIZE)
        if page_root is None:
            print(f"{company_label}: failed to parse XML page={page}.")
            break

        rows = page_root.findall("./jobVoList/jobVoList")
        if not rows:
            print(f"{company_label}: no jobs on page={page}, stopping.")
            break

        print(f"{company_label}: page={page} jobs={len(rows)}")

        stop_all = False
        for row in rows:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
                stop_all = True
                break

            job_seq = _first_text(row, "jobSeq")
            title = _first_text(row, "jobTitle")
            if not job_seq or not title:
                continue

            job_hash = generate_job_hash(company, job_seq)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(
                    f"{company_label}: existing job_hash found for job_id={job_seq}, "
                    "stopping subsequent jobs (API assumed sorted)."
                )
                stop_all = True
                break

            detail_root = _fetch_job_detail(job_seq)
            detail_job = detail_root.find("jobVO") if detail_root is not None else None

            raw_desc = _first_text(detail_job, "jobDesc") or ""
            description = _clean_html_text(raw_desc)
            req_exp = _first_text(detail_job, "jobReqExp") or ""
            min_yoe, max_yoe = _extract_yoe(f"{req_exp} {title} {description}")

            city = (
                _first_text(detail_job, "locations")
                or _first_text(detail_job, "jobLocation")
                or _first_text(row, "locations")
                or _first_text(row, "jobLocation")
            )

            apply_link = (
                "https://paytm.ripplehire.com/candidate/"
                f"?token={TOKEN}&source={SOURCE}#apply/job/{job_seq}"
            )

            skills = _extract_skills(description)
            category = match_category(
                title=title,
                page_text=description,
                skills=skills,
            )

            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_seq,
                company=company,
                source_type=source_type,
                title=title,
                role=None,
                category=category,
                min_yoe=min_yoe,
                max_yoe=max_yoe,
                city=normalize_city(city),
                state=None,
                country="India",
                workplace_type=None,
                skills=skills,
                apply_link=apply_link,
                created_at=now_iso,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
                print(f"{company_label}: saved job_id={job_seq}")

            if stop_fetch:
                print(
                    f"{company_label}: duplicate detected while saving job_id={job_seq}, stopping further fetch."
                )
                stop_all = True
                break

        if stop_all:
            break

        if len(rows) < PAGE_SIZE:
            print(f"{company_label}: reached final page at page={page}.")
            break

        page += 1

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved


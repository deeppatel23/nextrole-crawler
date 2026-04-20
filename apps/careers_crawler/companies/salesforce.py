from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.category_enricher import match_category
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles

COMPANY = "Salesforce"
CAREERS_URL = "https://careers.salesforce.com/en/jobs/?search"
SOURCE_TYPE = "HTML"


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_yoe(text: str) -> Tuple[Optional[int], Optional[int]]:
    source = (text or "").lower()
    if not source:
        return None, None

    mins: List[int] = []
    maxs: List[int] = []

    for lo, hi in re.findall(r"(\d{1,2})\s*(?:-|to)\s*(\d{1,2})\s*(?:years|year|yrs|yr)", source):
        lo_i, hi_i = int(lo), int(hi)
        mins.append(min(lo_i, hi_i))
        maxs.append(max(lo_i, hi_i))
    for lo in re.findall(r"(\d{1,2})\s*\+\s*(?:years|year|yrs|yr)", source):
        mins.append(int(lo))

    if not mins and not maxs:
        return None, None
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _extract_skills(text: str) -> List[str]:
    pool = (text or "").lower()
    tokens = [
        "python",
        "java",
        "javascript",
        "sql",
        "aws",
        "salesforce",
        "apex",
        "kubernetes",
        "docker",
        "react",
        "node",
        "analytics",
        "communication",
    ]
    out: List[str] = []
    for token in tokens:
        if token in pool:
            out.append(token.upper() if token in {"aws", "sql"} else token.title())
        if len(out) >= 3:
            break
    return out


def _fetch_html(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    resp.raise_for_status()
    return resp.text


def _extract_job_links(list_html: str) -> List[str]:
    links: List[str] = []
    seen: Set[str] = set()

    card_blocks = re.findall(
        r"<div[^>]*class=[\"'][^\"']*card[^\"']*card-job[^\"']*[\"'][^>]*>(.*?)</div>",
        list_html,
        flags=re.I | re.S,
    )
    for block in card_blocks:
        for href in re.findall(r'href=[\"\']([^\"\']+)[\"\']', block, flags=re.I):
            if "/en/jobs/" not in href:
                continue
            full = urljoin(CAREERS_URL, html.unescape(href))
            if full in seen:
                continue
            seen.add(full)
            links.append(full)

    if links:
        return links

    # Fallback if card extraction misses nested markup.
    for href in re.findall(r'href=[\"\']([^\"\']+/en/jobs/[^\"\']+)[\"\']', list_html, flags=re.I):
        full = urljoin(CAREERS_URL, html.unescape(href))
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
    return links


def _city_from_location_text(text: str) -> Optional[str]:
    clean = _clean_text(text)
    if not clean:
        return None
    parts = [p.strip() for p in re.split(r"\s*[-–—]\s*", clean) if p.strip()]
    if len(parts) > 1 and parts[1]:
        # Example: "Germany - Munich" -> "Munich"
        return parts[1]
    return None


def _extract_cities_from_locations_ul(html_fragment: str) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    loc_ul = re.search(
        r"<ul[^>]*class=[\"'][^\"']*list-inline[^\"']*locations[^\"']*[\"'][^>]*>([\s\S]*?)</ul>",
        html_fragment,
        flags=re.I,
    )
    if not loc_ul:
        return out
    for li in re.findall(r"<li[^>]*>(.*?)</li>", loc_ul.group(1), flags=re.I | re.S):
        city = _city_from_location_text(li)
        if not city:
            continue
        key = city.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(city.strip())
    return out


def _extract_listing_city_by_job_id(list_html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    pattern = (
        r"<div[^>]*class=[\"'][^\"']*card-job-actions[^\"']*js-job[^\"']*[\"'][^>]*"
        r"data-id=[\"']([^\"']+)[\"'][^>]*>([\s\S]*?)"
        r"(?=<div[^>]*class=[\"'][^\"']*card-job-actions[^\"']*js-job|$)"
    )
    for job_id, block in re.findall(pattern, list_html, flags=re.I):
        loc_ul = re.search(
            r"<ul[^>]*class=[\"'][^\"']*list-inline[^\"']*locations[^\"']*[\"'][^>]*>([\s\S]*?)</ul>",
            block,
            flags=re.I,
        )
        if not loc_ul:
            continue
        for li in re.findall(r"<li[^>]*>(.*?)</li>", loc_ul.group(1), flags=re.I | re.S):
            city = _city_from_location_text(li)
            if city:
                out[job_id.strip()] = city
                break
    return out


def _extract_listing_cities_by_job_id(list_html: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    pattern = (
        r"<div[^>]*class=[\"'][^\"']*card-job-actions[^\"']*js-job[^\"']*[\"'][^>]*"
        r"data-id=[\"']([^\"']+)[\"'][^>]*>([\s\S]*?)"
        r"(?=<div[^>]*class=[\"'][^\"']*card-job-actions[^\"']*js-job|$)"
    )
    for job_id, block in re.findall(pattern, list_html, flags=re.I):
        cities = _extract_cities_from_locations_ul(block)
        if cities:
            out[job_id.strip()] = cities
    return out


def _extract_title(detail_html: str) -> str:
    for pattern in [
        r"<h1[^>]*>(.*?)</h1>",
        r'<meta[^>]+property=[\"\']og:title[\"\'][^>]+content=[\"\']([^\"\']+)[\"\']',
        r"<title[^>]*>(.*?)</title>",
    ]:
        match = re.search(pattern, detail_html, flags=re.I | re.S)
        if match:
            title = _clean_text(match.group(1))
            if title:
                return title
    return ""


def _extract_city(detail_html: str, detail_text: str) -> Optional[str]:
    html_location_match = re.search(
        r"<ul[^>]*class=[\"'][^\"']*list-inline[^\"']*locations[^\"']*[\"'][^>]*>[\s\S]*?</ul>",
        detail_html,
        flags=re.I,
    )
    if html_location_match:
        block = html_location_match.group(0)
        li_values = re.findall(r"<li[^>]*>(.*?)</li>", block, flags=re.I | re.S)
        for li in li_values:
            city = _city_from_location_text(li)
            if city:
                return city

    for pattern in [
        r"\bLocation(?:s)?\s*[:\-]\s*([A-Za-z .\-]+)",
        r"\bPrimary Location\s*[:\-]\s*([A-Za-z .\-]+)",
    ]:
        match = re.search(pattern, detail_text, flags=re.I)
        if not match:
            continue
        raw = (match.group(1) or "").strip(" .,-")
        if not raw:
            continue
        return raw.split(",")[0].strip() or None
    return None


def _extract_cities(detail_html: str, detail_text: str) -> List[str]:
    cities = _extract_cities_from_locations_ul(detail_html)
    if cities:
        return cities
    single = _extract_city(detail_html, detail_text)
    return [single] if single else []


def _city_suffix(city: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", city.strip().lower()).strip("_")


def _extract_job_id(job_url: str) -> str:
    match = re.search(r"/en/jobs/([^/]+)/", job_url)
    if match:
        return match.group(1)
    return job_url.rstrip("/").split("/")[-1]


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = source_cfg.get("company") or COMPANY
    source_type = (source_cfg.get("source_type") or SOURCE_TYPE).upper()
    careers_url = source_cfg.get("careers_url") or CAREERS_URL
    max_saved = int(source_cfg.get("max_saved_jobs", 9999))
    last_saved = source_cfg.get("last_saved")

    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(f"{company}: last_saved={last_saved} is >= today={today.isoformat()}, skipping.")
                return 0
        except ValueError:
            print(f"{company}: invalid last_saved={last_saved}, continuing.")

    checker = MongoJobHashChecker()
    now_iso = today.isoformat()
    total_saved = 0

    print(f"{company}: fetching careers page {careers_url}")
    try:
        list_html = _fetch_html(careers_url)
    except Exception as exc:
        print(f"{company}: failed to fetch careers_url: {exc}")
        return 0

    job_links = _extract_job_links(list_html)
    listing_city_by_job_id = _extract_listing_city_by_job_id(list_html)
    listing_cities_by_job_id = _extract_listing_cities_by_job_id(list_html)
    if not job_links:
        print(f"{company}: no job links found in careers listing.")
        return 0

    for job_url in job_links:
        if total_saved >= max_saved:
            print(f"{company}: reached max_saved_jobs={max_saved}, stopping.")
            break

        job_id = _extract_job_id(job_url)
        try:
            detail_html = _fetch_html(job_url)
        except Exception as exc:
            print(f"{company}: failed to fetch detail job_id={job_id}: {exc}")
            continue

        title = _extract_title(detail_html)
        if not title:
            print(f"{company}: missing title in detail page job_id={job_id}, skipping.")
            continue

        detail_text = _clean_text(detail_html)
        cities = _extract_cities(detail_html, detail_text)
        if not cities:
            fallback_city = listing_city_by_job_id.get(job_id)
            if fallback_city:
                cities = [fallback_city]
        if not cities:
            cities = listing_cities_by_job_id.get(job_id, [])
        if not cities:
            cities = [None]

        skills = _extract_skills(detail_text)
        min_yoe, max_yoe = _extract_yoe(f"{title} {detail_text}")
        category = match_category(
            title=title,
            department=None,
            skills=skills,
            page_text=detail_text,
            category_hint=None,
        )

        for city_value in cities:
            final_city = normalize_city(city_value)
            derived_job_id = job_id
            if final_city and len(cities) > 1:
                suffix = _city_suffix(final_city)
                if suffix:
                    derived_job_id = f"{job_id}__{suffix}"

            job_hash = generate_job_hash(COMPANY, derived_job_id)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(f"{company}: existing job_hash found for job_id={derived_job_id}, skipping.")
                continue

            role = RoleDetail(
                job_hash=job_hash,
                job_id=derived_job_id,
                company=COMPANY,
                source_type=source_type,
                title=title,
                role=None,
                category=category,
                min_yoe=min_yoe,
                max_yoe=max_yoe,
                city=final_city,
                state=None,
                country=None,
                workplace_type=None,
                skills=skills,
                apply_link=job_url,
                created_at=now_iso,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
                print(f"{company}: saved job_id={derived_job_id}")
            if stop_fetch:
                print(f"{company}: duplicate detected while saving job_id={derived_job_id}, stopping.")
                break

    print(f"{company}: total saved {total_saved} jobs.")
    return total_saved

"""
Media.net careers parser.
HTML-based: crawls category pages + pagination, extracts job links by category URL prefix.
Ordering: uses page/link discovery order (no guaranteed date-descending sort).
De-dupe: if job_hash exists (mongo), append_roles returns stop_fetch and the crawler stops early.
Skips any link that matches the category URL itself.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests

from config.config import OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.html_utils import fetch_visible_text
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment


CATEGORY_URLS = [
    "https://careers.media.net/data-science-analytics/",
    "https://careers.media.net/engineering/",
    "https://careers.media.net/design/",
    "https://careers.media.net/product-management-operations/",
    "https://careers.media.net/business-development/",
    "https://careers.media.net/system-operations/",
    "https://careers.media.net/product-marketing/",
    "https://careers.media.net/business-operations/",
    "https://careers.media.net/information-technology/",
    "https://careers.media.net/finance-accounting/",
    "https://careers.media.net/human-resource/",
    "https://careers.media.net/legal-and-compliance/",
]

REQUEST_TIMEOUT = 30


def _fetch_html(url: str) -> Optional[str]:
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def _normalize_category(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[0] if path else ""


def _extract_job_links(html: str, category_url: str) -> List[str]:
    if not html:
        return []
    category_slug = _normalize_category(category_url)
    if not category_slug:
        return []

    pattern = re.compile(r"https?://[^\s\"'<>]+")
    candidates = pattern.findall(html)

    prefix = f"https://careers.media.net/{category_slug}/"
    links: List[str] = []
    seen: set[str] = set()
    for raw in candidates:
        link = raw.rstrip("),.;:\"]}")
        if not link.startswith(prefix):
            continue
        if link.startswith(f"{prefix}page/"):
            continue
        if link in seen:
            continue
        seen.add(link)
        links.append(link)
    return links


def _extract_pagination_links(html: str, category_url: str) -> List[str]:
    if not html:
        return []
    base = category_url.rstrip("/") + "/"
    pattern = re.compile(r"https?://[^\s\"'<>]+")
    candidates = pattern.findall(html)
    links: List[str] = []
    seen: set[str] = set()
    for raw in candidates:
        link = raw.rstrip("),.;:\"]}")
        if not link.startswith(base):
            continue
        if "/page/" not in link:
            continue
        if link in seen:
            continue
        seen.add(link)
        links.append(link)
    return links


def _extract_title(html: str) -> Optional[str]:
    if not html:
        return None
    match = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    if not match:
        return None
    return _clean_title(match.group(1))


def _clean_title(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    title = re.sub(r"\s+", " ", value).strip()
    if not title:
        return None
    title = re.sub(r"\s*\|\s*careers@media\.net", "", title, flags=re.I).strip()
    title = re.sub(r"\s+", " ", title).strip()
    return title or None


def _extract_job_id(job_url: str) -> str:
    path = urlparse(job_url).path.strip("/")
    if path:
        return path
    return job_url


def _build_role(
    job_url: str,
    category: str,
    company: str,
    source_type: Optional[str],
) -> Optional[RoleDetail]:
    if job_url in CATEGORY_URLS:
        return None
    html = _fetch_html(job_url)
    title = _extract_title(html or "")
    page_text = html if html else ""
    enrichment_text = fetch_visible_text(job_url) or page_text
    enrichment = get_enrichment(
        title,
        job_url,
        enrichment_text,
    )

    job_id = _extract_job_id(job_url)
    if not job_id or not title:
        return None

    return RoleDetail(
        job_hash=generate_job_hash(company, job_id),
        job_id=job_id,
        company=company,
        source_type=source_type,
        title=title,
        category=enrichment["category"],
        apply_link=job_url,
        skills=enrichment["skills"],
        min_yoe=enrichment["min_yoe"],
        max_yoe=enrichment["max_yoe"],
        created_at=datetime.utcnow().date().isoformat(),
    )


def _iter_category_pages(category_url: str) -> List[str]:
    first_html = _fetch_html(category_url)
    if not first_html:
        return []

    pages = [category_url]
    for link in _extract_pagination_links(first_html, category_url):
        pages.append(link)

    normalized = []
    seen = set()
    for page in pages:
        normalized_page = urljoin(category_url, page)
        if normalized_page in seen:
            continue
        seen.add(normalized_page)
        normalized.append(normalized_page)
    return normalized


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
                    f"Media.net: last_saved={last_saved} is >= today={today.isoformat()}, skipping."
                )
                return 0
        except ValueError:
            print(f"Media.net: invalid last_saved={last_saved}, continuing.")

    total_saved = 0

    for category_url in CATEGORY_URLS:
        category = _normalize_category(category_url)
        pages = _iter_category_pages(category_url)
        if not pages:
            print(f"Media.net: no pages found for category {category}")
            continue

        job_links: List[str] = []
        for page_url in pages:
            html = _fetch_html(page_url)
            if not html:
                continue
            job_links.extend(_extract_job_links(html, category_url))

        if not job_links:
            print(f"Media.net: no openings found for category {category}")
            continue

        unique_links: List[str] = []
        seen: set[str] = set()
        for link in job_links:
            if link in seen:
                continue
            seen.add(link)
            unique_links.append(link)

        roles: List[RoleDetail] = []
        for job_url in unique_links:
            if total_saved >= max_saved:
                print(f"Media.net: reached max_saved_jobs={max_saved}, stopping.")
                return total_saved
            role = _build_role(job_url, category, company, source_type)
            if role:
                roles.append(role)

        if not roles:
            continue

        remaining = max_saved - total_saved
        if remaining <= 0:
            print(f"Media.net: reached max_saved_jobs={max_saved}, stopping.")
            return total_saved
        if len(roles) > remaining:
            roles = roles[:remaining]
            print(f"Media.net: reached max_saved_jobs={max_saved}, stopping after this batch.")

        saved_count, stop_fetch = append_roles(OUTPUT_FILE, roles)
        total_saved += saved_count
        print(
            f"Media.net: category {category} saved {saved_count}/{len(roles)} jobs"
        )
        if stop_fetch:
            print("Media.net: existing job_hash found, stopping further fetch.")
            return total_saved
        if total_saved >= max_saved:
            print(f"Media.net: reached max_saved_jobs={max_saved}, stopping.")
            return total_saved

    return total_saved

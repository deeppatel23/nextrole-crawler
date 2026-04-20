"""
Request URL
https://careers.nutanix.com/en/jobs/?search=&country=India&pagesize=20
Request Method
GET

Sample HTML Response
<div class="card card-job job-hover" data-id="31439">

    <div class="inner">

            
<div class="card-job-actions js-job" data-id="31439" data-jobtitle="Principal Talent Advisor">
    <button type="button" class="btn-add-job " aria-label="Save Principal Talent Advisor"  title="Save">
        <svg class="icon-sprite" aria-hidden="true"><use xlink:href="/images/sprite.svg?v=2xemnMo5hMe3n5g52s_nDZKwPbd1v_BaGpQWb_VVaAg#heart"></use></svg>
        <span class="visually-hidden">Save</span>
    </button>

    <button type="button" class="btn-remove-job d-none" aria-label="Remove Principal Talent Advisor" hidden title="Remove">
        <svg class="icon-sprite" aria-hidden="true"><use xlink:href="/images/sprite.svg?v=2xemnMo5hMe3n5g52s_nDZKwPbd1v_BaGpQWb_VVaAg#heart"></use></svg>
        <span class="visually-hidden">Saved</span>
    </button>
</div>

        <p class="reference-number">31439</p>
        <h2 class="card-title"><a href="/en/jobs/31439/principal-talent-advisor/" class="stretched-link js-view-job">Principal Talent Advisor</a></h2>


        <div class="job-meta-container">
            <p class="job-meta job-meta-location" lang="en-US">
                Bangalore, India
            </p>
            <p class="job-meta job-meta-team">
                Human Resources
            </p>
        </div>
        <div class="faux-button btn-arrow-right">
            <svg class="icon-sprite" aria-hidden="true"><use xlink:href="/images/sprite.svg?v=2xemnMo5hMe3n5g52s_nDZKwPbd1v_BaGpQWb_VVaAg#arrow-right"></use></svg>
        </div>
    </div>

</div>

Job Detail URL: https://careers.nutanix.com/en/jobs/31439/principal-talent-advisor/
Sample HTML Response for Job Detail Page
class="container job-detail" id="js-job-detail" has all the details


Mapping with role_details
    job_hash: str = hash of (data-id from div with class "card card-job job-hover") + Nutanix

    jobDescription =  id="job-desc" from job detail response
    
    # identity
    job_id: str = data-id from div with class "card card-job job-hover"
    company: str = Uber

    # core job info
    title: str = data-jobtitle
    role: Optional[str] = None
    category: Optional[str] = get_enrichment with extra_text = fetch title + jobDescription and get the caegory

    # experience
    min_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 
    max_yoe: Optional[int] = None = get_enrichment with extra_text = fetch title + jobDescription 

    # location
    city: Optional[str] = None = job-meta job-meta-location split by "," remove space and take 0th part (only city)
    state: Optional[str] = None
    country: Optional[str] = None = job-meta job-meta-location split by "," remove space and take 1st part
    workplace_type: Optional[str] = None  # onsite / remote / hybrid

    # content
    skills: List[str] = get_enrichment with extra_text = fetch title + jobDescription 

    # links
    apply_link: Optional[str] = None = "https://www.uber.com/global/en/careers/list/" + id
    source_type: Optional[str] = API

    # metadata
    created_at: Optional[str] = None = datetime.now().isoformat().date()
    updated_at: Optional[str] = None

You will not get all the jobs response in one API call. 
Change page_size in each iteration.
Loop until we have iterated through all the jobs or reached max_saved_jobs, total parameter is there in response and size parameter is the query parameter we are sending in the API request.
Stop when no job found on the page.

The response of the API is not sorted, so if we are saving it to mongo and a job exists, (if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):) then skip that particular jobs and go to next job

Add this comapny details in careers_sources.yaml with last_saved and max_saved_jobs parameters.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop). Make max_saved_jobs check at siteNumber level since each siteNumber has a different set of job openings.

Try to add print statement wherever necessary.

"""

from __future__ import annotations

import html
import os
import re
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

from config.config import DEBUG_MODE
from config.config import OUTPUT_DESTINATION, OUTPUT_FILE
from models.role_detail import RoleDetail
from utils.browser_cookie_fetcher import get_cookie_header_via_playwright
from utils.extract_utils import normalize_city
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

COMPANY = "Nutanix"
LIST_URL = "https://careers.nutanix.com/en/jobs/"
SITE_BASE_URL = "https://careers.nutanix.com"
DEFAULT_COUNTRY = "India"
DEFAULT_PAGE_SIZE = 20
DEFAULT_MAX_PAGES = 50
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)
DEFAULT_PLAYWRIGHT_HEADLESS = True
DEFAULT_PLAYWRIGHT_USER_DATA_DIR = "apps/careers_crawler/.tmp/playwright_nutanix"


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _resolve_max_saved(source_cfg: Dict[str, Any], site_key: str) -> int:
    raw = source_cfg.get("max_saved_jobs", 9999)
    if isinstance(raw, dict):
        value = raw.get(site_key, raw.get("default", 9999))
    else:
        value = raw
    try:
        parsed = int(value)
        return parsed if parsed >= 0 else 0
    except (TypeError, ValueError):
        return 9999


def _build_headers(source_cfg: Dict[str, Any], *, referer: Optional[str]) -> Dict[str, str]:
    # Note: several anti-bot systems validate header combinations. We switch a few values
    # depending on whether this is an initial navigation (no referer) or an in-site navigation.
    fetch_site = "none" if not referer else "same-origin"
    headers: Dict[str, str] = {
        "user-agent": str(source_cfg.get("user_agent") or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": str(source_cfg.get("accept_language") or "en-US,en;q=0.9").strip()
        or "en-US,en;q=0.9",
        "accept-encoding": "gzip, deflate",
        "connection": "keep-alive",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": fetch_site,
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
    }
    if referer:
        headers["referer"] = referer
    cookie = source_cfg.get("cookie")
    if isinstance(cookie, str) and cookie.strip():
        headers["cookie"] = cookie.strip()
    return headers


def _fetch_html(session: requests.Session, url: str, source_cfg: Dict[str, Any], *, referer: Optional[str]) -> str:
    headers = _build_headers(source_cfg, referer=referer)
    use_browser_cookie = _as_bool(
        source_cfg.get("use_browser_cookie", os.getenv("CAREERS_USE_BROWSER_COOKIE")),
        default=False,
    )
    playwright_headless = _as_bool(
        source_cfg.get("playwright_headless", os.getenv("CAREERS_PLAYWRIGHT_HEADLESS")),
        default=DEFAULT_PLAYWRIGHT_HEADLESS,
    )
    playwright_user_data_dir = str(
        source_cfg.get("playwright_user_data_dir", os.getenv("CAREERS_PLAYWRIGHT_USER_DATA_DIR") or DEFAULT_PLAYWRIGHT_USER_DATA_DIR)
    ).strip() or DEFAULT_PLAYWRIGHT_USER_DATA_DIR

    # If enabled, bootstrap a Cloudflare clearance cookie via Playwright once per run.
    if use_browser_cookie and not (isinstance(source_cfg.get("cookie"), str) and source_cfg.get("cookie", "").strip()):
        try:
            cookie_header = get_cookie_header_via_playwright(
                LIST_URL,
                user_agent=headers.get("user-agent") or DEFAULT_USER_AGENT,
                headless=playwright_headless,
                user_data_dir=playwright_user_data_dir,
            )
            source_cfg["cookie"] = cookie_header
            headers = _build_headers(source_cfg, referer=referer)
            print("Nutanix: fetched browser cookies via Playwright.")
        except Exception as exc:
            print(f"Nutanix: failed to fetch browser cookies via Playwright: {exc}")

    # Go straight to curl to avoid Cloudflare TLS fingerprint blocking on requests/urllib3
    try:
        html_text = _fetch_html_via_curl(url, headers)
        if use_browser_cookie and _looks_like_bot_challenge(html_text):
            # Cookie might be stale/insufficient; try a fresh browser cookie once.
            cookie_header = get_cookie_header_via_playwright(
                LIST_URL,
                user_agent=headers.get("user-agent") or DEFAULT_USER_AGENT,
                headless=playwright_headless,
                user_data_dir=playwright_user_data_dir,
            )
            source_cfg["cookie"] = cookie_header
            headers = _build_headers(source_cfg, referer=referer)
            html_text = _fetch_html_via_curl(url, headers)
        return html_text
    except Exception as curl_exc:
        print(f"curl failed ({curl_exc}), falling back to requests...")

    resp = session.get(url, headers=headers, timeout=30)
    if resp.status_code == 403:
        server = resp.headers.get("server")
        ray = resp.headers.get("cf-ray")
        raise RuntimeError(f"403 Forbidden (server={server} cf-ray={ray}) for url: {url}")
    resp.raise_for_status()
    return resp.text


def _fetch_html_via_curl(url: str, headers: Dict[str, str]) -> str:
    cmd: List[str] = [
        "curl", "-L", "-sS", "--max-time", "30",
        "--http2",                        # Use HTTP/2 (Cloudflare expects it)
        "--tlsv1.2",                       # Enforce TLS 1.2+
        "--compressed",                    # Accept gzip/br (real browsers do)
        "--location",                      # Follow redirects
    ]
    for key, value in headers.items():
        if value is None:
            continue
        cmd.extend(["-H", f"{key}: {value}"])
    cmd.append(url)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"curl failed (code={result.returncode}): {stderr or 'unknown error'}")
    if not result.stdout:
        raise RuntimeError("curl returned empty response")
    return result.stdout


def _prime_session(session: requests.Session, source_cfg: Dict[str, Any]) -> None:
    # Some deployments require a landing-page hit first to set anti-bot cookies.
    try:
        session.get(
            LIST_URL,
            headers=_build_headers(source_cfg, referer=None),
            timeout=30,
        )
    except Exception:
        return


def _looks_like_bot_challenge(page_html: str) -> bool:
    if not page_html:
        return False
    text = page_html.lower()
    markers = (
        "cloudflare",
        "cf-ray",
        "attention required",
        "just a moment",
        "checking your browser",
        "verify you are human",
        "captcha",
        "/cdn-cgi/",
    )
    return any(marker in text for marker in markers)


_ACTIONS_RE = re.compile(
    r'<div[^>]+class="[^"]*\\bcard-job-actions\\b[^"]*\\bjs-job\\b[^"]*"'
    r'[^>]*\\bdata-id="(?P<job_id>[^"]+)"[^>]*\\bdata-jobtitle="(?P<title>[^"]+)"',
    flags=re.I,
)

_CARD_START_RE = re.compile(
    r'<div[^>]+class="[^"]*\\bcard-job\\b[^"]*\\bjob-hover\\b[^"]*"[^>]*\\bdata-id="(?P<job_id>[^"]+)"[^>]*>',
    flags=re.I,
)


def _extract_listings(list_html: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: Set[str] = set()
    if not list_html:
        return out

    # Primary extraction: "card-job-actions js-job" carries id + title.
    for match in _ACTIONS_RE.finditer(list_html):
        job_id = (match.group("job_id") or "").strip()
        if not job_id or job_id in seen:
            continue

        window = list_html[match.start() : match.start() + 12000]
        title = _clean_text(match.group("title") or "")

        href_match = re.search(
            rf'href="(?P<href>/en/jobs/{re.escape(job_id)}/[^"]+/)"',
            window,
            flags=re.I,
        )
        if not href_match:
            href_match = re.search(r'href="(?P<href>/en/jobs/[^"]+/[^"]+/)"', window, flags=re.I)

        location_match = re.search(
            r'class="job-meta\s+job-meta-location"[^>]*>(?P<loc>[\s\S]*?)</p>',
            window,
            flags=re.I,
        )

        href = html.unescape(href_match.group("href")).strip() if href_match else ""
        location = _clean_text(location_match.group("loc")) if location_match else ""
        if not title or not href:
            continue

        seen.add(job_id)
        out.append({"job_id": job_id, "title": title, "href": href, "location": location})

    if out:
        return out

    for match in _CARD_START_RE.finditer(list_html):
        job_id = (match.group("job_id") or "").strip()
        if not job_id or job_id in seen:
            continue

        window = list_html[match.start() : match.start() + 7000]
        title_match = re.search(r'data-jobtitle="(?P<title>[^"]+)"', window, flags=re.I)
        if not title_match:
            title_match = re.search(
                r'<h2[^>]*class="card-title"[^>]*>[\s\S]*?<a[^>]*>(?P<title>[\s\S]*?)</a>',
                window,
                flags=re.I,
            )
        href_match = re.search(
            r'<a[^>]+href="(?P<href>/en/jobs/[^"]+/[^"]+/)"[^>]*class="[^"]*js-view-job[^"]*"',
            window,
            flags=re.I,
        )
        if not href_match:
            href_match = re.search(rf'<a[^>]+href="(?P<href>/en/jobs/{re.escape(job_id)}/[^"]+/)"', window, flags=re.I)

        location_match = re.search(
            r'class="job-meta\s+job-meta-location"[^>]*>(?P<loc>[\s\S]*?)</p>',
            window,
            flags=re.I,
        )

        title = _clean_text(title_match.group("title")) if title_match else ""
        href = html.unescape(href_match.group("href")).strip() if href_match else ""
        location = _clean_text(location_match.group("loc")) if location_match else ""

        if not title or not href:
            continue

        seen.add(job_id)
        out.append({"job_id": job_id, "title": title, "href": href, "location": location})

    if out:
        return out

    # Fallback: extract any job links if the markup changes.
    for href, title in re.findall(
        r'<a[^>]+href="(/en/jobs/[^"]+/[^"]+/)"[^>]*>([^<]+)</a>',
        list_html,
        flags=re.I,
    ):
        job_id = (href.split("/en/jobs/")[-1].split("/", 1)[0] or "").strip()
        if not job_id or job_id in seen:
            continue
        t = _clean_text(title)
        if not t:
            continue
        seen.add(job_id)
        out.append({"job_id": job_id, "title": t, "href": html.unescape(href).strip(), "location": ""})
    return out


def _split_location(raw: str) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None
    text = _clean_text(raw)
    if not text:
        return None, None

    parts = [p.strip() for p in text.split(",") if p.strip()]
    city = parts[0] if parts else None
    country = parts[1] if len(parts) > 1 else None

    # Handle multi-city strings like "Bangalore/Pune" -> "Bangalore"
    if city and "/" in city:
        city = city.split("/", 1)[0].strip() or city
    return city, country


def _extract_job_description(detail_html: str) -> str:
    if not detail_html:
        return ""

    match = re.search(r'id="job-desc"[^>]*>(?P<body>[\s\S]*?)</', detail_html, flags=re.I)
    if match:
        return _clean_text(match.group("body"))

    match = re.search(
        r'id="js-job-detail"[^>]*class="[^"]*job-detail[^"]*"[^>]*>(?P<body>[\s\S]*?)</div>',
        detail_html,
        flags=re.I,
    )
    if match:
        return _clean_text(match.group("body"))

    return ""


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company_label = source_cfg.get("company") or COMPANY
    source_type = (source_cfg.get("source_type") or "HTML").upper()
    site_key = str(source_cfg.get("site_number") or "default")
    max_saved = _resolve_max_saved(source_cfg, site_key)

    last_saved = source_cfg.get("last_saved")
    today = datetime.utcnow().date()
    if last_saved:
        try:
            last_saved_date = datetime.strptime(last_saved, "%Y-%m-%d").date()
            if today <= last_saved_date:
                print(f"{company_label}: last_saved={last_saved} is >= today={today.isoformat()}, skipping.")
                return 0
        except ValueError:
            print(f"{company_label}: invalid last_saved={last_saved}, continuing.")

    country = str(source_cfg.get("country") or DEFAULT_COUNTRY).strip() or DEFAULT_COUNTRY
    page_size = int(source_cfg.get("page_size") or source_cfg.get("pagesize") or DEFAULT_PAGE_SIZE)
    if page_size <= 0:
        page_size = DEFAULT_PAGE_SIZE

    max_pages = int(source_cfg.get("max_pages") or DEFAULT_MAX_PAGES)
    if max_pages <= 0:
        max_pages = DEFAULT_MAX_PAGES

    checker = MongoJobHashChecker()
    created_at = today.isoformat()
    total_saved = 0
    session = requests.Session()
    _prime_session(session, source_cfg)

    print(f"{company_label}: starting crawl country={country} page_size={page_size}")

    for page in range(1, max_pages + 1):
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping.")
            break

        params = {"search": "", "country": country, "pagesize": str(page_size)}
        if page > 1:
            params["page"] = str(page)

        url = f"{LIST_URL}?search={params['search']}&country={country}&pagesize={page_size}"
        if page > 1:
            url += f"&page={page}"

        print(f"{company_label}: fetching list page={page}")
        try:
            list_html = _fetch_html(session, url, source_cfg, referer=None if page == 1 else LIST_URL)
        except Exception as exc:
            print(f"{company_label}: failed to fetch list page={page}: {exc}")
            print(
                f"{company_label}: if this is a 403 but works in browser/Postman, it's usually Cloudflare bot protection "
                f"(TLS fingerprint + clearance cookies). The crawler already retries via `curl` automatically; "
                f"if it still fails, add a fresh browser `cookie:` value under Nutanix in careers_sources.yaml and retry."
            )
            break

        if _looks_like_bot_challenge(list_html):
            if DEBUG_MODE:
                debug_path = f"apps/careers_crawler/output/nutanix_list_page_{page}.html"
                try:
                    with open(debug_path, "w", encoding="utf-8") as fh:
                        fh.write(list_html)
                    print(f"{company_label}: DEBUG_MODE=true saved challenge HTML to {debug_path}")
                except Exception:
                    pass
            print(
                f"{company_label}: received a Cloudflare challenge page (HTTP 200). "
                f"Set `use_browser_cookie: true` (Playwright) or provide `cookie:` in careers_sources.yaml and retry."
            )
            break

        listings = _extract_listings(list_html)
        if not listings:
            if DEBUG_MODE:
                debug_path = f"apps/careers_crawler/output/nutanix_list_page_{page}.html"
                try:
                    with open(debug_path, "w", encoding="utf-8") as fh:
                        fh.write(list_html)
                    print(f"{company_label}: DEBUG_MODE=true saved list HTML to {debug_path}")
                except Exception:
                    pass
            print(f"{company_label}: page={page} returned no jobs, stopping.")
            break

        for listing in listings:
            if total_saved >= max_saved:
                print(f"{company_label}: reached max_saved_jobs={max_saved} for site={site_key}, stopping.")
                break

            job_id = listing.get("job_id") or ""
            title = listing.get("title") or ""
            href = listing.get("href") or ""
            if not job_id or not title or not href:
                continue

            job_id_str = str(job_id).strip()
            job_hash = generate_job_hash(COMPANY, job_id_str)
            if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
                print(f"{company_label}: duplicate job_hash job_id={job_id_str}, skipping.")
                continue

            apply_link = urljoin(SITE_BASE_URL, href)

            print(f"{company_label}: fetching detail job_id={job_id_str}")
            try:
                detail_html = _fetch_html(session, apply_link, source_cfg, referer=url)
            except Exception as exc:
                print(f"{company_label}: failed to fetch job detail job_id={job_id_str}: {exc}")
                continue

            job_description = _extract_job_description(detail_html)
            extra_text = f"{title}\n\n{job_description}".strip()
            enrichment = get_enrichment(title=title, apply_link=apply_link, extra_text=extra_text)

            city_raw, country_raw = _split_location(listing.get("location") or "")
            role = RoleDetail(
                job_hash=job_hash,
                job_id=job_id_str,
                company=COMPANY,
                source_type=source_type,
                title=title,
                role=None,
                category=enrichment.get("category"),
                min_yoe=enrichment.get("min_yoe"),
                max_yoe=enrichment.get("max_yoe"),
                city=normalize_city(city_raw),
                state=None,
                country=country_raw,
                workplace_type=None,
                skills=enrichment.get("skills") or [],
                apply_link=apply_link,
                created_at=created_at,
                updated_at=None,
            )

            saved_count, stop_fetch = append_roles(OUTPUT_FILE, [role])
            if saved_count:
                total_saved += saved_count
                checker.record(job_hash)
                print(f"{company_label}: saved job_id={job_id_str}")
            elif stop_fetch:
                print(f"{company_label}: job already exists job_id={job_id_str}, skipping.")

    print(f"{company_label}: total saved {total_saved} jobs.")
    return total_saved

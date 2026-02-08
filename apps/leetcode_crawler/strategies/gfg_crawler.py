import argparse
import re
import sys
import time
from hashlib import sha256
from html import unescape
from pathlib import Path
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from processor.extractor import InterviewExtractor
from utils.llm_client import LLMClient
from utils.output_writer import append_interviews
from utils.rules import is_interview_post

ORG_NAMES = [
    "Accenture",
    "Adobe",
    "Amazon",
    "Amdocs",
    "Apple",
    "Atlassian",
    "BNY-Mellon",
    "Cisco",
    "Cognizant",
    "DE-Shaw",
    "Deloitte",
    "Directi",
    "EY",
    "Facebook",
    "Flipkart",
    "Goldman-Sachs",
    "Google",
    "HCL",
    "IBM",
    "Infosys",
    "Intuit",
    "Jenkins",
    "jpmorgan-chase-and-co",
    "Juspay",
    "KPMG",
    "Microsoft",
    "Morgan-Stanley",
    "Netflix",
    "Nvidia",
    "OLA-Cabs",
    "Oracle",
    "Paytm",
    "PwC",
    "Qualcomm",
    "Salesforce",
    "Samsung-electronics",
    "SAP-Labs",
    "Siemens",
    "Synopsys",
    "TCS",
    "Tiger-Analytics",
    "Virtusa",
    "Visa",
    "VMWare",
    "Walmart",
    "Wipro",
    "Zoho-corporation"
]

GFG_API_BASE = "https://apiwrite.geeksforgeeks.org/organization/slug"
GFG_ARTICLE_BASE = "https://www.geeksforgeeks.org/interview-experiences"
REQUEST_TIMEOUT = 30


def build_org_slug(name: str) -> str:
    value = name.lower()
    value = re.sub(r"\(.*?\)", "", value)
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value


def org_slug_candidates(name: str) -> list[str]:
    slug = build_org_slug(name)
    candidates = []
    if slug:
        candidates.append(slug)
    raw = name.strip()
    if raw:
        candidates.append(quote(raw))
    # De-duplicate while preserving order
    seen = set()
    ordered = []
    for item in candidates:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def extract_links(text: str) -> list[str]:
    pattern = r"https?://[^\s\)\]\}<>\"']+"
    return sorted({match.rstrip(".,;:") for match in re.findall(pattern, text)})


def build_interview_hash(source_url: str, slug: str | None) -> str:
    base = f"{source_url}:{slug or ''}"
    return sha256(base.encode("utf-8")).hexdigest()


def fetch_org_articles(
    session: requests.Session,
    org_name: str,
) -> tuple[list[dict], str | None]:
    for slug in org_slug_candidates(org_name):
        print(f"→ Fetching org articles: org={org_name}, slug={slug}")
        url = f"{GFG_API_BASE}/{slug}/articles"
        all_results: list[dict] = []
        seen_article_slugs: set[str] = set()
        page = 1
        total_expected: int | None = None

        while True:
            page_url = f"{url}?page={page}"
            try:
                resp = session.get(page_url, timeout=REQUEST_TIMEOUT)
                if resp.status_code != 200:
                    print(
                        f"  ⚠ Org API HTTP {resp.status_code} for slug={slug}, page={page}"
                    )
                    break
                data = resp.json()
            except Exception:
                print(f"  ⚠ Org API failed for slug={slug}, page={page}")
                break

            results = (
                data.get("results")
                or data.get("data")
                or data.get("articles")
                or []
            )
            if total_expected is None:
                total_expected = (
                    data.get("count")
                    or data.get("total")
                    or data.get("total_count")
                )

            if not isinstance(results, list) or not results:
                break

            added = 0
            for item in results:
                article_slug = (
                    item.get("article_slug")
                    or item.get("slug")
                    or item.get("articleSlug")
                )
                if not article_slug or article_slug in seen_article_slugs:
                    continue
                seen_article_slugs.add(article_slug)
                all_results.append(item)
                added += 1

            if added == 0:
                break

            if total_expected is not None and len(all_results) >= total_expected:
                break

            page += 1

        if all_results:
            if total_expected is not None:
                print(
                    f"  ✔ Org API returned {len(all_results)}/{total_expected} articles for slug={slug}"
                )
            else:
                print(f"  ✔ Org API returned {len(all_results)} articles for slug={slug}")
            return all_results, slug

    return [], None


def extract_title(html: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return unescape(title) if title else None


def html_to_text(html: str) -> str:
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def fetch_article_html(
    session: requests.Session,
    article_slug: str,
) -> str | None:
    url = f"{GFG_ARTICLE_BASE}/{article_slug}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            print(f"  ⚠ Article HTTP {resp.status_code} for {article_slug}")
            return None
        return resp.text
    except Exception:
        print(f"  ⚠ Article fetch failed for {article_slug}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl GeeksforGeeks interview experience articles by organization."
    )
    parser.add_argument(
        "--orgs",
        type=str,
        default=",".join(ORG_NAMES),
        help="Comma-separated organization names to crawl.",
    )
    parser.add_argument(
        "--max-per-org",
        type=int,
        default=12,
        help="Maximum articles to process per organization.",
    )
    parser.add_argument(
        "--max-total",
        type=int,
        default=None,
        help="Maximum articles to process total.",
    )
    args = parser.parse_args()

    orgs = [o.strip() for o in args.orgs.split(",") if o.strip()]
    per_org_limit = min(args.max_per_org, 12) if args.max_per_org is not None else 12

    llm = LLMClient()
    extractor = InterviewExtractor(llm)
    session = requests.Session()

    total_processed = 0

    for org in orgs:
        if args.max_total is not None and total_processed >= args.max_total:
            print(f"Reached max total {args.max_total}. Stopping.")
            break

        articles, used_slug = fetch_org_articles(session, org)
        if not articles:
            print(f"⚠ No articles found for {org}")
            continue

        print(
            f"✔ Found {len(articles)} articles for {org}"
            + (f" (slug={used_slug})" if used_slug else "")
        )

        per_org = 0
        for article in articles:
            if args.max_total is not None and total_processed >= args.max_total:
                break
            if per_org >= per_org_limit:
                break
            
            per_org += 1
            article_slug = (
                article.get("article_slug")
                or article.get("slug")
                or article.get("articleSlug")
            )
            if not article_slug:
                print(f"  ⚠ Missing article_slug in org={org}")
                continue

            print(f"→ Processing article: {article_slug}")
            html = fetch_article_html(session, article_slug)
            if not html:
                print(f"⚠ Failed to fetch HTML for {article_slug}")
                continue

            text_content = html_to_text(html)
            if not text_content:
                print(f"⚠ Empty content for {article_slug}")
                continue

            if not is_interview_post(text_content):
                print(f"⚠ Skipped non-interview post: {article_slug}")
                continue

            source_url = f"{GFG_ARTICLE_BASE}/{article_slug}"
            title = extract_title(html)

            try:
                interview = extractor.extract(text_content, title=title)
            except Exception as e:
                print(f"⚠ LLM failed for {article_slug}: {e}, source url is {source_url}")
                continue

            if not interview.company:
                print(f"⚠ Skipped post (missing company): {source_url}")
                continue

            if not interview.questions:
                print(f"⚠ Skipped post (missing questions): {source_url}")
                continue

            interview.source_url = source_url
            interview.additional_links = []
            interview.title = title
            interview.created_date = int(time.time())
            interview.interview_hash = build_interview_hash(source_url, article_slug)
            interview.source_summary = ""
            interview.source_tags = [org]
            interview.original_content = text_content

            try:
                append_interviews(
                    "apps/leetcode_crawler/output/interview.json",
                    [interview],
                )
            except Exception as e:
                print(
                    "⚠ Failed to save interview for "
                    f"{article_slug}: {e}. Continuing."
                )
                continue

            total_processed += 1
            print(f"✔ Extracted: {interview.company} (org={org})")


if __name__ == "__main__":
    main()

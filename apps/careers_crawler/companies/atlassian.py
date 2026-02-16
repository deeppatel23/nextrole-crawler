"""
GET: https://www.atlassian.com/endpoint/careers/listings
Sample response:
[
{
        "portalJobPost": {
            "portalId": 17,
            "portalUrl": "https://globalcareers-atlassian.icims.com/jobs/22440/cloud-senior-support-engineer/job",
            "id": 22440,
            "updatedDate": "2026-01-15 02:22 AM"
        },
        "id": 22440,
        "portalId": 17,
        "title": "Cloud Senior Support Engineer",
        "locations": [
            "Yokohama - Japan -   Yokohama, Kanagawa 220-8114 Japan",
            "Remote - Remote"
        ],
        "category": "Support",
        "overview": "<p><strong><u>Working at Atlassian</u></strong></p><p>Atlassians can choose where they work – whether in an office, from home, or a combination of the two. That way, Atlassians have more control over supporting their family, personal goals, and other priorities. We can hire people in any country where we have a legal entity. Interviews and onboarding are conducted virtually, a part of being a distributed-first company.</p>",
        "responsibilities": "<p>We're looking for a Senior Technical Support Engineer to join our Cloud products support team with the goal of making our customers awesome. You will join a growing team of specialists improving our support capabilities, capacity, and quality for our customers in Japan.</p><p><strong><u>More about you:</u></strong></p><ul><li><p>You will be key to providing a consistent quality experience, bringing new and improved support methodologies to Atlassian, and building a wide and loyal customer base for the Atlassian products and brand. You will perform triage, root cause analysis, debugging, and solving across one-to-many Atlassian products. Additionally, you will work closely with engineering teams to resolve high-priority customer issues and the most complex technical challenges, and to drive mission-critical projects by leveraging your deep technical expertise. As part of the team, you will receive onboarding training to make you a specialist in one or more of our products, system technologies, and network technologies.</p></li></ul><p></p>",
        "qualifications": "<p><strong><u>On your first day, we'll expect you to have:</u></strong></p><ul><li><p>5+ years of experience in technical support, software services, and/or system administration for a large end-user community;</p></li><li><p>2+ years of experience solving complex technical problems ;</p></li><li><p>Demonstrated ability to coach and mentor other support engineers to grow their technical and troubleshooting skills;</p></li><li><p>Proven ability de-escalating difficult situations with customers, while multi-tasking between tickets and mentoring your team;</p></li><li><p>Strong interpersonal skills to effectively collaborate with a wide variety of people, from junior engineers to senior executives;</p></li><li><p>Technical experiences ;</p><ul><li><p>Strong database skills, with the expertise to write and update SQL queries with ease;</p></li><li><p>Familiarity with one or more of the scripting languages (Shell/Python etc.);</p></li><li><p>Experience with Web technologies such as DNS, HTTP, APIs and REST calls;</p></li><li><p>Familiarity with Cloud technologies such as AWS;</p></li><li><p>An understanding of Network terminologies, LAN, WAN, TCP/IP, OSI, NAT, DHCP, TLS/SSL, Routing Protocols;</p></li></ul></li><li><p>Fluent in Japanese speaking/writing and Business-level English;</p></li></ul><p> </p><p><strong><u>Preferred qualifications:</u></strong></p><ul><li><p>Experience in understanding and supporting Java apps;</p></li><li><p>Experience with Splunk;</p></li><li><p>The ability to effectively communicate as the internal expert with customers at an executive level on in-depth technical details, progress, and next steps.</p></li><li><p>Experience with AI-related technology;</p></li><li><p>Experience with technical team leader or management;</p></li><li><p>Experience with Atlassian products;</p></li></ul><p></p><p></p><p><strong>Benefits &amp; Perks</strong></p><p>Atlassian offers a wide range of perks and benefits designed to support you, your family and to help you engage with your local community. Our offerings include health and wellbeing resources, paid volunteer days, and so much more. To learn more, visit <a href=\"http://go.atlassian.com/perksandbenefits\"><strong><u>go.atlassian.com/perksandbenefits</u></strong></a><strong>.</strong></p><p><strong>About Atlassian</strong></p><p>At Atlassian, we're motivated by a common goal: to unleash the potential of every team. Our software products help teams all over the planet and our solutions are designed for all types of work. Team collaboration through our tools makes what may be impossible alone, possible together.</p><p>We believe that the unique contributions of all Atlassians create our success. To ensure that our products and culture continue to incorporate everyone's perspectives and experience, we never discriminate based on race, religion, national origin, gender identity or expression, sexual orientation, age, or marital, veteran, or disability status. All your information will be kept confidential according to EEO guidelines.</p><p>To provide you the best experience, we can support with accommodations or adjustments at any stage of the recruitment process. Simply inform our Recruitment team during your conversation with them.</p><p>To learn more about our culture and hiring process, visit <a href=\"http://go.atlassian.com/crh\"><strong><u>go.atlassian.com/crh</u></strong></a><strong>.</strong></p>",
        "applyUrl": "https://globalcareers-atlassian.icims.com/jobs/22440/cloud-senior-support-engineer/job?mode=apply"
    }
]
    job_hash: hash of id and company name
    
    # identity
    job_id: str = portalJobPost.id if null then id
    company: str = Atlassian

    # core job info
    title: str = title
    role: Optional[str] = None
    category: Optional[str] = None = category

    # experience
    min_yoe: Optional[int] = None = fetching content by opening the portalJobPost.portalUrl and applying LLM to extract yoe requirements from the content
    max_yoe: Optional[int] = None = fetching content by opening the portalJobPost.portalUrl and applying LLM to extract yoe requirements from the content

    # location
    city: Optional[str] = None = locations[0].split(" - ")[0] if locations else None
    state: Optional[str] = None
    country: Optional[str] = None = locations[0].split(" - ")[1] if len(locations) > 1 else None
    workplace_type: Optional[str] = None = locations[1].split(" - ")[0] if locations else None

    # content
    skills: List[str] = field(default_factory=list) = fetching content by opening the portalJobPost.portalUrl and applying LLM to extract skills from the content

    # links
    apply_link: Optional[str] = None = applyUrl
    source_type: Optional[str] = None = html

    # metadata
    created_at: Optional[str] = None = today()
    updated_at: Optional[str] = None

Loop the API response one after the other. 
Only consider where locations[0] sring contains India, else skip.
For each job first check if it exists in mongo db when CAREERS_OUTPUT_DESTINATION = MONGO. If not then proceed to build the role_detail object and save.

Add logic for last_saved (if today <= last_saved then dont save) and max_saved_jobs (if job opening saved ?= this value then stop).

Try to add print statement wherever necessary.

Consider this for building the role_detail object.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from config.config import OUTPUT_FILE, OUTPUT_DESTINATION
from models.role_detail import RoleDetail
from utils.hash_utils import generate_job_hash
from utils.mongo_job_hash_checker import MongoJobHashChecker
from utils.output_writer import append_roles
from utils.role_enricher import get_enrichment

API_URL = "https://www.atlassian.com/endpoint/careers/listings"


def _fetch_jobs() -> List[Dict[str, Any]]:
    resp = requests.get(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def _extract_location_parts(raw: Optional[str], second_location: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {"city": None, "country": None, "workplace_type": None}
    parts = [p.strip() for p in raw.split(" - ") if p.strip()]
    city = parts[0] if parts else None
    country = parts[1] if len(parts) > 1 else None
    workplace_type = second_location.split(" - ")[0] if second_location else None
    return {"city": city, "country": country, "workplace_type": workplace_type}


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    company = "Atlassian"
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

    print(f"{company_label}: fetching jobs from {API_URL}")
    jobs = _fetch_jobs()
    if not jobs:
        print(f"{company_label}: no jobs found.")
        return 0

    checker = MongoJobHashChecker()
    today_str = today.isoformat()
    total_saved = 0

    for job in jobs:
        if total_saved >= max_saved:
            print(f"{company_label}: reached max_saved_jobs={max_saved}, stopping.")
            break

        if not isinstance(job, dict):
            continue

        portal = job.get("portalJobPost") or {}
        portal_id = portal.get("id") if isinstance(portal, dict) else None
        job_id_val = portal_id or job.get("id")
        if not job_id_val:
            continue

        locations = job.get("locations") or []
        if not isinstance(locations, list) or not locations:
            continue

        first_location = locations[0]
        second_location = locations[1] if len(locations) > 1 else None
        if not isinstance(first_location, str):
            continue
        if "india" not in first_location.lower():
            continue

        job_hash = generate_job_hash(company, str(job_id_val))
        if OUTPUT_DESTINATION == "MONGO" and checker.exists(job_hash):
            print(f"{company_label}: existing job_hash found, skipping.")
            continue

        location_parts = _extract_location_parts(first_location, second_location)

        title = job.get("title")
        portal_url = portal.get("portalUrl") if isinstance(portal, dict) else None
        apply_link = job.get("applyUrl")

        enrichment = get_enrichment(
            title,
            portal_url,
            job.get("qualifications") or job.get("overview") or job.get("responsibilities")
        )

        role = RoleDetail(
            job_hash=job_hash,
            job_id=str(job_id_val),
            company=company,
            source_type=source_type,
            title=title,
            role=None,
            category=enrichment["category"],
            city=location_parts["city"],
            state=None,
            country=location_parts["country"],
            workplace_type=location_parts["workplace_type"],
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

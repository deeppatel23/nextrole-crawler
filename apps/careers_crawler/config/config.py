import os
from dotenv import load_dotenv

load_dotenv()

OUTPUT_FILE = os.getenv(
    "CAREERS_OUTPUT_FILE",
    "apps/careers_crawler/output/jobs.json",
)
OUTPUT_DESTINATION = os.getenv("CAREERS_OUTPUT_DESTINATION", "FILE").upper()

# MongoDB settings (used when CAREERS_OUTPUT_DESTINATION=MONGO)
MONGO_DB_USER = os.getenv("MONGO_DB_USER", "")
MONGO_DB_PSD = os.getenv("MONGO_DB_PSD", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "nextrole")
MONGO_COLLECTION = os.getenv("CAREERS_MONGO_COLLECTION", "careers_jobs")

# Prefer explicit MONGO_URI, otherwise build from template + creds.
_mongo_uri = os.getenv("MONGO_URI", "").strip()
_mongo_uri_template = os.getenv("MONGO_URI_TEMPLATE", "").strip()
if not _mongo_uri and _mongo_uri_template:
    _mongo_uri = _mongo_uri_template.format(
        user=MONGO_DB_USER,
        password=MONGO_DB_PSD,
    )
MONGO_URI = _mongo_uri

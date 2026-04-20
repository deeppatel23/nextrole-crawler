import os
from dotenv import load_dotenv

load_dotenv()

def _get_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


_is_vercel = os.getenv("VERCEL", "").lower() in {"1", "true", "yes", "y"}

OUTPUT_FILE = os.getenv(
    "CAREERS_OUTPUT_FILE",
    "/tmp/careers_jobs.json" if _is_vercel else "apps/careers_crawler/output/jobs.json",
)
OUTPUT_DESTINATION = os.getenv("CAREERS_OUTPUT_DESTINATION", "MONGO").upper()

# When enabled, the crawler reads `careers_sources.yaml` but will not write updates back (e.g., last_saved).
# Supports env vars: debug_mode, DEBUG_MODE, CAREERS_DEBUG_MODE.
DEBUG_MODE = _get_bool(
    os.getenv("CAREERS_DEBUG_MODE", os.getenv("DEBUG_MODE", os.getenv("debug_mode", "false"))),
    default=False,
)

# MongoDB settings (used when CAREERS_OUTPUT_DESTINATION=MONGO)
MONGO_DB_USER = os.getenv("MONGO_DB_USER", "")
MONGO_DB_PSD = os.getenv("MONGO_DB_PSD", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "nextrole")
MONGO_COLLECTION = os.getenv("CAREERS_MONGO_COLLECTION", "")
CAREERS_TTL_DAYS = int(os.getenv("CAREERS_TTL_DAYS", "21"))
MONGO_TLS_ALLOW_INVALID_CERTS = os.getenv(
    "MONGO_TLS_ALLOW_INVALID_CERTS", "false"
).lower() in {"1", "true", "yes", "y"}

# Prefer explicit MONGO_URI, otherwise build from template + creds.
_mongo_uri = os.getenv("MONGO_URI", "").strip()
_mongo_uri_template = os.getenv("MONGO_URI_TEMPLATE", "").strip()
if not _mongo_uri and _mongo_uri_template:
    _mongo_uri = _mongo_uri_template.format(
        user=MONGO_DB_USER,
        password=MONGO_DB_PSD,
    )
MONGO_URI = _mongo_uri

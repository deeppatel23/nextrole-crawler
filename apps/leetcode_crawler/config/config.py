import os
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_MODEL = "moonshotai/Kimi-K2-Instruct-0905"

LEETCODE_GRAPHQL_URL = "https://leetcode.com/graphql"

OUTPUT_FILE = "interview_experiences.json"
OUTPUT_DESTINATION = os.getenv("LEETCODE_OUTPUT_DESTINATION", "FILE").upper()

# MongoDB settings (used when OUTPUT_DESTINATION=MONGO)
MONGO_DB_USER = os.getenv("MONGO_DB_USER", "")
MONGO_DB_PSD = os.getenv("MONGO_DB_PSD", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "nextrole")
MONGO_COLLECTION = os.getenv("LEETCODE_MONGO_COLLECTION", "leetcode_interviews")

# Prefer explicit MONGO_URI, otherwise build from template + creds.
_mongo_uri = os.getenv("MONGO_URI", "").strip()
_mongo_uri_template = os.getenv("MONGO_URI_TEMPLATE", "").strip()
if not _mongo_uri and _mongo_uri_template:
    _mongo_uri = _mongo_uri_template.format(
        user=MONGO_DB_USER,
        password=MONGO_DB_PSD,
    )
MONGO_URI = _mongo_uri

REQUEST_TIMEOUT = 30
MAX_POSTS = 10

EXTRACTOR_PROMPT_MODE = os.getenv("EXTRACTOR_PROMPT_MODE", "only_questions")

# Incremental load settings
LEETCODE_STATE_FILE = os.getenv(
    "LEETCODE_STATE_FILE",
    "apps/leetcode_crawler/output/leetcode_state.yml",
)

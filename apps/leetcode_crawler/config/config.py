import os
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.getenv("HF_TOKEN", "")
HF_MODEL = "moonshotai/Kimi-K2-Instruct-0905"

LEETCODE_GRAPHQL_URL = "https://leetcode.com/graphql"

OUTPUT_FILE = "interview_experiences.json"

REQUEST_TIMEOUT = 20
MAX_POSTS = 10

EXTRACTOR_PROMPT_MODE = os.getenv("EXTRACTOR_PROMPT_MODE", "only_questions")

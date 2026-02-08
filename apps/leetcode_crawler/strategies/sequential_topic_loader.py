import argparse
import re
import sys
from pathlib import Path
from hashlib import sha256

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from processor.extractor import InterviewExtractor
from utils.llm_client import LLMClient
from utils.leetcode_client import LeetCodeClient
from utils.output_writer import append_interviews
from utils.rules import is_interview_post


def build_discuss_url(topic_id: int) -> str:
    return f"https://leetcode.com/discuss/post/{topic_id}/"


def extract_links(text: str) -> list[str]:
    pattern = r"https?://[^\s\)\]\}<>\"']+"
    return sorted({match.rstrip(".,;:") for match in re.findall(pattern, text)})


def build_interview_hash(topic_id: int, slug: str | None) -> str:
    base = f"{topic_id}:{slug or ''}"
    return sha256(base.encode("utf-8")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sequentially crawl LeetCode discuss posts by topic id."
    )
    parser.add_argument("--start-id", type=int, default=6_000_000)
    parser.add_argument("--end-id", type=int, default=None)
    parser.add_argument("--max-steps", type=int, default=None)
    parser.add_argument("--max-misses", type=int, default=1000)
    args = parser.parse_args()

    leetcode = LeetCodeClient()
    llm = LLMClient()
    extractor = InterviewExtractor(llm)

    current_id = args.start_id
    processed = 0
    consecutive_misses = 0

    while True:
        if args.end_id is not None and current_id > args.end_id:
            print(f"Reached end id {args.end_id}. Stopping.")
            break
        if args.max_steps is not None and processed >= args.max_steps:
            print(f"Reached max steps {args.max_steps}. Stopping.")
            break
        if args.max_misses is not None and consecutive_misses >= args.max_misses:
            print(f"Reached max consecutive misses {args.max_misses}. Stopping.")
            break

        content = leetcode.fetch_post_content(slug="", topic_id=current_id)
        if not content:
            consecutive_misses += 1
            current_id += 1
            continue

        consecutive_misses = 0
        processed += 1

        if not is_interview_post(content):
            print(f"⚠ Skipped non-interview post id: {current_id}")
            current_id += 1
            continue

        source_url = build_discuss_url(current_id)
        try:
            interview = extractor.extract(content, title=None)
        except Exception as e:
            print(f"⚠ LLM failed for {current_id}: {e}, source url is {source_url}")
            current_id += 1
            continue

        if not interview.company:
            print(f"⚠ Skipped post (missing company): {source_url}")
            current_id += 1
            continue

        if not interview.questions:
            print(f"⚠ Skipped post (missing questions): {source_url}")
            current_id += 1
            continue

        interview.source_url = source_url
        interview.additional_links = extract_links(content)
        interview.title = None
        interview.created_date = None
        interview.interview_hash = build_interview_hash(current_id, None)
        interview.source_summary = ""
        interview.source_tags = []
        interview.original_content = content

        append_interviews("apps/leetcode_crawler/output/interview.json", [interview])

        print(f"✔ Extracted: {interview.company} (topic_id={current_id})")
        current_id += 1


if __name__ == "__main__":
    main()

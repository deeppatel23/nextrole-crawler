from utils.leetcode_client import LeetCodeClient
from utils.llm_client import LLMClient
from processor.extractor import InterviewExtractor
from models.interview import InterviewExperience
from processor.repository import InterviewRepository
from utils.rules import is_interview_post
from config.config import MAX_POSTS
import re
from utils.output_writer import append_interviews
from config.config import OUTPUT_DESTINATION, LEETCODE_STATE_FILE
from utils.state_store import read_state, set_one_time_last_saved_timestamp
from utils.mongo_writer import has_interview_hash
from hashlib import sha256
from datetime import datetime


def build_discuss_url(topic_id: int | None, slug: str) -> str | None:
    if not topic_id:
        return None
    return f"https://leetcode.com/discuss/post/{topic_id}/{slug}/"


def extract_links(text: str) -> list[str]:
    # Keep it simple and robust for raw content.
    pattern = r"https?://[^\s\)\]\}<>\"']+"
    return sorted({match.rstrip(".,;:") for match in re.findall(pattern, text)})


def build_interview_hash(topic_id: int | None, slug: str | None) -> str:
    base = f"{topic_id or ''}:{slug or ''}"
    return sha256(base.encode("utf-8")).hexdigest()


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def main():
    leetcode = LeetCodeClient()
    llm = LLMClient()
    extractor = InterviewExtractor(llm)
    repo = InterviewRepository()

    results = []
    state = read_state(LEETCODE_STATE_FILE)
    one_time_data_load = bool(state.get("one_time_data_load", True))
    one_time_post_limit = state.get("one_time_post_limit", 9999)
    one_time_load_last_saved_timestamp = state.get(
        "one_time_load_last_saved_timestamp"
    )
    one_time_cutoff = _parse_iso_timestamp(one_time_load_last_saved_timestamp)
    incremental_mode = OUTPUT_DESTINATION == "MONGO" and not one_time_data_load

    skip = 0
    processed = 0
    order_by = "MOST_RECENT" if incremental_mode or one_time_cutoff else "HOT"
    passed_one_time_cutoff = one_time_cutoff is None

    while True:
        posts = leetcode.fetch_posts(MAX_POSTS, tag="interview", skip=skip, order_by=order_by)
        if not posts:
            break

        for post in posts:
            try:
                processed += 1
                if one_time_data_load and processed > one_time_post_limit:
                    print(
                        f"Reached one-time post limit of {one_time_post_limit}. Stopping."
                    )
                    return

                slug = post["slug"]
                summary = post.get("summary", "")
                created_at = post.get("createdAt")

                if one_time_data_load and not passed_one_time_cutoff:
                    created_dt = _parse_iso_timestamp(created_at)
                    if created_dt and created_dt >= one_time_cutoff:
                        print(f"Skipping post {slug} created at {created_at} (after cutoff)")
                        continue
                    passed_one_time_cutoff = True

                if incremental_mode:
                    topic_id = post.get("topicId")
                    interview_hash = build_interview_hash(topic_id, slug)
                    if has_interview_hash(interview_hash):
                        return

                try:
                    print("Post is being processed:", post.get("title"), " counter is ", processed)
                    slug = post["slug"]
                    topic_id = post.get("topicId")

                    content = leetcode.fetch_post_content(slug, topic_id)

                    print(f"✔ Fetched content for {slug}")

                    if not content:
                        print(f"⚠ Using summary for {slug}")
                        content = summary

                except Exception as e:
                    print(f"⚠ Using summary for {slug}: {e}")
                    content = summary

                if not is_interview_post(content):
                    print(f"⚠ Skipped non-interview post: {slug}")
                    continue

                source_url = build_discuss_url(topic_id, slug)
                try:
                    interview = extractor.extract(content, title=post.get("title"))
                except Exception as e:
                    print(f"⚠ LLM failed for {slug}: {e}, source url is {source_url}")
                    continue

                if not interview.company:
                    print(
                        f"⚠ Skipped post (missing company): {source_url or slug}"
                    )
                    continue

                if not interview.questions:
                    print(
                        f"⚠ Skipped post (missing questions): {source_url or slug}"
                    )
                    continue

                interview.source_url = source_url
                interview.additional_links = extract_links(content)
                interview.title = post.get("title")
                interview.created_date = created_at
                interview.interview_hash = build_interview_hash(topic_id, slug)
                interview.source_summary = summary
                interview.source_tags = [t.get("slug") for t in post.get("tags", []) if t]
                interview.original_content = content
                # append_interviews expects an iterable; pass a single-item list
                append_interviews("apps/leetcode_crawler/output/interview.json", [interview])

                if one_time_data_load and created_at:
                    set_one_time_last_saved_timestamp(
                        LEETCODE_STATE_FILE, created_at
                    )

                print(f"✔ Extracted: {interview.company}, created at {created_at}")

            except Exception as e:
                print(f"⚠ Skipped post: {e}")

        skip += MAX_POSTS




if __name__ == "__main__":
    main()

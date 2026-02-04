from utils.leetcode_client import LeetCodeClient
from utils.llm_client import LLMClient
from processor.extractor import InterviewExtractor
from models.interview import InterviewExperience
from processor.repository import InterviewRepository
from utils.rules import is_interview_post
from config.config import MAX_POSTS
import re
from utils.output_writer import append_interviews


def build_discuss_url(topic_id: int | None, slug: str) -> str | None:
    if not topic_id:
        return None
    return f"https://leetcode.com/discuss/post/{topic_id}/{slug}/"


def extract_links(text: str) -> list[str]:
    # Keep it simple and robust for raw content.
    pattern = r"https?://[^\s\)\]\}<>\"']+"
    return sorted({match.rstrip(".,;:") for match in re.findall(pattern, text)})


def main():
    leetcode = LeetCodeClient()
    llm = LLMClient()
    extractor = InterviewExtractor(llm)
    repo = InterviewRepository()

    posts = leetcode.fetch_posts(MAX_POSTS, tag="interview")
    results = []

    for post in posts:
        try:
            slug = post["slug"]
            summary = post.get("summary", "")

            try:
                print("Post is being processed:", post.get("title"))
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
            interview.source_summary = summary
            interview.source_tags = [t.get("slug") for t in post.get("tags", []) if t]
            interview.original_content = content
            # append_interviews expects an iterable; pass a single-item list
            append_interviews("apps/leetcode_crawler/output/interview.json", [interview])

            print(f"✔ Extracted: {interview.company}")

        except Exception as e:
            print(f"⚠ Skipped post: {e}")




if __name__ == "__main__":
    main()

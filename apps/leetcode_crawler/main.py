from utils.leetcode_client import LeetCodeClient
from utils.llm_client import LLMClient
from processor.extractor import InterviewExtractor
from processor.repository import InterviewRepository
from utils.rules import is_interview_post
from config.config import MAX_POSTS
import re
from utils.json_writer import append_jobs


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
                print("Post is being processed:", post)
                slug = post["slug"]
                topic_id = post.get("topicId")

                content = leetcode.fetch_post_content(slug, topic_id)

                print(f"✔ Fetched content for {slug}, content {content}")

                if not content:
                    print(f"⚠ Using summary for {slug}")
                    content = summary

            except Exception as e:
                print(f"⚠ Using summary for {slug}: {e}")
                content = summary

            if not is_interview_post(content):
                continue

            interview = extractor.extract(content)
            interview.source_url = build_discuss_url(topic_id, slug)
            interview.additional_links = extract_links(content)
            # append_jobs expects an iterable of jobs; pass a single-item list
            append_jobs("apps/leetcode_crawler/output/interview.json", [interview])

            print(f"✔ Extracted: {interview.company}")

        except Exception as e:
            print(f"⚠ Skipped post: {e}")




if __name__ == "__main__":
    main()

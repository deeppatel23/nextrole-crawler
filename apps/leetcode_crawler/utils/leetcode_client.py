import requests
from typing import List
from config.config import LEETCODE_GRAPHQL_URL, REQUEST_TIMEOUT


class LeetCodeClient:
    def fetch_posts(
        self,
        limit: int,
        tag: str | None = "interview",
        skip: int = 0,
        order_by: str = "MOST_RECENT", #HOT
    ) -> List[dict]:

        query = """
        query discussPostItems(
          $orderBy: ArticleOrderByEnum,
          $keywords: [String]!,
          $tagSlugs: [String!],
          $skip: Int,
          $first: Int
        ) {
          ugcArticleDiscussionArticles(
            orderBy: $orderBy
            keywords: $keywords
            tagSlugs: $tagSlugs
            skip: $skip
            first: $first
          ) {
            edges {
              node {
                uuid
                title
                slug
                summary
                topicId
                createdAt
                tags {
                  name
                  slug
                }
              }
            }
          }
        }
        """

        payload = {
            "query": query,
            "variables": {
                "orderBy": order_by,
                "keywords": [""],
                "tagSlugs": [] if not tag else [tag],
                "skip": skip,
                "first": limit,
            },
            "operationName": "discussPostItems",
        }

        headers = {
            "Content-Type": "application/json",
            "Referer": "https://leetcode.com/discuss/",
            "User-Agent": "Mozilla/5.0",
        }

        try:
            response = requests.post(
                LEETCODE_GRAPHQL_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code != 200:
                raise RuntimeError(
                    f"HTTP {response.status_code} | {response.text[:300]}"
                )

            data = response.json()

            if "errors" in data:
                raise RuntimeError(f"GraphQL errors: {data['errors']}")

            edges = data["data"]["ugcArticleDiscussionArticles"]["edges"]

            return [edge["node"] for edge in edges]

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Network error calling LeetCode: {e}")

        except KeyError:
            raise RuntimeError(
                f"Unexpected response structure: {response.text[:300]}"
            )
        
    def fetch_post_content(
        self,
        slug: str,
        topic_id: int | None,
        uuid: str | None = None,
    ) -> str | None:

        def _query(query, variables, key):
            try:
                resp = requests.post(
                    LEETCODE_GRAPHQL_URL,
                    json={"query": query, "variables": variables},
                    headers={
                        "Content-Type": "application/json",
                        "Referer": "https://leetcode.com/discuss/",
                        "User-Agent": "Mozilla/5.0",
                    },
                    timeout=REQUEST_TIMEOUT,
                )

                data = resp.json()
                # print(f"Fetched data for {key}: {data}")
                if "errors" in data:
                    return None

                return data.get("data", {}).get(key)

            except Exception:
                return None

        # ---------- 1️⃣ UGC discussion article ----------
        ugc_query = """
        query($slug: String!) {
          ugcArticleDiscussionArticle(slug: $slug) {
            content
          }
        }
        """

        content_node = _query(
            ugc_query,
            {"slug": slug},
            "ugcArticleDiscussionArticle",
        )
        if content_node:
            return content_node.get("content")

        # ---------- 2️⃣ topic ----------
        if topic_id:
            topic_query = """
            query($id: Int!) {
            topic(id: $id) {
                post {
                  content
                }
            }
            }
            """

            topic_node = _query(topic_query, {"id": topic_id}, "topic")
            if topic_node and topic_node.get("post"):
                return topic_node["post"].get("content")

        return None

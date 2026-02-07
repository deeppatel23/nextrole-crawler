# nextrole-crawler

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py
deactivate
```

Local LLM
```
brew install ollama
ollama serve

ollama pull llama3
```

## LeetCode One-Time Load State

The LeetCode crawler reads one-time load settings from:
`apps/leetcode_crawler/output/leetcode_state.yml`

Fields:
- `one_time_data_load`: boolean.
- `one_time_post_limit`: integer. Maximum number of posts to attempt in one-time mode.
- `skip_posts`: integer. Number of most-recent posts to skip when running one-time mode.

Behavior:
- When `one_time_data_load: true`:
  - The crawler runs a one-time batch and stops after `one_time_post_limit` attempted posts.
  - Fetches posts in `MOST_RECENT` order.
  - Uses `skip_posts` as the GraphQL `skip` value to resume from a specific offset.
  - After each successfully saved post, `skip_posts` is incremented and written back to the YAML.
- When `one_time_data_load: false`:
  - Incremental mode is enabled (only when output destination is Mongo).
  - Posts are fetched in `MOST_RECENT` order and the crawler stops once it sees a post whose `interview_hash` already exists in Mongo.

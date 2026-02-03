import requests
import json


class LLMClient:
    def __init__(self, base_url="http://localhost:11434"):
        self.base_url = base_url

    def extract_json(self, prompt: str) -> str:
        payload = {
            "model": "mistral",
            "prompt": prompt,
            "stream": False,
        }

        try:
            resp = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()["response"].strip()

        except Exception as e:
            raise RuntimeError(f"Local LLM failed: {e}")

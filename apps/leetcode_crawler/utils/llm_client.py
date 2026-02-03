import requests
import json


class LLMClient:
    def __init__(self, base_url="http://0.0.0.0:6969/ask"): #OLLAMA: http://localhost:11434 GROK:http://localhost:6969/ask
        self.base_url = base_url

    def extract_json(self, prompt: str) -> str:
        #OLLAMA
        # payload = {
        #     "model": "mistral",
        #     "prompt": prompt,
        #     "stream": False,
        # }

        #Grok
        payload = {
            "proxy": "http://user:pass@ip:port",
            "message": prompt,
            "model": "grok-3-fast",
            "extra_data": None
        }

        try:
            resp = requests.post(
                f"{self.base_url}", #add /api/generate for OLLAMA
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()["response"].strip()

        except Exception as e:
            raise RuntimeError(f"Local LLM failed: {e}")

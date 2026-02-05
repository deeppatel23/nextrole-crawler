import os
from typing import Any

import requests
from dotenv import load_dotenv


class LLMClient:
    def __init__(self):
        load_dotenv()
        self.provider = os.getenv("LLM_PROVIDER", "GEMINI").upper()

        # Ollama settings
        self.ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.ollama_model = os.getenv("OLLAMA_MODEL", "mistral")

        # Gemini settings
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "")
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        self.gemini_api_version = os.getenv("GEMINI_API_VERSION", "v1beta")
        self.gemini_base_url = os.getenv(
            "GEMINI_BASE_URL",
            f"https://generativelanguage.googleapis.com/{self.gemini_api_version}",
        )

        self.timeout = int(os.getenv("LLM_TIMEOUT", "60"))

    def extract_json(self, prompt: str) -> str:
        if self.provider == "GEMINI":
            return self._extract_json_gemini(prompt)

        return self._extract_json_ollama(prompt)

    def _extract_json_ollama(self, prompt: str) -> str:
        payload = {
            "model": self.ollama_model,
            "prompt": prompt,
            "stream": False,
        }
        try:
            resp = requests.post(
                f"{self.ollama_base_url}/api/generate",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()["response"].strip()

        except Exception as e:
            raise RuntimeError(f"Ollama LLM failed: {e}")

    def _extract_json_gemini(self, prompt: str) -> str:
        if not self.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not set in .env")

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "response_mime_type": "application/json",
            },
        }
        model_name = self.gemini_model
        if model_name.startswith("models/"):
            model_name = model_name.split("/", 1)[1]

        try:
            resp = requests.post(
                f"{self.gemini_base_url}/models/{model_name}:generateContent",
                headers={
                    "x-goog-api-key": self.gemini_api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return self._parse_gemini_response(resp.json()).strip()
        except Exception as e:
            raise RuntimeError(f"Gemini LLM failed: {e}")

    @staticmethod
    def _parse_gemini_response(data: dict[str, Any]) -> str:
        candidates = data.get("candidates", [])
        if not candidates:
            raise RuntimeError("Gemini response missing candidates")
        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        if not parts or "text" not in parts[0]:
            raise RuntimeError("Gemini response missing text")
        return parts[0]["text"]

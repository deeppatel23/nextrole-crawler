from __future__ import annotations

from typing import Any, Dict

from companies.salesforce import fetch_and_save as fetch_salesforce_and_save


# Backward-compatible alias. Source has been migrated from Informatica to Salesforce.
COMPANY = "Salesforce"
CAREERS_URL = "https://careers.salesforce.com/en/jobs/?search"
SOURCE_TYPE = "HTML"


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    source_cfg = dict(source_cfg or {})
    source_cfg.setdefault("company", COMPANY)
    source_cfg.setdefault("careers_url", CAREERS_URL)
    source_cfg.setdefault("source_type", SOURCE_TYPE)
    return fetch_salesforce_and_save(source_cfg)

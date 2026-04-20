from __future__ import annotations

from typing import Any, Dict

from companies._provider_common import fetch_company_jobs


COMPANY = "Toppr"
CAREERS_URL = "https://apply.workable.com/toppr/?lng=en"
SOURCE_TYPE = "API"
GREENHOUSE_BOARD = ""
WORKABLE_ACCOUNT = "toppr"
LEVER_COMPANY = ""


def fetch_and_save(source_cfg: Dict[str, Any]) -> int:
    return fetch_company_jobs(
        source_cfg,
        default_company=COMPANY,
        default_careers_url=CAREERS_URL,
        default_source_type=SOURCE_TYPE,
        greenhouse_board=GREENHOUSE_BOARD or None,
        workable_account=WORKABLE_ACCOUNT or None,
        lever_company=LEVER_COMPANY or None,
    )

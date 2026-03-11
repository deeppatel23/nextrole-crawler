from __future__ import annotations

from typing import List, Tuple

from models.role_detail import RoleDetail
from utils.category_enricher import get_category_options


def _is_blank(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    return False


def validate_role(role: RoleDetail) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    # Mandatory fields
    if _is_blank(role.job_hash):
        errors.append("job_hash")
    if _is_blank(role.job_id):
        errors.append("job_id")
    if _is_blank(role.company):
        errors.append("company")
    if _is_blank(role.title):
        errors.append("title")
    if _is_blank(role.category):
        errors.append("category")
    if _is_blank(role.city):
        errors.append("city")
    if _is_blank(role.skills):
        errors.append("skills")
    if _is_blank(role.apply_link):
        errors.append("apply_link")
    if _is_blank(role.created_at):
        errors.append("created_at")

    # Category whitelist
    if not _is_blank(role.category):
        allowed = set(get_category_options())
        if role.category not in allowed:
            errors.append("category_invalid")

    return len(errors) == 0, errors

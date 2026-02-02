from models.role_detail import RoleDetail
from parsers.value_resolver import get_by_path
from utils.hash_utils import generate_job_hash


def map_to_role(raw_job, mapping, company):
    mapped = {}

    for field, path in mapping.items():
        mapped[field] = get_by_path(raw_job, path)

    job_id = mapped.get("job_id")

    job_hash = generate_job_hash(
        company=company,
        job_id=job_id
    )

    return RoleDetail(
        job_hash=job_hash,
        company=company,
        # raw=raw_job, #uncomment for debugging
        **mapped
    )

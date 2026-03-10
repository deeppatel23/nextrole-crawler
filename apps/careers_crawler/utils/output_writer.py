from typing import Iterable, Tuple

from config.config import OUTPUT_DESTINATION
from storage.json_writer import append_jobs
from utils.mongo_writer import append_jobs_mongo
from utils.role_validator import validate_role


def append_roles(destination_path: str, jobs: Iterable) -> Tuple[int, bool]:
    jobs_list = []
    for job in jobs:
        ok, errors = validate_role(job)
        if ok:
            jobs_list.append(job)
        else:
            print(
                f"Careers: skipped invalid job company={getattr(job, 'company', None)} "
                f"job_id={getattr(job, 'job_id', None)} errors={','.join(errors)}"
            )

    if not jobs_list:
        return 0, False

    if OUTPUT_DESTINATION == "MONGO":
        return append_jobs_mongo(jobs_list)
    else:
        append_jobs(destination_path, jobs_list)
        return len(jobs_list), False

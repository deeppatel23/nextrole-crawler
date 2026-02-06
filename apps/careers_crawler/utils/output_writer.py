from typing import Iterable, Tuple

from config.config import OUTPUT_DESTINATION
from storage.json_writer import append_jobs
from utils.mongo_writer import append_jobs_mongo


def append_roles(destination_path: str, jobs: Iterable) -> Tuple[int, bool]:
    jobs_list = list(jobs)
    if OUTPUT_DESTINATION == "MONGO":
        return append_jobs_mongo(jobs_list)
    else:
        append_jobs(destination_path, jobs_list)
        return len(jobs_list), False

from typing import Iterable

from config.config import OUTPUT_DESTINATION
from storage.json_writer import append_jobs
from utils.mongo_writer import append_jobs_mongo


def append_roles(destination_path: str, jobs: Iterable) -> None:
    if OUTPUT_DESTINATION == "MONGO":
        append_jobs_mongo(jobs)
    else:
        append_jobs(destination_path, jobs)

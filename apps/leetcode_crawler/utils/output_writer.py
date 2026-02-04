from typing import Iterable

from config.config import OUTPUT_DESTINATION
from utils.json_writer import append_jobs
from utils.mongo_writer import append_jobs_mongo


def append_interviews(destination_path: str, interviews: Iterable) -> None:
    if OUTPUT_DESTINATION == "MONGO":
        append_jobs_mongo(interviews)
    else:
        append_jobs(destination_path, interviews)

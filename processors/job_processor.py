from clients.http_client import call_api
from mappers.role_mapper import map_to_role
import importlib


def process_source(source_cfg):
    response = call_api(
        method=source_cfg["api"]["method"],
        url=source_cfg["api"]["url"],
        headers=source_cfg["api"].get("headers"),
        body=source_cfg["api"].get("body"),
    )

    jobs = []

    # If a custom parser is specified in the source config, delegate parsing to it
    parser_name = source_cfg.get("parser")
    if parser_name:
        try:
            parser_mod = importlib.import_module(f"parsers.custom.{parser_name}")
            raw_jobs_iter = parser_mod.parse(response, source_cfg)
            for raw_job in raw_jobs_iter:
                jobs.append(raw_job)
            return jobs
        except Exception as e:
            print(f"Failed to load custom parser '{parser_name}': {e}")
            return []
    else:
        print("No custom parser specified, using default extraction")
        return []

import yaml
from processors.job_processor import process_source
from storage.json_writer import append_jobs


def load_sources():
    with open("config/careers_sources.yaml") as f:
        return yaml.safe_load(f)


def main():
    sources = load_sources()

    for source in sources:
        jobs = process_source(source)
        append_jobs("output/jobs.json", jobs)
        print(f"{source['company']}: saved {len(jobs)} jobs")


if __name__ == "__main__":
    main()

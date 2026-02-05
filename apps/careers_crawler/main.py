import yaml
from processors.job_processor import process_source


def load_sources():
    with open("apps/careers_crawler/config/careers_sources.yaml") as f:
        return yaml.safe_load(f)


def main():
    sources = load_sources()

    for source in sources:
        saved_count = process_source(source)
        print(f"{source['company']}: saved {saved_count} jobs")


if __name__ == "__main__":
    main()

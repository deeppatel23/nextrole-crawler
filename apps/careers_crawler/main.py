import yaml
from config.config import OUTPUT_DESTINATION
from processors.job_processor import process_source


def load_sources():
    with open("apps/careers_crawler/config/careers_sources.yaml") as f:
        return yaml.safe_load(f)


def main():
    sources = load_sources()

    for source in sources:
        result = process_source(source)
        company = source["company"]
        saved_count = int(result.get("saved_count", 0))
        status = result.get("status", "failed")
        print(f"{company}: saved {saved_count} jobs")

        if OUTPUT_DESTINATION == "MONGO":
            try:
                from utils.crawler_logs_writer import write_crawler_log

                write_crawler_log(
                    company_name=company,
                    saved_count=saved_count,
                    status=status,
                )
            except Exception as exc:
                print(f"{company}: failed to write crawler log: {exc}")


if __name__ == "__main__":
    main()

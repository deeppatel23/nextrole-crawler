import importlib
from datetime import date
from pathlib import Path

import yaml


def process_source(source_cfg):
    handler_name = source_cfg.get("handler")
    if not handler_name:
        print(f"No handler specified for {source_cfg.get('company')}, skipping.")
        return {"saved_count": 0, "status": "failed"}

    company = source_cfg.get("company")
    today = date.today().isoformat()
    last_saved = source_cfg.get("last_saved")
    if last_saved and today <= last_saved:
        print(f"{company}: last_saved={last_saved}, today={today}. Skipping.")
        return {"saved_count": 0, "status": "success"}

    try:
        handler_mod = importlib.import_module(f"companies.{handler_name}")
        saved_count = handler_mod.fetch_and_save(source_cfg)

        config_path = Path("apps/careers_crawler/config/careers_sources.yaml")
        sources = yaml.safe_load(config_path.read_text())
        for source in sources:
            if source.get("company") == company:
                source["last_saved"] = today
                break
        config_path.write_text(yaml.safe_dump(sources, sort_keys=False))
        print(f"{company}: updated last_saved to {today}")

        return {"saved_count": saved_count, "status": "success"}
    except Exception as e:
        print(f"Failed to load handler '{handler_name}': {e}")
        return {"saved_count": 0, "status": "failed"}

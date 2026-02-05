import importlib


def process_source(source_cfg):
    handler_name = source_cfg.get("handler")
    if not handler_name:
        print(f"No handler specified for {source_cfg.get('company')}, skipping.")
        return 0

    try:
        handler_mod = importlib.import_module(f"companies.{handler_name}")
        return handler_mod.fetch_and_save(source_cfg)
    except Exception as e:
        print(f"Failed to load handler '{handler_name}': {e}")
        return 0

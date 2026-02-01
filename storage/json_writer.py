import json
from dataclasses import asdict
from pathlib import Path


def append_jobs(file_path, jobs):
    path = Path(file_path)
    # ensure parent dir exists
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        with open(path) as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = []
    else:
        existing = []

    for job in jobs:
        existing.append(asdict(job))

    with open(path, "w") as f:
        json.dump(existing, f, indent=2)

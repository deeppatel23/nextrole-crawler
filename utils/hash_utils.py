import hashlib

def generate_job_hash(company: str, job_id: str) -> str:
    raw = f"{company}|{job_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

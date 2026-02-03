def is_interview_post(text: str) -> bool:
    keywords = ["interview", "round", "selected", "rejected", "OA"]
    text = text.lower()
    return any(k in text for k in keywords)

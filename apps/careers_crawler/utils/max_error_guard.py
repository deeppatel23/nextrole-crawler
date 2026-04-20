from __future__ import annotations

from contextlib import contextmanager
from typing import Callable, Iterator, Optional

import requests


class MaxAllowedErrorsReached(BaseException):
    def __init__(self, company_label: str, max_allowed_error: int, last_error: str):
        super().__init__(
            f"{company_label}: max_allowed_error={max_allowed_error} reached. Last error: {last_error}"
        )
        self.company_label = company_label
        self.max_allowed_error = max_allowed_error
        self.last_error = last_error


def _should_count_status(status_code: int) -> bool:
    # Count the statuses that generally indicate throttling/bot protection/outages.
    return status_code in {401, 403, 408, 409, 425, 429} or status_code >= 500


@contextmanager
def guard_requests_errors(company_label: str, max_allowed_error: int) -> Iterator[None]:
    """
    Guard outgoing `requests` traffic for a single company run.

    - Counts network exceptions.
    - Counts HTTP statuses that likely indicate an error state (403/429/5xx, etc.).
    - When the counter reaches `max_allowed_error`, raises `MaxAllowedErrorsReached`
      (a BaseException) so company handlers that catch `Exception` won't swallow it.
    """
    if max_allowed_error is None:
        max_allowed_error = 5
    try:
        limit = int(max_allowed_error)
    except Exception:
        limit = 5
    if limit <= 0:
        # Disable the guard entirely.
        yield
        return

    error_count = 0

    def bump(reason: str) -> None:
        nonlocal error_count
        error_count += 1
        print(f"{company_label}: error_count={error_count}/{limit} reason={reason}")
        if error_count >= limit:
            raise MaxAllowedErrorsReached(company_label, limit, reason)

    orig_session_request: Callable[..., requests.Response] = requests.sessions.Session.request
    orig_request: Callable[..., requests.Response] = requests.request

    def wrapped_session_request(self, method: str, url: str, **kwargs):  # type: ignore[no-untyped-def]
        try:
            resp = orig_session_request(self, method, url, **kwargs)
        except Exception as exc:  # noqa: BLE001
            bump(f"{method} {url} exc={type(exc).__name__}")
            raise
        try:
            if hasattr(resp, "status_code") and isinstance(resp.status_code, int) and _should_count_status(resp.status_code):
                bump(f"{method} {url} status={resp.status_code}")
        except MaxAllowedErrorsReached:
            raise
        except Exception:
            pass
        return resp

    def wrapped_request(method: str, url: str, **kwargs):  # type: ignore[no-untyped-def]
        try:
            resp = orig_request(method, url, **kwargs)
        except Exception as exc:  # noqa: BLE001
            bump(f"{method} {url} exc={type(exc).__name__}")
            raise
        try:
            if hasattr(resp, "status_code") and isinstance(resp.status_code, int) and _should_count_status(resp.status_code):
                bump(f"{method} {url} status={resp.status_code}")
        except MaxAllowedErrorsReached:
            raise
        except Exception:
            pass
        return resp

    requests.sessions.Session.request = wrapped_session_request  # type: ignore[assignment]
    requests.request = wrapped_request  # type: ignore[assignment]
    try:
        yield
    finally:
        requests.sessions.Session.request = orig_session_request  # type: ignore[assignment]
        requests.request = orig_request  # type: ignore[assignment]


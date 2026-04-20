from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional


def build_cookie_header(cookies: Dict[str, str]) -> str:
    parts = []
    for name, value in cookies.items():
        if not name or value is None:
            continue
        parts.append(f"{name}={value}")
    return "; ".join(parts)


def _looks_like_cloudflare_challenge(title: str, html_text: str) -> bool:
    title_l = (title or "").strip().lower()
    html_l = (html_text or "").lower()
    if "attention required" in title_l or "cloudflare" in title_l:
        return True
    markers = (
        "/cdn-cgi/",
        "cf-chl",
        "turnstile",
        "captcha",
        "checking your browser",
        "verify you are human",
    )
    return any(m in html_l for m in markers)


def get_cookie_header_via_playwright(
    url: str,
    *,
    user_agent: str,
    timeout_ms: int = 45000,
    headless: bool = True,
    user_data_dir: Optional[str] = None,
) -> str:
    """
    Fetch cookies for `url` using a real browser engine (Playwright).

    This is primarily used to get Cloudflare clearance cookies (e.g., cf_clearance)
    for sites that block python-requests/curl.
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Playwright is not installed. Install it with:\n"
            "  pip install playwright\n"
            "  python -m playwright install chromium\n"
        ) from exc

    with sync_playwright() as p:
        context = None
        browser = None
        if user_data_dir:
            Path(user_data_dir).mkdir(parents=True, exist_ok=True)
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                user_agent=user_agent,
            )
        else:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(user_agent=user_agent)

        try:
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1500)

            title = ""
            html_text = ""
            try:
                title = page.title()
            except Exception:
                title = ""
            try:
                html_text = page.content()
            except Exception:
                html_text = ""

            if _looks_like_cloudflare_challenge(title, html_text):
                if headless:
                    raise RuntimeError(
                        "Cloudflare challenge detected in headless browser. "
                        "Re-run with headless=false to solve it once interactively."
                    )

                print(
                    "Playwright opened a browser and hit a Cloudflare challenge. "
                    "Solve the captcha/verification in the visible browser window, "
                    "then press Enter here to continue..."
                )
                input()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(1500)

            cookie_list = context.cookies()
        finally:
            try:
                context.close()
            except Exception:
                pass
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass

    cookie_map: Dict[str, str] = {}
    for cookie in cookie_list:
        name = cookie.get("name")
        value = cookie.get("value")
        if isinstance(name, str) and name and isinstance(value, str):
            cookie_map[name] = value

    header = build_cookie_header(cookie_map)
    if not header:
        raise RuntimeError("Playwright returned no cookies.")
    return header

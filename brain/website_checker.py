import re
from typing import Dict, Optional

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class WebsiteChecker:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def check_url(self, url: str) -> Dict[str, Optional[object]]:
        if not url:
            return {
                "url": None,
                "status": "missing",
                "http_status": None,
                "wordpress": False,
                "outdated": False,
                "reason": "No URL was provided.",
            }

        normalized = self._normalize_url(url)
        try:
            response = self.session.get(normalized, timeout=self.timeout)
            status_code = response.status_code
            body = response.text or ""
            is_wordpress = self._detect_wordpress(body)
            is_outdated = self._detect_outdated(body)
            status = self._resolve_status(status_code)
            return {
                "url": normalized,
                "status": status,
                "http_status": status_code,
                "wordpress": is_wordpress,
                "outdated": is_outdated,
                "reason": self._build_reason(status, is_wordpress, is_outdated),
            }
        except requests.RequestException as exc:
            return {
                "url": normalized,
                "status": "dead",
                "http_status": None,
                "wordpress": False,
                "outdated": False,
                "reason": f"Request failed: {exc}",
            }

    @staticmethod
    def _normalize_url(url: str) -> str:
        candidate = url.strip()
        if not candidate.startswith(("http://", "https://")):
            candidate = "https://" + candidate
        return candidate

    @staticmethod
    def _resolve_status(status_code: int) -> str:
        if status_code == 200:
            return "live"
        if status_code in {404, 500, 502, 503, 504}:
            return "dead"
        return "unknown"

    @staticmethod
    def _detect_wordpress(html: str) -> bool:
        lowered = html.lower()
        return "wp-content" in lowered or "wordpress" in lowered

    @staticmethod
    def _detect_outdated(html: str) -> bool:
        years = re.findall(r"(20\d{2})", html)
        for token in years:
            try:
                year = int(token)
            except ValueError:
                continue
            if year < 2023:
                return True
        return False

    @staticmethod
    def _build_reason(status: str, wordpress: bool, outdated: bool) -> str:
        parts = []
        if status == "live":
            parts.append("Site is reachable.")
        elif status == "dead":
            parts.append("URL returned a dead status.")
        else:
            parts.append("Site status could not be determined.")
        if wordpress:
            parts.append("Detected WordPress signals.")
        if outdated:
            parts.append("Site appears outdated.")
        return " ".join(parts)

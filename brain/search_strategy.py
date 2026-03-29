import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class SearchResult:
    title: str
    url: str
    source: str


class SearchStrategy:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def find_leads(self, country: str, city: str, category: str) -> Dict[str, Any]:
        query = f"{category} in {city}, {country}"
        primary = self.search_google_maps(query)
        strategy = ["Google Maps"]
        secondary: List[SearchResult] = []
        tertiary: List[SearchResult] = []

        if len(primary) < 10:
            secondary = self.search_google_search(query)
            strategy.append("Google Search")

        if len(primary) + len(secondary) < 10:
            tertiary = self.search_bing_places(query)
            strategy.append("Bing Places")

        combined = self._unique_results(primary + secondary + tertiary)

        return {
            "query": query,
            "strategy": strategy,
            "primary": [result.__dict__ for result in primary],
            "secondary": [result.__dict__ for result in secondary],
            "tertiary": [result.__dict__ for result in tertiary],
            "results": [result.__dict__ for result in combined],
            "count": len(combined),
        }

    def search_google_maps(self, query: str) -> List[SearchResult]:
        url = f"https://www.google.com/maps/search/{quote_plus(query)}"
        html = self._fetch(url)
        if not html:
            return []
        return self._parse_google_maps(html, base_url=url)

    def search_google_search(self, query: str) -> List[SearchResult]:
        url = f"https://www.google.com/search?q={quote_plus(query)}"
        html = self._fetch(url)
        if not html:
            return []
        return self._parse_search_results(html, "Google Search")

    def search_bing_places(self, query: str) -> List[SearchResult]:
        url = f"https://www.bing.com/search?q={quote_plus(query + ' site:bing.com/places')}"
        html = self._fetch(url)
        if not html:
            return []
        return self._parse_search_results(html, "Bing Places")

    def _fetch(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            return None

    def _parse_google_maps(self, html: str, base_url: str) -> List[SearchResult]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[SearchResult] = []

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            title = anchor.get_text(strip=True)
            if not title or len(title) < 2:
                continue
            if "/place/" in href or "/search/" in href:
                full_url = urljoin(base_url, href)
                results.append(SearchResult(title=title, url=full_url, source="Google Maps"))

        if not results:
            matches = re.findall(r'"name":"([^"]+)"', html)
            for idx, title in enumerate(matches[:10]):
                results.append(SearchResult(title=title, url=base_url, source="Google Maps"))

        return self._unique_results(results)

    def _parse_search_results(self, html: str, source: str) -> List[SearchResult]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[SearchResult] = []

        for anchor in soup.select("a[href]"):
            href = anchor["href"]
            title = anchor.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            if source == "Google Search" and href.startswith("/url?"):
                link = self._extract_google_redirect(href)
            else:
                link = href
            if link and link.startswith("http"):
                results.append(SearchResult(title=title, url=link, source=source))

        return self._unique_results(results)

    @staticmethod
    def _extract_google_redirect(url: str) -> Optional[str]:
        match = re.search(r"/url\?q=([^&]+)", url)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _unique_results(results: List[SearchResult]) -> List[SearchResult]:
        seen = set()
        unique: List[SearchResult] = []
        for result in results:
            key = (result.url, result.title)
            if key in seen:
                continue
            seen.add(key)
            unique.append(result)
        return unique

    def export_plan(self, country: str, city: str, category: str) -> str:
        plan = self.find_leads(country, city, category)
        return json.dumps(plan, indent=2)

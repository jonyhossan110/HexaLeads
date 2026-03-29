import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, List, Optional
from urllib.parse import parse_qs, quote_plus, urlparse, urljoin

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
    name: str
    url: str
    source: str
    address: Optional[str] = None
    snippet: Optional[str] = None


class MultiSearchScraper:
    def __init__(self, delay: float = 2.0, timeout: int = 10):
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def search(self, country: str, city: str, category: str) -> Dict[str, object]:
        query = f"{category} in {city} {country}".strip()
        primary = self.search_google(query)
        secondary: List[SearchResult] = []
        tertiary: List[SearchResult] = []
        sources = ["Google Search"]

        if len(primary) < 10:
            secondary = self.search_bing(query)
            sources.append("Bing Search")

        if len(primary) + len(secondary) < 10:
            tertiary = self.search_duckduckgo(query)
            sources.append("DuckDuckGo")

        combined = self._merge_results(primary + secondary + tertiary)
        return {
            "query": query,
            "sources": sources,
            "primary": [result.__dict__ for result in primary],
            "secondary": [result.__dict__ for result in secondary],
            "tertiary": [result.__dict__ for result in tertiary],
            "results": [result.__dict__ for result in combined],
            "count": len(combined),
        }

    def search_google(self, query: str) -> List[SearchResult]:
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=20"
        html = self._fetch_html(url)
        results: List[SearchResult] = []
        if not html:
            return results

        soup = BeautifulSoup(html, "html.parser")
        for result in soup.select("div.g"):
            title_elem = result.select_one("h3")
            link_elem = result.select_one("a[href]")
            snippet_elem = result.select_one("span.aCOpRe") or result.select_one("div.IsZvec")
            if not title_elem or not link_elem:
                continue
            raw_url = link_elem["href"]
            url = self._clean_google_url(raw_url)
            if not url:
                continue
            title = title_elem.get_text(strip=True)
            snippet = snippet_elem.get_text(strip=True) if snippet_elem else None
            address = self._extract_address(snippet)
            results.append(SearchResult(name=title, url=url, source="Google", address=address, snippet=snippet))

        self._sleep_rate_limit()
        return results

    def search_bing(self, query: str) -> List[SearchResult]:
        url = f"https://www.bing.com/search?q={quote_plus(query)}&count=20"
        html = self._fetch_html(url)
        results: List[SearchResult] = []
        if not html:
            return results

        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select("li.b_algo"):
            title_elem = item.select_one("h2")
            link_elem = title_elem.select_one("a[href]") if title_elem else None
            snippet_elem = item.select_one("p")
            if not title_elem or not link_elem:
                continue
            url = link_elem["href"].strip()
            title = title_elem.get_text(strip=True)
            snippet = snippet_elem.get_text(strip=True) if snippet_elem else None
            address = self._extract_address(snippet)
            results.append(SearchResult(name=title, url=url, source="Bing", address=address, snippet=snippet))

        self._sleep_rate_limit()
        return results

    def search_duckduckgo(self, query: str) -> List[SearchResult]:
        url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        html = self._fetch_html(url)
        results: List[SearchResult] = []
        if not html:
            return results

        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select("div.result"):
            link_elem = item.select_one("a.result__a[href]")
            title = link_elem.get_text(strip=True) if link_elem else None
            raw_url = link_elem["href"] if link_elem else None
            snippet_elem = item.select_one("a.result__snippet") or item.select_one("div.result__snippet")
            if not title or not raw_url:
                continue
            url = raw_url.strip()
            snippet = snippet_elem.get_text(strip=True) if snippet_elem else None
            address = self._extract_address(snippet)
            results.append(SearchResult(name=title, url=url, source="DuckDuckGo", address=address, snippet=snippet))

        self._sleep_rate_limit()
        return results

    def _fetch_html(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            return None

    def _sleep_rate_limit(self) -> None:
        time.sleep(self.delay)

    @staticmethod
    def _clean_google_url(raw_url: str) -> Optional[str]:
        if raw_url.startswith("/url?"):
            parsed = urlparse(raw_url)
            params = parse_qs(parsed.query)
            target = params.get("q")
            if target:
                return target[0]
            return None
        if raw_url.startswith("http"):
            return raw_url
        return None

    @staticmethod
    def _extract_address(text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        address_candidates = re.findall(
            r"\b(?:\d+\s+\w+|Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Lane|Ln\.?|Boulevard|Blvd\.?|Dhaka|Bangladesh)\b[\w\s,.-]{0,80}",
            text,
            flags=re.IGNORECASE,
        )
        if address_candidates:
            return address_candidates[0].strip()
        return None

    def _merge_results(self, results: List[SearchResult]) -> List[SearchResult]:
        merged: List[SearchResult] = []
        seen_names = []

        for result in sorted(results, key=lambda item: (0 if item.address else 1, item.source)):
            normalized_name = self._normalize_text(result.name)
            existing = self._find_similar(normalized_name, seen_names)
            if existing is not None:
                existing_result = merged[existing]
                if not existing_result.address and result.address:
                    merged[existing] = result
                continue
            seen_names.append(normalized_name)
            merged.append(result)

        return merged

    @staticmethod
    def _normalize_text(text: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]", " ", text.lower())
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def _find_similar(self, name: str, candidates: List[str]) -> Optional[int]:
        for index, candidate in enumerate(candidates):
            if self._similarity(name, candidate) > 0.82:
                return index
        return None


if __name__ == "__main__":
    scraper = MultiSearchScraper()
    payload = scraper.search("Bangladesh", "Dhaka", "restaurants")
    print(f"Found {payload['count']} unique businesses")
    for entry in payload["results"][:20]:
        print(f"- {entry['name']} ({entry['source']}) {entry.get('address') or ''}")

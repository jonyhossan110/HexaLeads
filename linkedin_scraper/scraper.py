"""
linkedin_scraper/scraper.py

Discover decision makers for target businesses without using the LinkedIn API.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from database.db import DB_PATH, get_all_leads, update_lead

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY", "").strip()
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "").strip()
GOOGLE_CSE_ID = os.environ.get("GOOGLE_CSE_ID", "").strip()
ROCKETREACH_API_KEY = os.environ.get("ROCKETREACH_API_KEY", "").strip()
CACHE_DIR = ROOT_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_PATH = CACHE_DIR / "linkedin_cache.json"
SEARCH_DELAY_SECONDS = 2

logger = logging.getLogger("linkedin_scraper")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

TITLE_KEYWORDS = ["cto", "it manager", "founder", "owner", "chief technology officer"]
SEARCH_ROLES = "(CTO OR \"IT Manager\" OR Founder OR Owner)"


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_lower(value: Any) -> str:
    return _normalize_text(value).lower()


def _cache_key(company_name: str, domain: str) -> str:
    return f"{_normalize_lower(company_name)}|{_normalize_lower(domain)}"


def _load_cache() -> Dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except Exception:
        return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    try:
        with CACHE_PATH.open("w", encoding="utf-8") as stream:
            json.dump(cache, stream, indent=2)
    except Exception as exc:
        logger.warning("Failed to write LinkedIn search cache: %s", exc)


def _sleep() -> None:
    time.sleep(SEARCH_DELAY_SECONDS)


def _extract_name_title(text: str) -> Dict[str, str]:
    text = _normalize_text(text)
    if not text:
        return {"name": "", "title": ""}
    # Example: "John Doe - CTO at Example" or "Jane Smith | Founder"
    parts = re.split(r"[-|•–|\|]", text)
    if len(parts) >= 2:
        name = _normalize_text(parts[0])
        title = _normalize_text(parts[1])
        return {"name": name, "title": title}
    words = text.split()
    if len(words) <= 2:
        return {"name": text, "title": ""}
    if any(keyword in _normalize_lower(text) for keyword in TITLE_KEYWORDS):
        name_parts = []
        for word in words:
            if word.lower() in TITLE_KEYWORDS:
                break
            name_parts.append(word)
        return {"name": " ".join(name_parts).strip(), "title": text}
    return {"name": text, "title": ""}


def _parse_serpapi_result(item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    link = _normalize_text(item.get("link") or item.get("url"))
    if "linkedin.com/in" not in link.lower():
        return None
    title = _normalize_text(item.get("title") or item.get("headline") or "")
    snippet = _normalize_text(item.get("snippet") or item.get("description") or "")
    name_title = _extract_name_title(title or snippet)
    return {
        "name": name_title["name"],
        "title": name_title["title"],
        "linkedin_url": link,
        "source": "serpapi",
    }


def _search_serpapi(company_name: str) -> List[Dict[str, str]]:
    if not SERPAPI_API_KEY:
        return []
    query = f'site:linkedin.com/in "{company_name}" {SEARCH_ROLES}'
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_API_KEY,
        "gl": "us",
        "hl": "en",
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        results: List[Dict[str, str]] = []
        for item in data.get("organic_results", []):
            parsed = _parse_serpapi_result(item)
            if parsed:
                results.append(parsed)
        return results
    except Exception as exc:
        logger.warning("SerpAPI search failed for %s: %s", company_name, exc)
        return []
    finally:
        _sleep()


def _search_google(custom_query: str) -> List[Dict[str, str]]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": custom_query,
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        results: List[Dict[str, str]] = []
        for item in data.get("items", []):
            link = _normalize_text(item.get("link"))
            if "linkedin.com/in" not in link.lower():
                continue
            title = _normalize_text(item.get("title") or "")
            snippet = _normalize_text(item.get("snippet") or "")
            parsed = _extract_name_title(title or snippet)
            results.append({
                "name": parsed["name"],
                "title": parsed["title"],
                "linkedin_url": link,
                "source": "google_cse",
            })
        return results
    except Exception as exc:
        logger.warning("Google custom search failed for %s: %s", custom_query, exc)
        return []
    finally:
        _sleep()


def _rocketreach_lookup(domain: str) -> List[Dict[str, str]]:
    if not ROCKETREACH_API_KEY:
        return []
    url = "https://api.rocketreach.co/v2/search" if "api.rocketreach.co" in ROCKETREACH_API_KEY else "https://api.rocketreach.co/v2/search"
    params = {"api_key": ROCKETREACH_API_KEY, "company_domain": domain}
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        results: List[Dict[str, str]] = []
        for item in data.get("data", []) or data.get("people", []):
            name = _normalize_text(item.get("name") or item.get("full_name") or "")
            title = _normalize_text(item.get("title") or item.get("job_title") or "")
            linkedin_url = _normalize_text(item.get("linkedin_url") or item.get("linkedin") or "")
            if not linkedin_url:
                continue
            results.append({
                "name": name,
                "title": title,
                "linkedin_url": linkedin_url,
                "email": _normalize_text(item.get("email") or item.get("work_email") or item.get("business_email") or ""),
                "source": "rocketreach",
            })
        return results
    except Exception as exc:
        logger.warning("RocketReach lookup failed for %s: %s", domain, exc)
        return []
    finally:
        _sleep()


def _fetch_html(url: str) -> str:
    try:
        response = requests.get(url, headers={"User-Agent": "HexaLeads/1.0"}, timeout=12)
        response.raise_for_status()
        return response.text
    except Exception as exc:
        logger.debug("Failed to fetch %s: %s", url, exc)
        return ""


def _extract_candidates_from_html(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[Dict[str, str]] = []

    for anchor in soup.select("a[href*"]"):
        href = anchor.get("href", "")
        if "linkedin.com/in" in href.lower():
            name = _normalize_text(anchor.text)
            title = ""
            if not name:
                parent = anchor.find_parent()
                if parent is not None:
                    name = _normalize_text(parent.text)
            candidates.append({
                "name": name,
                "title": title,
                "linkedin_url": href.split("?")[0],
                "source": "website_scrape",
            })

    text = soup.get_text(separator=" \n")
    for match in re.finditer(r"([A-Z][a-z]+(?: [A-Z][a-z]+){0,2})[\s,\-\n]{1,10}(CTO|IT Manager|Founder|Owner)", text, re.IGNORECASE):
        name = _normalize_text(match.group(1))
        title = _normalize_text(match.group(2))
        candidates.append({
            "name": name,
            "title": title,
            "linkedin_url": "",
            "source": "website_scrape",
        })
    return candidates


def _scrape_website_for_profiles(domain: str) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    host = domain.lower().lstrip("https://").lstrip("http://").lstrip("www.").split("/")[0]
    paths = ["/about", "/about-us", "/team", "/our-team", "/team-members"]
    for path in paths:
        url = f"https://{host}{path}"
        html = _fetch_html(url)
        if not html:
            url = f"http://{host}{path}"
            html = _fetch_html(url)
        if not html:
            continue
        candidates.extend(_extract_candidates_from_html(html, url))
        if candidates:
            break
    _sleep()
    return candidates


def _select_best_candidate(candidates: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not candidates:
        return None
    # Prefer candidates with LinkedIn URL and title keywords.
    def sort_key(item: Dict[str, str]) -> int:
        score = 0
        if item.get("linkedin_url"):
            score += 30
        if item.get("email"):
            score += 20
        title = _normalize_lower(item.get("title"))
        if any(role in title for role in TITLE_KEYWORDS):
            score += 20
        if item.get("source") == "rocketreach":
            score += 10
        return score

    sorted_candidates = sorted(candidates, key=sort_key, reverse=True)
    return sorted_candidates[0]


def _fetch_domain(company_name: str, domain: str) -> str:
    candidate = _normalize_text(domain)
    if candidate:
        return candidate
    return re.sub(r"^https?://", "", _normalize_text(company_name)).split("/", 1)[0]


def _normalize_domain(domain: str) -> str:
    domain = _normalize_text(domain)
    if domain.startswith("http://") or domain.startswith("https://"):
        domain = re.sub(r"^https?://", "", domain)
    domain = domain.split("/")[0]
    return domain.lstrip("www.")


def _load_email_finder() -> Optional[Any]:
    try:
        from email_finder.finder import find_email

        return find_email
    except Exception as exc:
        logger.warning("Email finder not available: %s", exc)
        return None


def _compute_confidence(candidate: Dict[str, str]) -> int:
    confidence = 20
    if candidate.get("linkedin_url"):
        confidence += 30
    if candidate.get("title") and any(role in _normalize_lower(candidate.get("title")) for role in TITLE_KEYWORDS):
        confidence += 25
    if candidate.get("email"):
        confidence += 25
    return min(confidence, 100)


def find_decision_maker(company_name: str, domain: str) -> Dict[str, Any]:
    if not company_name and not domain:
        raise ValueError("Company name or domain is required.")

    company_key = _cache_key(company_name, domain)
    cache = _load_cache()
    if company_key in cache:
        logger.info("LinkedIn search cache hit for %s", company_name)
        return cache[company_key]

    results: List[Dict[str, str]] = []
    if SERPAPI_API_KEY:
        results.extend(_search_serpapi(company_name))
    if not results and GOOGLE_API_KEY and GOOGLE_CSE_ID:
        results.extend(_search_google(f'site:linkedin.com/in "{company_name}" {SEARCH_ROLES}'))
    if not results and ROCKETREACH_API_KEY:
        results.extend(_rocketreach_lookup(_normalize_domain(domain or company_name)))
    if not results:
        results.extend(_scrape_website_for_profiles(domain or company_name))

    candidate = _select_best_candidate(results) if results else None
    if not candidate:
        logger.info("No decision maker found for %s", company_name)
        result = {
            "name": "",
            "title": "",
            "linkedin_url": "",
            "email": "",
            "confidence_score": 0,
            "source": "none",
        }
        cache[company_key] = result
        _save_cache(cache)
        return result

    email_finder = _load_email_finder()
    if not candidate.get("email") and email_finder:
        try:
            domain_name = _normalize_domain(domain or company_name)
            email_result = email_finder(domain_name)
            candidate["email"] = _normalize_text(email_result.get("email"))
        except Exception as exc:
            logger.warning("Email finder failed during LinkedIn enrichment: %s", exc)

    candidate_result = {
        "name": candidate.get("name", ""),
        "title": candidate.get("title", ""),
        "linkedin_url": candidate.get("linkedin_url", ""),
        "email": candidate.get("email", ""),
        "confidence_score": _compute_confidence(candidate),
        "source": candidate.get("source", "unknown"),
    }
    cache[company_key] = candidate_result
    _save_cache(cache)
    logger.info("Decision maker found for %s: %s", company_name, candidate_result)
    return candidate_result


def batch_find(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for lead in leads:
        company_name = _normalize_text(lead.get("business_name") or lead.get("name") or "")
        domain = _normalize_text(lead.get("website") or lead.get("domain") or "")
        try:
            result = find_decision_maker(company_name, domain)
            if lead.get("id") is not None and result["name"]:
                enrich_lead(int(lead["id"]))
            results.append({"lead_id": lead.get("id"), **result})
        except Exception as exc:
            logger.warning("Decision maker search failed for lead %s: %s", lead.get("id"), exc)
            results.append({"lead_id": lead.get("id"), "name": "", "title": "", "linkedin_url": "", "email": "", "confidence_score": 0})
    return results


def enrich_lead(lead_id: int) -> bool:
    lead = None
    for row in get_all_leads():
        if int(row["id"]) == lead_id:
            lead = {key: row[key] for key in row.keys()}
            break
    if not lead:
        logger.warning("Lead %s not found for enrichment.", lead_id)
        return False

    company_name = _normalize_text(lead.get("business_name") or lead.get("name") or "")
    domain = _normalize_text(lead.get("website") or lead.get("domain") or "")
    if not company_name and not domain:
        logger.warning("Lead %s has no company name or domain.", lead_id)
        return False

    decision_maker = find_decision_maker(company_name, domain)
    if not decision_maker["name"]:
        return False

    update_fields: Dict[str, Any] = {
        "contact_name": decision_maker["name"],
        "contact_title": decision_maker["title"],
        "contact_linkedin": decision_maker["linkedin_url"],
    }
    if decision_maker["email"]:
        update_fields["email"] = decision_maker["email"]

    try:
        update_lead(lead_id=lead_id, **update_fields)
        logger.info("Enriched lead %s with decision maker data.", lead_id)
        return True
    except Exception as exc:
        logger.warning("Failed to update lead %s: %s", lead_id, exc)
        return False


if __name__ == "__main__":
    sample = find_decision_maker("Example Company", "example.com")
    print(json.dumps(sample, indent=2))

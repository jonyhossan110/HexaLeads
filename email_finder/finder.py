"""
email_finder/finder.py

Find and verify professional lead email addresses.
"""

from __future__ import annotations

import logging
import os
import re
import smtplib
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv

from database.db import update_lead

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "").strip()
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "").strip()
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
API_DELAY_SECONDS = 1

logger = logging.getLogger("email_finder.finder")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

POSITION_PRIORITIES = ["ceo", "cto", "founder", "it manager", "owner"]
EMAIL_FILTER_SKIP = ["noreply@", "no-reply@", "info@"]
EMAIL_PREFERRED = ["ceo@", "founder@", "owner@", "admin@", "contact@", "hello@"]
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


def _normalize_domain(target: str) -> str:
    target = str(target or "").strip().lower()
    if not target:
        raise ValueError("Domain is required")
    target = re.sub(r"^https?://", "", target)
    target = target.split("/")[0]
    target = target.lstrip("www.")
    return target


def _sleep_between_api_calls() -> None:
    time.sleep(API_DELAY_SECONDS)


def _http_get(url: str) -> Optional[requests.Response]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        return response
    except requests.RequestException as exc:
        logger.warning("HTTP request failed for %s: %s", url, exc)
        return None


def _parse_hunter_email_results(data: dict[str, Any]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for item in data.get("data", {}).get("emails", []):
        email = _normalize_email(item.get("value"))
        if not email:
            continue
        name_parts: List[str] = []
        if item.get("first_name"):
            name_parts.append(str(item.get("first_name")).strip())
        if item.get("last_name"):
            name_parts.append(str(item.get("last_name")).strip())
        name = " ".join(name_parts).strip()
        results.append(
            {
                "email": email,
                "name": name,
                "position": _normalize_text(item.get("position")),
                "source": "hunter.io",
            }
        )
    return results


def _parse_apollo_email_results(data: dict[str, Any]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    organizations = data.get("organizations") or data.get("data") or []
    if not isinstance(organizations, list):
        organizations = [organizations]
    for org in organizations:
        contacts = org.get("contacts") or org.get("people") or org.get("data") or []
        if isinstance(contacts, dict):
            contacts = [contacts]
        for person in contacts:
            email = _normalize_email(person.get("email") or person.get("work_email") or person.get("business_email"))
            if not email:
                continue
            name = _normalize_text(person.get("name") or person.get("full_name") or person.get("first_name") + " " + person.get("last_name"))
            position = _normalize_text(person.get("title") or person.get("job_title") or person.get("position"))
            results.append(
                {
                    "email": email,
                    "name": name,
                    "position": position,
                    "source": "apollo.io",
                }
            )
    return results


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_email(value: Any) -> Optional[str]:
    value = _normalize_text(value).lower()
    if not value or "@" not in value:
        return None
    return value


def _candidate_sort_key(candidate: Dict[str, str]) -> int:
    position = _normalize_lower(candidate.get("position"))
    for index, title in enumerate(POSITION_PRIORITIES):
        if title in position:
            return index
    local = candidate.get("email", "").split("@", 1)[0]
    for index, pattern in enumerate(EMAIL_PREFERRED):
        if local == pattern.split("@", 1)[0]:
            return len(POSITION_PRIORITIES) + index
    return len(POSITION_PRIORITIES) + len(EMAIL_PREFERRED)


def _normalize_lower(value: Any) -> str:
    return _normalize_text(value).lower()


def _pick_best_candidate(candidates: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    unique: dict[str, Dict[str, str]] = {}
    for candidate in candidates:
        email = candidate.get("email")
        if not email:
            continue
        unique[email] = candidate
    if not unique:
        return None
    sorted_candidates = sorted(unique.values(), key=_candidate_sort_key)
    return sorted_candidates[0]


def _hunter_lookup(domain: str) -> List[Dict[str, str]]:
    if not HUNTER_API_KEY:
        logger.info("Skipping Hunter.io lookup because HUNTER_API_KEY is not configured.")
        return []

    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": HUNTER_API_KEY}
    response = _http_get(url=url)
    _sleep_between_api_calls()
    if not response or response.status_code != 200:
        logger.warning("Hunter.io returned no valid result for %s", domain)
        return []

    try:
        data = response.json()
    except ValueError:
        logger.warning("Hunter.io response could not be parsed for %s", domain)
        return []

    return _parse_hunter_email_results(data)


def _apollo_lookup(domain: str) -> List[Dict[str, str]]:
    if not APOLLO_API_KEY:
        logger.info("Skipping Apollo lookup because APOLLO_API_KEY is not configured.")
        return []

    url = "https://api.apollo.io/v1/organizations/search"
    params = {"domain": domain, "api_key": APOLLO_API_KEY}
    response = _http_get(url=url)
    _sleep_between_api_calls()
    if not response or response.status_code != 200:
        logger.warning("Apollo returned no valid result for %s", domain)
        return []

    try:
        data = response.json()
    except ValueError:
        logger.warning("Apollo response could not be parsed for %s", domain)
        return []

    return _parse_apollo_email_results(data)


def _extract_emails_from_html(html: str) -> List[str]:
    return [email.lower() for email in set(EMAIL_REGEX.findall(html))]


def _filter_scanned_emails(emails: List[str]) -> List[str]:
    filtered: List[str] = []
    for email in emails:
        if any(token in email for token in EMAIL_FILTER_SKIP):
            continue
        filtered.append(email)
    return filtered


def _scrape_contact_page(domain: str) -> List[Dict[str, str]]:
    candidates: List[Dict[str, str]] = []
    paths = ["/contact", "/contact-us"]
    for path in paths:
        url = f"https://{domain}{path}"
        response = _http_get(url)
        if response and response.status_code == 200:
            emails = _extract_emails_from_html(response.text)
            for email in _filter_scanned_emails(emails):
                candidates.append(
                    {
                        "email": email,
                        "name": "",
                        "position": "",
                        "source": "scrape",
                    }
                )
            if candidates:
                break
    if not candidates:
        logger.info("No contact page emails found for %s", domain)
    return candidates


def _verify_email_smtp(email: str) -> bool:
    try:
        domain = email.split("@", 1)[1]
    except IndexError:
        logger.warning("Invalid email format for verification: %s", email)
        return False

    try:
        import dns.resolver
    except ImportError:
        logger.warning("dnspython is not installed; SMTP verification skipped.")
        return False

    try:
        answers = dns.resolver.resolve(domain, "MX")
    except Exception as exc:
        logger.warning("MX lookup failed for %s: %s", domain, exc)
        return False

    mx_hosts = sorted(((r.preference, str(r.exchange).rstrip(".")) for r in answers), key=lambda item: item[0])
    sender = f"verify@{socket.gethostname() or 'example.com'}"

    for _, mx_host in mx_hosts:
        try:
            server = smtplib.SMTP(mx_host, 25, timeout=10)
            server.ehlo_or_helo_if_needed()
            server.mail(sender)
            code, _ = server.rcpt(email)
            server.quit()
            if code in {250, 251}:
                return True
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, smtplib.SMTPRecipientsRefused, smtplib.SMTPResponseException, socket.error) as exc:
            logger.debug("SMTP verify failed on %s for %s: %s", mx_host, email, exc)
            continue
    return False


def _save_email_to_lead(lead_id: int, email: str) -> None:
    try:
        update_lead(lead_id=lead_id, email=email)
    except Exception as exc:
        logger.warning("Failed to save email for lead %s: %s", lead_id, exc)


def find_email(domain: str) -> Dict[str, Any]:
    normalized = _normalize_domain(domain)
    candidates: List[Dict[str, str]] = []

    hunter_candidates = _hunter_lookup(normalized)
    if hunter_candidates:
        logger.info("Hunter.io returned %d candidate(s) for %s", len(hunter_candidates), normalized)
        candidates.extend(hunter_candidates)
    else:
        logger.info("Hunter.io found no candidates for %s", normalized)

    if not candidates:
        apollo_candidates = _apollo_lookup(normalized)
        if apollo_candidates:
            logger.info("Apollo returned %d candidate(s) for %s", len(apollo_candidates), normalized)
            candidates.extend(apollo_candidates)
        else:
            logger.info("Apollo found no candidates for %s", normalized)

    if not candidates:
        scrape_candidates = _scrape_contact_page(normalized)
        if scrape_candidates:
            logger.info("Scraped %d candidate(s) from %s/contact", len(scrape_candidates), normalized)
            candidates.extend(scrape_candidates)
        else:
            logger.info("No scrape candidates found for %s", normalized)

    best = _pick_best_candidate(candidates)
    if not best:
        logger.info("No email candidates found for %s", normalized)
        return {
            "email": "",
            "name": "",
            "position": "",
            "source": "none",
            "verified": False,
        }

    verified = verify_email(best["email"])
    logger.info("Selected email %s for %s (verified=%s)", best["email"], normalized, verified)
    return {
        "email": best["email"],
        "name": best.get("name", ""),
        "position": best.get("position", ""),
        "source": best.get("source", "unknown"),
        "verified": verified,
    }


def batch_find(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for lead in leads:
        domain = _normalize_domain(lead.get("domain") or lead.get("website") or "")
        lead_id = lead.get("id")
        logger.info("Finding email for lead %s (%s)", lead_id or "unknown", domain)
        result = find_email(domain)
        if lead_id and result["email"]:
            _save_email_to_lead(int(lead_id), result["email"])
        results.append({"lead_id": lead_id, "domain": domain, **result})
    return results


def verify_email(email: str) -> bool:
    return _verify_email_smtp(email)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads email finder")
    parser.add_argument("domain", help="Website domain to search emails for")
    args = parser.parse_args()
    result = find_email(args.domain)
    print(result)

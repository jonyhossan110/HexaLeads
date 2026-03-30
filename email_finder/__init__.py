from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from utils.logger import get_logger

logger = get_logger("email_finder")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")

EMAIL_PATTERNS = [
    "contact@{domain}",
    "hello@{domain}",
    "info@{domain}",
    "support@{domain}",
    "admin@{domain}",
]


def _normalize_domain(target: str) -> str:
    target = str(target or "").strip().lower()
    target = re.sub(r"^https?://", "", target)
    target = target.split("/")[0]
    target = target.lstrip("www.")
    if not target:
        raise ValueError("Domain is required.")
    return target


async def _hunter_lookup(domain: str) -> List[str]:
    if not HUNTER_API_KEY:
        return []

    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": HUNTER_API_KEY}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        try:
            async with session.get(url, params=params) as response:
                data = await response.json()
                emails = [item.get("value") for item in data.get("data", {}).get("emails", []) if item.get("value")]
                return list(dict.fromkeys(emails))
        except Exception as exc:  # pragma: no cover
            logger.warning("Hunter.io lookup failed for %s: %s", domain, exc)
            return []


async def _apollo_lookup(domain: str) -> List[str]:
    if not APOLLO_API_KEY:
        return []

    url = "https://api.apollo.io/v1/people/find"
    params = {"api_key": APOLLO_API_KEY, "email_domain": domain}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        try:
            async with session.get(url, params=params) as response:
                data = await response.json()
                emails = []
                for person in data.get("people", []):
                    email = person.get("email")
                    if email:
                        emails.append(email)
                return list(dict.fromkeys(emails))
        except Exception as exc:  # pragma: no cover
            logger.warning("Apollo lookup failed for %s: %s", domain, exc)
            return []


def _build_fallback_emails(domain: str) -> List[str]:
    return [pattern.format(domain=domain) for pattern in EMAIL_PATTERNS]


async def find_emails(domain: str) -> Dict[str, Any]:
    """Lookup candidate business emails for a website domain."""
    normalized = _normalize_domain(domain)
    result: Dict[str, Any] = {"domain": normalized, "emails": [], "sources": []}

    hunter_emails = await _hunter_lookup(normalized)
    if hunter_emails:
        result["emails"].extend(hunter_emails)
        result["sources"].append("hunter.io")

    apollo_emails = await _apollo_lookup(normalized)
    if apollo_emails:
        result["emails"].extend(apollo_emails)
        result["sources"].append("apollo.io")

    if not result["emails"]:
        fallback = _build_fallback_emails(normalized)
        result["emails"].extend(fallback)
        result["sources"].append("fallback")

    result["emails"] = list(dict.fromkeys(result["emails"]))
    return result


async def run(domain: str) -> Dict[str, Any]:
    """Entry point for the email_finder package."""
    result = await find_emails(domain)
    logger.info("Email finder result for %s: %s", domain, result["emails"])
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads email finder")
    parser.add_argument("domain", help="Website domain to search emails for")
    args = parser.parse_args()
    output = asyncio.run(run(args.domain))
    print(output)

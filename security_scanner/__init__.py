from __future__ import annotations

import asyncio
import os
import re
import ssl
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from database.db import insert_lead, insert_security_issue
from utils.logger import get_logger

logger = get_logger("security_scanner")
COMMON_PROBES = [
    "/wp-admin/",
    "/wp-login.php",
    "/xmlrpc.php",
    "/readme.html",
    "/admin/",
    "/.env",
    "/phpmyadmin/",
    "/robots.txt",
    "/.git/config",
]

SECURITY_HEADERS = {
    "x-frame-options": "X-Frame-Options",
    "x-content-type-options": "X-Content-Type-Options",
    "referrer-policy": "Referrer-Policy",
    "strict-transport-security": "Strict-Transport-Security",
    "content-security-policy": "Content-Security-Policy",
}


def _normalize_url(website: str) -> str:
    website = str(website or "").strip()
    if not website:
        raise ValueError("Website URL is required.")
    if not re.match(r"^[a-zA-Z]+://", website):
        website = f"https://{website}"
    parsed = urlparse(website)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid website URL: {website}")
    return parsed.geturl().rstrip("/")


def _build_probe_urls(base_url: str) -> List[str]:
    return [urljoin(base_url + "/", probe.lstrip("/")) for probe in COMMON_PROBES]


async def _fetch(session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
    result = {
        "url": url,
        "status": None,
        "headers": {},
        "text": "",
        "error": None,
    }
    try:
        async with session.get(url, ssl=False, timeout=10) as response:
            result["status"] = response.status
            result["headers"] = {k.lower(): v for k, v in response.headers.items()}
            result["text"] = await response.text(errors="ignore")
    except Exception as exc:
        result["error"] = str(exc)
    return result


def _extract_cms(html: str) -> Optional[str]:
    if "wordpress" in html.lower():
        return "WordPress"
    if "joomla" in html.lower():
        return "Joomla"
    if "drupal" in html.lower():
        return "Drupal"
    return None


def _build_issue(issue_type: str, severity: str, details: str) -> Dict[str, str]:
    return {"issue_type": issue_type, "severity": severity, "details": details}


async def scan_website(website: str) -> List[Dict[str, str]]:
    """Scan a website for common vulnerability signals and record findings."""
    base_url = _normalize_url(website)
    parsed = urlparse(base_url)
    issues: List[Dict[str, str]] = []

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": "HexaLeadsScanner/1.0"}) as session:
        home = await _fetch(session, base_url)
        if home["error"]:
            logger.warning("Website scan failed for %s: %s", base_url, home["error"])
            issues.append(_build_issue("unreachable_site", "high", f"Unable to reach {base_url}: {home['error']}"))
            return issues

        if parsed.scheme != "https":
            issues.append(_build_issue("missing_https", "critical", f"Website is not served over HTTPS: {base_url}"))

        if not home["headers"].get("strict-transport-security"):
            issues.append(_build_issue("missing_hsts", "high", "Missing Strict-Transport-Security header."))
        if not home["headers"].get("x-frame-options"):
            issues.append(_build_issue("missing_x_frame_options", "medium", "Missing X-Frame-Options header."))
        if not home["headers"].get("x-content-type-options"):
            issues.append(_build_issue("missing_x_content_type_options", "medium", "Missing X-Content-Type-Options header."))
        if not home["headers"].get("referrer-policy"):
            issues.append(_build_issue("missing_referrer_policy", "low", "Missing Referrer-Policy header."))
        if not home["headers"].get("content-security-policy"):
            issues.append(_build_issue("missing_csp", "low", "Missing Content-Security-Policy header."))

        cms = _extract_cms(home["text"])
        if cms:
            issues.append(_build_issue("detected_cms", "medium", f"Detected CMS: {cms}."))

        probe_urls = _build_probe_urls(base_url)
        fetch_tasks = [_fetch(session, url) for url in probe_urls]
        probe_results = await asyncio.gather(*fetch_tasks)
        for result in probe_results:
            if result["status"] == 200:
                if any(path in result["url"].lower() for path in ["/wp-admin", "/wp-login.php", "/xmlrpc.php"]):
                    issues.append(
                        _build_issue(
                            "exposed_admin_or_xmlrpc",
                            "high",
                            f"Accessible admin or XML-RPC endpoint: {result['url']}",
                        )
                    )
                elif result["url"].endswith("/readme.html"):
                    issues.append(
                        _build_issue(
                            "exposed_readme",
                            "medium",
                            f"Public readme page found: {result['url']}",
                        )
                    )
                elif result["url"].endswith("/.env") or result["url"].endswith("/.git/config"):
                    issues.append(
                        _build_issue(
                            "sensitive_file_exposed",
                            "critical",
                            f"Potential sensitive file accessible: {result['url']}",
                        )
                    )
            elif result["status"] in {403, 401}:
                logger.info("Probe %s returned %s", result["url"], result["status"])
            elif result["error"]:
                logger.debug("Probe error for %s: %s", result["url"], result["error"])

    if not issues:
        issues.append(_build_issue("no_critical_findings", "low", "No common security signals detected during scan."))
    return issues


async def run(website: str) -> Dict[str, Any]:
    """Entry point for the security_scanner package."""
    issues = await scan_website(website)
    lead_id = insert_lead(website=website, source="security_scan")
    if lead_id:
        for issue in issues:
            insert_security_issue(
                lead_id=lead_id,
                issue_type=issue["issue_type"],
                severity=issue["severity"],
                details=issue["details"],
            )
    return {"website": website, "issues": issues, "lead_id": lead_id}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads security scanner")
    parser.add_argument("website", help="Target website to scan")
    args = parser.parse_args()
    result = asyncio.run(run(args.website))
    print(result)

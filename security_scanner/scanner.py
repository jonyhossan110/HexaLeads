"""
security_scanner/scanner.py

Scan a website for common security issues and write findings to the HexaLeads SQLite database.
"""

from __future__ import annotations

import asyncio
import re
import ssl
import socket
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from requests.exceptions import RequestException

from database.db import get_lead_by_website, insert_lead, insert_security_issue

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 10
SECURITY_HEADERS = {
    "x-frame-options": ("X-Frame-Options", "medium"),
    "x-xss-protection": ("X-XSS-Protection", "medium"),
    "content-security-policy": ("Content-Security-Policy", "high"),
    "strict-transport-security": ("Strict-Transport-Security", "high"),
    "x-content-type-options": ("X-Content-Type-Options", "medium"),
}
ADMIN_PATHS = [
    "/admin",
    "/wp-admin",
    "/administrator",
    "/login",
    "/cpanel",
    "/phpmyadmin",
]
SENSITIVE_ROBOTS_PATTERNS = [
    "/admin",
    "/wp-admin",
    "/administrator",
    "/login",
    "/cpanel",
    "/phpmyadmin",
    "/.env",
    "/.git",
    "/config",
    "/backup",
    "/private",
]


def _normalize_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        raise ValueError("Website URL is required.")
    if not re.match(r"^[a-zA-Z]+://", url):
        url = f"https://{url}"
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid website URL: {url}")
    normalized = parsed._replace(path=parsed.path.rstrip("/"), params="", query="", fragment="").geturl()
    return normalized


def _build_issue(issue_type: str, severity: str, detail: str) -> dict[str, str]:
    return {"type": issue_type, "severity": severity, "detail": detail}


def _http_get(url: str, allow_redirects: bool = True) -> tuple[requests.Response | None, str | None]:
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=allow_redirects)
        return response, None
    except RequestException as exc:
        return None, str(exc)


def _parse_version(text: str) -> tuple[int, int] | None:
    match = re.search(r"Version\s*([0-9]+)(?:\.([0-9]+))?", text, re.IGNORECASE)
    if not match:
        return None
    major = int(match.group(1))
    minor = int(match.group(2) or "0")
    return major, minor


def _save_issues(website: str, issues: list[dict[str, str]]) -> None:
    lead_id = insert_lead(website=website, source="security_scanner")
    if not lead_id:
        row = get_lead_by_website(website)
        lead_id = row["id"] if row else None
    if not lead_id:
        return

    for issue in issues:
        insert_security_issue(
            lead_id=lead_id,
            issue_type=issue["type"],
            severity=issue["severity"],
            details=issue["detail"],
        )


def _check_ssl(website: str) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    parsed = urlparse(website)
    host = parsed.hostname
    if not host:
        return issues

    port = 443
    context = ssl.create_default_context()

    try:
        with socket.create_connection((host, port), timeout=REQUEST_TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=host) as ssl_sock:
                cert = ssl_sock.getpeercert()
    except ssl.SSLError as exc:
        issues.append(_build_issue("ssl_invalid", "critical", f"SSL certificate invalid for {host}: {exc}"))
        return issues
    except (OSError, socket.timeout) as exc:
        issues.append(_build_issue("ssl_connection_failed", "high", f"Unable to verify SSL certificate for {host}: {exc}"))
        return issues

    if not cert:
        issues.append(_build_issue("ssl_missing_certificate", "critical", f"No SSL certificate was returned by {host}."))
        return issues

    not_after = cert.get("notAfter")
    if not not_after:
        issues.append(_build_issue("ssl_missing_expiry", "high", f"SSL certificate expiry date missing for {host}."))
        return issues

    try:
        expires = datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            expires = datetime.strptime(not_after, "%Y%m%d%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            issues.append(_build_issue("ssl_unparsable_expiry", "high", f"Unable to parse SSL expiry date for {host}: {not_after}"))
            return issues

    now = datetime.now(timezone.utc)
    if expires < now:
        issues.append(_build_issue("ssl_expired", "critical", f"SSL certificate expired on {expires.date()} for {host}."))
        return issues

    remaining = expires - now
    if remaining <= timedelta(days=30):
        issues.append(_build_issue("ssl_expiring", "high", f"SSL certificate expires in {remaining.days} days for {host}."))

    return issues


def _check_security_headers(url: str, html: str | None) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    response, error = _http_get(url)
    if not response:
        issues.append(_build_issue("security_headers_unreachable", "high", f"Unable to fetch headers from {url}: {error}"))
        return issues

    headers = {key.lower(): value for key, value in response.headers.items()}
    for lower_name, (original_name, severity) in SECURITY_HEADERS.items():
        if lower_name not in headers:
            issues.append(_build_issue(f"missing_{original_name.lower().replace('-', '_')}", severity, f"Missing {original_name} header."))

    if response.headers.get("Server"):
        server_value = response.headers["Server"]
        if re.search(r"/[0-9]", server_value):
            issues.append(_build_issue("server_version_exposed", "medium", f"Server header exposes version: {server_value}"))

    if html and re.search(r"<meta[^>]+name=['\"]generator['\"][^>]+content=['\"]([^'\"]+)['\"]", html, re.IGNORECASE):
        generator = re.search(r"<meta[^>]+name=['\"]generator['\"][^>]+content=['\"]([^'\"]+)['\"]", html, re.IGNORECASE)
        if generator:
            cms_value = generator.group(1).strip()
            issues.append(_build_issue("generator_meta_exposed", "low", f"Generator meta tag reveals CMS: {cms_value}"))

    return issues


def _detect_cms(website: str, html: str | None) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if not html:
        return issues

    lower_html = html.lower()
    cms_name: str | None = None
    if "wp-content" in lower_html or "wp-includes" in lower_html or "wordpress" in lower_html:
        cms_name = "WordPress"
    elif "joomla" in lower_html or "/components/" in lower_html:
        cms_name = "Joomla"
    elif "drupal" in lower_html or "drupal.settings" in lower_html:
        cms_name = "Drupal"

    if cms_name:
        issues.append(_build_issue("detected_cms", "medium", f"Site appears to be running {cms_name}."))

        if cms_name == "WordPress":
            parsed = urlparse(website)
            base = f"{parsed.scheme}://{parsed.netloc}"
            readme_url = urljoin(base + "/", "readme.html")
            response, error = _http_get(readme_url)
            if response and response.status_code == 200:
                version_info = _parse_version(response.text)
                if version_info:
                    major, minor = version_info
                    if major < 6 or (major == 6 and minor < 2):
                        issues.append(_build_issue(
                            "outdated_wordpress",
                            "high",
                            f"WordPress version {major}.{minor} appears outdated and exposed via readme.html."))
                else:
                    issues.append(_build_issue(
                        "wordpress_version_exposed",
                        "medium",
                        "WordPress readme.html is publicly accessible and may expose version details."))

    return issues


def _check_exposed_admin(website: str) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    parsed = urlparse(website)
    base_https = f"https://{parsed.netloc}"
    base_http = f"http://{parsed.netloc}"

    for path in ADMIN_PATHS:
        candidate = urljoin(base_https + "/", path.lstrip("/"))
        response, _ = _http_get(candidate, allow_redirects=True)
        if response and response.status_code == 200:
            issues.append(_build_issue("exposed_admin_panel", "high", f"Admin panel is accessible at {candidate}."))
            break

    for path in ADMIN_PATHS:
        candidate = urljoin(base_http + "/", path.lstrip("/"))
        response, _ = _http_get(candidate, allow_redirects=False)
        if response and response.status_code == 200:
            issues.append(_build_issue(
                "insecure_admin_access",
                "critical",
                f"Admin endpoint is accessible over HTTP without redirect to HTTPS: {candidate}."))
            break

    return issues


def _check_robots_txt(website: str) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    parsed = urlparse(website)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    response, _ = _http_get(robots_url)
    if not response or response.status_code != 200:
        return issues

    sensitive_paths = []
    for line in response.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "disallow:" in line.lower():
            path = line.split(":", 1)[1].strip()
            for pattern in SENSITIVE_ROBOTS_PATTERNS:
                if path.lower().startswith(pattern):
                    sensitive_paths.append(path)
                    break

    if sensitive_paths:
        issues.append(_build_issue(
            "sensitive_robots_disclosure",
            "medium",
            f"robots.txt discloses sensitive paths: {', '.join(sensitive_paths)}."))

    return issues


def _check_http_to_https(website: str) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    parsed = urlparse(website)
    if parsed.scheme != "https":
        issues.append(_build_issue("missing_https", "critical", f"Site is not served over HTTPS: {website}."))
        return issues

    http_url = f"http://{parsed.netloc}"
    response, _ = _http_get(http_url, allow_redirects=False)
    if not response:
        issues.append(_build_issue(
            "http_service_unavailable",
            "medium",
            f"Unable to verify HTTP-to-HTTPS redirect on {http_url}."))
        return issues

    location = response.headers.get("Location", "")
    if response.status_code not in (301, 302) or not location.lower().startswith("https://"):
        issues.append(_build_issue(
            "http_not_redirect_to_https",
            "high",
            f"HTTP traffic does not redirect to HTTPS for {parsed.netloc}."))

    return issues


def _build_summary(website: str, issues: list[dict[str, str]]) -> str:
    if not issues:
        return "No security issues detected."

    severity_count = {"critical": 0, "high": 0, "medium": 0, "low": 0}
    for issue in issues:
        sev = issue["severity"].lower()
        if sev in severity_count:
            severity_count[sev] += 1
    top_detail = issues[0]["detail"] if issues else ""
    parts = []
    for label in ("critical", "high", "medium", "low"):
        count = severity_count[label]
        if count:
            parts.append(f"{count} {label}")

    return f"{', '.join(parts)} issues found. {top_detail}"


def _calculate_score(issues: list[dict[str, str]]) -> int:
    severity_weight = {
        "critical": 40,
        "high": 25,
        "medium": 15,
        "low": 7,
    }
    score = 0
    for issue in issues:
        score += severity_weight.get(issue["severity"].lower(), 10)
    return min(score, 100)


def scan_website(url: str) -> dict[str, object]:
    website = _normalize_url(url)
    scan_time = datetime.now(timezone.utc).isoformat()
    issues: list[dict[str, str]] = []

    response, error = _http_get(website)
    html = response.text if response and response.status_code == 200 else None
    if not response:
        issues.append(_build_issue("unreachable_site", "critical", f"Unable to reach {website}: {error}"))
    else:
        issues.extend(_check_ssl(website))
        issues.extend(_check_security_headers(website, html))
        issues.extend(_detect_cms(website, html))
        issues.extend(_check_exposed_admin(website))
        issues.extend(_check_robots_txt(website))
        issues.extend(_check_http_to_https(website))

    if not issues:
        issues.append(_build_issue("no_findings", "low", "No obvious security issues were detected."))

    score = _calculate_score(issues)
    summary = _build_summary(website, issues)
    result = {
        "website": website,
        "scan_time": scan_time,
        "issues": issues,
        "score": score,
        "summary": summary,
    }

    _save_issues(website, issues)
    return result


async def batch_scan(urls: list[str]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    total = len(urls)
    print(f"Starting batch scan for {total} websites.")

    async def _scan_task(index: int, website: str) -> dict[str, object]:
        print(f"[{index}/{total}] Scanning {website}")
        result = await asyncio.to_thread(scan_website, website)
        print(f"[{index}/{total}] Completed {website} (score={result['score']})")
        return result

    tasks = [asyncio.create_task(_scan_task(idx + 1, site)) for idx, site in enumerate(urls)]
    for future in asyncio.as_completed(tasks):
        result = await future
        results.append(result)

    print("Batch scan completed.")
    return results


if __name__ == "__main__":
    demo_url = "https://example.com"
    print(f"Running demo scan for {demo_url}")
    demo_result = scan_website(demo_url)
    print(demo_result)

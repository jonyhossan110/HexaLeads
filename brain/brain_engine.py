"""
Local Brain: spaCy medium model (50–100MB class) for relevance scoring,
plus optional deep contact discovery (HTTP / curl-style fetch).
"""
from __future__ import annotations

import re
import subprocess
import sys
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote_plus

# en_core_web_md ~40MB — preferred; en_core_web_sm ~12MB fallback
SPACY_MODELS: Tuple[str, ...] = ("en_core_web_md", "en_core_web_sm")

_nlp = None
_model_name: Optional[str] = None

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)
LINKEDIN_RE = re.compile(r"https?://(?:[\w.-]+\.)?linkedin\.com/[\w\-./?#&=%]+", re.IGNORECASE)
HTTP_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)

AGGREGATOR_HOSTS = frozenset(
    {
        "google.com",
        "maps.google.com",
        "gstatic.com",
        "yelp.com",
        "bing.com",
        "duckduckgo.com",
        "facebook.com",
        "instagram.com",
        "youtube.com",
        "wikipedia.org",
    }
)


def _rich_warn(msg: str) -> None:
    try:
        from rich.console import Console

        Console().print(f"[yellow]{msg}[/yellow]")
    except Exception:
        print(msg, file=sys.stderr)


def load_nlp():
    """Load best available spaCy model (cached)."""
    global _nlp, _model_name
    if _nlp is not None:
        return _nlp
    import spacy

    last_err: Optional[Exception] = None
    for name in SPACY_MODELS:
        try:
            _nlp = spacy.load(name)
            _model_name = name
            return _nlp
        except OSError as exc:
            last_err = exc
            continue
    raise OSError(
        f"No spaCy model found (tried {list(SPACY_MODELS)}). "
        "Install: python -m spacy download en_core_web_md"
    ) from last_err


def active_model_name() -> str:
    load_nlp()
    return _model_name or "unknown"


def _host_allowed(url: str) -> bool:
    try:
        from urllib.parse import urlparse

        netloc = (urlparse(url).netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        for agg in AGGREGATOR_HOSTS:
            if netloc == agg or netloc.endswith("." + agg):
                return False
        return bool(netloc)
    except Exception:
        return False


def relevance_score(lead: Dict[str, Any], target_category: str) -> float:
    """0..1 semantic-ish relevance vs mission category (spaCy vectors when available)."""
    try:
        nlp = load_nlp()
    except Exception:
        return 0.5

    name = str(lead.get("name") or "")[:500]
    cat = str(lead.get("category") or "")[:200]
    blob = f"{name} {cat}".strip()
    if not blob:
        return 0.0

    target = f"{target_category} local business services".strip()
    doc_a = nlp(blob[:5000])
    doc_b = nlp(target[:5000])
    if doc_a.vector_norm and doc_b.vector_norm:
        sim = float(doc_a.similarity(doc_b))
        return max(0.0, min(1.0, sim))
    overlap = set(t.lower_ for t in doc_a if t.is_alpha) & set(t.lower_ for t in doc_b if t.is_alpha)
    return min(1.0, len(overlap) / 8.0) if overlap else 0.35


def verify_lead_text(lead: Dict[str, Any], target_category: str) -> Tuple[bool, str]:
    """
    Gate leads using spaCy + category relevance (stricter than name-only).
    """
    try:
        nlp = load_nlp()
    except Exception as exc:
        return True, f"spaCy unavailable ({exc}); skipping NLP gate."

    name = str(lead.get("name") or "").strip()
    if len(name) < 2:
        return False, "Name too short for NLP validation."

    doc = nlp(name[:500])
    alpha = [t for t in doc if t.is_alpha]
    if not alpha:
        return False, "No alphabetic tokens in business name (spaCy)."

    blob = f"{name} {lead.get('category', '')} {target_category}".lower()
    junk_markers = ("test test", "lorem", "asdf", "xxx", "n/a", "none none")
    for m in junk_markers:
        if m in blob:
            return False, f"Junk pattern detected ({m})."

    if len(name) < 4 and len(alpha) < 2:
        return False, "Name too sparse for a credible lead."

    rel = relevance_score(lead, target_category)
    if rel < 0.22:
        return False, f"Low category relevance ({rel:.2f}) for Local Brain."

    return True, f"spaCy gate passed ({active_model_name()}), relevance={rel:.2f}."


def _extract_from_html(html: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not html:
        return out
    emails = EMAIL_RE.findall(html)
    if emails:
        out["email"] = emails[0]
    lm = LINKEDIN_RE.search(html)
    if lm:
        out["linkedin"] = lm.group(0).split("?")[0].rstrip("/")
    for m in HTTP_URL_RE.finditer(html):
        u = m.group(0).rstrip(").,;]")
        if _host_allowed(u):
            out["website"] = u
            break
    return out


def _fetch_url_requests(url: str, timeout: int = 18) -> Optional[str]:
    try:
        import requests

        r = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def _fetch_url_curl(url: str, timeout: int = 18) -> Optional[str]:
    try:
        proc = subprocess.run(
            [
                "curl",
                "-sL",
                "--max-time",
                str(timeout),
                "-A",
                "Mozilla/5.0 (compatible; HexaLeads/1.0)",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )
        if proc.returncode != 0 or not (proc.stdout or "").strip():
            return None
        return proc.stdout
    except Exception:
        return None


def is_non_aggregator_url(url: str) -> bool:
    """True if URL host is not a known directory/aggregator."""
    return _host_allowed(url)


def deep_search_contact(
    lead: Dict[str, Any],
    city: str,
    country: str,
    category: str,
) -> Dict[str, Any]:
    """
    One-shot deep search when email/website is missing: DuckDuckGo HTML via requests, then curl fallback.
    Returns fields to merge into lead (may be empty).
    """
    has_email = bool(str(lead.get("email") or "").strip())
    has_site = bool(str(lead.get("website") or "").strip())
    has_li = bool(str(lead.get("linkedin") or "").strip())
    if has_email and has_site and has_li:
        return {}

    name = str(lead.get("name") or "").strip()
    if not name:
        return {}

    q = f'{name} {city} {country} {category} official website email contact'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(q)}"

    html = _fetch_url_requests(url)
    if not html:
        html = _fetch_url_curl(url)
    if not html:
        return {"deep_search": "incomplete", "deep_search_note": "fetch_failed"}

    found = _extract_from_html(html)
    if not found:
        return {"deep_search": "incomplete", "deep_search_note": "no_signals_in_results"}

    found["deep_search"] = "partial" if not (found.get("email") and found.get("website")) else "ok"
    return found

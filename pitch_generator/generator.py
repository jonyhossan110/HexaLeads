"""
pitch_generator/generator.py

Generate personalized cold email pitches for HexaLeads leads.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from database.db import get_all_leads, get_security_issues, insert_outreach

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
GPT4ALL_MODEL_PATH = os.environ.get("GPT4ALL_MODEL_PATH", "")
GPT4ALL_DIR = os.environ.get("GPT4ALL_DIR", r"C:\Users\Public\GPT4All")
API_DELAY_SECONDS = 1
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
SYSTEM_PROMPT = """
You are a professional cybersecurity consultant and web developer named Jony from HexaCyberLab. 
Write cold email pitches that are:
- Short (max 150 words in body)
- Specific to the client's actual problems
- Professional but warm tone
- End with a clear soft call-to-action
- Never sound like spam or mass email
- Always mention 1-2 specific issues found on their website
- Offer a FREE website security audit as the hook
Do not use: buzzwords, excessive caps, aggressive sales language.
"""

PITCH_TYPES = {"security": "SECURITY_PITCH", "webdev": "WEBDEV_PITCH", "general": "GENERAL_PITCH"}

logger = logging.getLogger("pitch_generator")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_lower(value: Any) -> str:
    return _normalize_text(value).lower()


def _first_name(lead: Dict[str, Any]) -> str:
    business_name = _normalize_text(lead.get("business_name") or lead.get("company") or "")
    if business_name:
        return business_name.split()[0]
    email = _normalize_text(lead.get("email") or "")
    if "@" in email:
        local = email.split("@", 1)[0]
        return local.split(".")[0].capitalize()
    return "there"


def _build_main_issue(lead: Dict[str, Any], security_issues: List[Dict[str, Any]]) -> str:
    if security_issues:
        issue_details = [issue.get("detail") or issue.get("type") or "security concern" for issue in security_issues]
        issue_text = issue_details[0]
        if len(issue_details) > 1:
            issue_text = f"{issue_details[0]} and {issue_details[1]}"
        return issue_text
    website_issue = _normalize_text(lead.get("website_status") or "outdated website")
    return website_issue or "a few website issues"


def _determine_pitch_type(lead: Dict[str, Any], security_issues: List[Dict[str, Any]]) -> str:
    if security_issues:
        return PITCH_TYPES["security"]
    website_status = _normalize_lower(lead.get("website_status") or "")
    if any(token in website_status for token in ["outdated", "poor", "broken", "bad", "needs work"]):
        return PITCH_TYPES["webdev"]
    if lead.get("outdated") or _normalize_lower(lead.get("industry") or "") in {"web design", "development"}:
        return PITCH_TYPES["webdev"]
    return PITCH_TYPES["general"]


def _template_fallback(lead: Dict[str, Any], security_issues: List[Dict[str, Any]], pitch_type: str) -> Dict[str, str]:
    business_name = _normalize_text(lead.get("business_name") or lead.get("website") or "your business")
    first_name = _first_name(lead)
    main_issue = _build_main_issue(lead, security_issues)
    subject = f"Quick security concern about {business_name}'s website"
    body = (
        f"Hi {first_name}, I noticed {business_name}'s website has {main_issue}. "
        "I'm Jony from HexaCyberLab — we specialize in web security for businesses like yours. "
        "I'd love to offer you a completely free website security audit, no strings attached. "
        "Would you be open to a quick look? Takes 24 hours, zero cost. "
        "Best, Jony | HexaCyberLab | hexacyberlab.com"
    )
    return {"subject": subject, "body": body, "pitch_type": pitch_type}


def _build_prompt(lead: Dict[str, Any], security_issues: List[Dict[str, Any]], pitch_type: str) -> str:
    business_name = _normalize_text(lead.get("business_name") or lead.get("website") or "your business")
    website = _normalize_text(lead.get("website") or "")
    country = _normalize_text(lead.get("country") or "")
    industry = _normalize_text(lead.get("industry") or "")
    first_name = _first_name(lead)
    issue_text = _build_main_issue(lead, security_issues)
    context_lines = [
        f"Lead name: {business_name}",
        f"Website: {website}",
    ]
    if country:
        context_lines.append(f"Country: {country}")
    if industry:
        context_lines.append(f"Industry: {industry}")
    if pitch_type == PITCH_TYPES["security"]:
        context_lines.append("Pitch type: security-focused")
    elif pitch_type == PITCH_TYPES["webdev"]:
        context_lines.append("Pitch type: website improvement")
    else:
        context_lines.append("Pitch type: general outreach")
    issue_summary = f"Specific issue: {issue_text}."
    return (
        SYSTEM_PROMPT
        + "\n\n"
        + "Use the following lead context to generate a subject and email body."
        + "\n".join(context_lines)
        + "\n"
        + issue_summary
        + "\n\n"
        + "Return only valid JSON with keys: subject, body."
    )


def _parse_ai_response(response: str) -> Optional[Dict[str, str]]:
    response_text = response.strip()
    if not response_text:
        return None
    try:
        parsed = json.loads(response_text)
        if isinstance(parsed, dict) and parsed.get("subject") and parsed.get("body"):
            return {"subject": _normalize_text(parsed["subject"]), "body": _normalize_text(parsed["body"])}
    except json.JSONDecodeError:
        pass
    if "subject:" in response_text.lower() and "body:" in response_text.lower():
        parts = re.split(r"subject:\s*|body:\s*", response_text, flags=re.IGNORECASE)
        if len(parts) >= 3:
            return {"subject": _normalize_text(parts[1]), "body": _normalize_text(parts[2])}
    lines = [line.strip() for line in response_text.splitlines() if line.strip()]
    if lines:
        subject = lines[0]
        body = "\n".join(lines[1:]) if len(lines) > 1 else ""
        return {"subject": subject, "body": body}
    return None


def _call_openai(prompt: str) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    try:
        import openai
    except ImportError:
        logger.warning("OpenAI package is not installed, skipping OpenAI backend.")
        return None
    try:
        openai.api_key = OPENAI_API_KEY
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
            max_tokens=250,
            temperature=0.7,
        )
        choice = response.choices[0]
        text = choice.message.get("content") if hasattr(choice, "message") else choice.text
        return _normalize_text(text)
    except Exception as exc:
        logger.warning("OpenAI request failed: %s", exc)
        return None


def _find_local_gpt4all_model() -> Optional[str]:
    candidates = [GPT4ALL_MODEL_PATH, os.path.join(GPT4ALL_DIR, "gpt4all-lora-unfiltered-q4_0.bin")]
    candidates.extend([
        r"C:\Users\Public\GPT4All\gpt4all-lora-unfiltered-q4_0.bin",
        r"C:\Users\GPT4All\gpt4all-lora-unfiltered-q4_0.bin",
    ])
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


def _call_gpt4all(prompt: str) -> Optional[str]:
    try:
        from gpt4all import GPT4All
    except ImportError:
        logger.warning("GPT4All package is not installed, skipping local model backend.")
        return None
    model_path = _find_local_gpt4all_model()
    if not model_path:
        logger.warning("GPT4All local model not found, skipping local model backend.")
        return None
    try:
        gpt = GPT4All(model=model_path)
        output = gpt.generate(prompt, max_tokens=250)
        if isinstance(output, str):
            return _normalize_text(output)
        if isinstance(output, list):
            return _normalize_text(" ".join(str(item) for item in output))
    except Exception as exc:
        logger.warning("GPT4All generation failed: %s", exc)
    return None


def _generate_with_ai(prompt: str) -> Optional[Dict[str, str]]:
    openai_response = _call_openai(prompt)
    if openai_response:
        parsed = _parse_ai_response(openai_response)
        if parsed:
            return parsed
    gpt4all_response = _call_gpt4all(prompt)
    if gpt4all_response:
        parsed = _parse_ai_response(gpt4all_response)
        if parsed:
            return parsed
    return None


def _pitch_quality_score(lead: Dict[str, Any], security_issues: List[Dict[str, Any]], generated_with_ai: bool) -> int:
    score = 0
    if _normalize_text(lead.get("business_name")):
        score += 2
    if _normalize_text(lead.get("website")):
        score += 1
    if security_issues:
        score += 3
    if _normalize_text(lead.get("country")) or _normalize_text(lead.get("industry")):
        score += 1
    if generated_with_ai:
        score += 3
    return min(score, 10)


def _save_pitch(lead: Dict[str, Any], subject: str, body: str) -> None:
    lead_id = lead.get("id")
    if not lead_id:
        return
    try:
        insert_outreach(
            lead_id=int(lead_id),
            email_subject=subject,
            email_body=body,
            status="draft",
            sent_at=None,
        )
    except Exception as exc:
        logger.warning("Unable to save pitch for lead %s: %s", lead_id, exc)


def generate_pitch(lead: Dict[str, Any], security_issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    pitch_type = _determine_pitch_type(lead, security_issues)
    prompt = _build_prompt(lead, security_issues, pitch_type)
    ai_result = _generate_with_ai(prompt)
    if ai_result:
        subject = ai_result["subject"]
        body = ai_result["body"]
        generated_with_ai = True
    else:
        fallback = _template_fallback(lead, security_issues, pitch_type)
        subject = fallback["subject"]
        body = fallback["body"]
        generated_with_ai = False

    quality_score = _pitch_quality_score(lead, security_issues, generated_with_ai)
    result = {
        "subject": subject,
        "body": body,
        "pitch_type": pitch_type,
        "quality_score": quality_score,
    }
    _save_pitch(lead, subject, body)
    return result


def _find_lead_by_id(lead_id: int) -> Optional[Dict[str, Any]]:
    for row in get_all_leads():
        if int(row["id"]) == int(lead_id):
            return {key: row[key] for key in row.keys()}
    return None


def batch_generate(lead_ids: List[int]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for lead_id in lead_ids:
        lead = _find_lead_by_id(lead_id)
        if not lead:
            logger.warning("Lead not found for id %s", lead_id)
            continue
        security_issues = get_security_issues(int(lead_id))
        result = generate_pitch(lead, [
            {key: issue[key] for key in issue.keys()} for issue in security_issues
        ])
        results.append({"lead_id": lead_id, **result})
    return results


def regenerate_pitch(lead_id: int) -> Dict[str, Any]:
    lead = _find_lead_by_id(lead_id)
    if not lead:
        raise ValueError(f"Lead not found for id {lead_id}")
    security_issues = get_security_issues(int(lead_id))
    result = generate_pitch(lead, [
        {key: issue[key] for key in issue.keys()} for issue in security_issues
    ])
    return {"lead_id": lead_id, **result}


if __name__ == "__main__":
    sample_lead = {
        "id": None,
        "business_name": "Bright Future Digital",
        "website": "https://example.com",
        "email": "contact@example.com",
        "country": "Bangladesh",
        "industry": "Web Development",
    }
    sample_issues = [
        {"type": "ssl_expiring", "severity": "high", "detail": "SSL certificate expires in 12 days"},
    ]
    pitch = generate_pitch(sample_lead, sample_issues)
    print(pitch)

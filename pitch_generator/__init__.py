from __future__ import annotations

import asyncio
import os
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

logger = get_logger("pitch_generator")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4")

DEFAULT_SERVICES = [
    "Web Penetration Testing & VAPT",
    "Website Security Audit",
    "Malware Removal & Cleanup",
    "Incident Response & Website Recovery",
    "Custom Web Development",
]


def _build_prompt(business_name: str, website: str, industry: str, services: List[str]) -> str:
    service_list = "\n".join(f"- {service}" for service in services)
    return (
        "You are a professional cybersecurity and web development consultant. "
        "Write a concise, personalized cold outreach email pitch for a business. "
        "Use a friendly but authoritative tone and mention potential security risk reduction.\n\n"
        f"Business Name: {business_name}\n"
        f"Website: {website}\n"
        f"Industry: {industry}\n"
        f"Services:\n{service_list}\n\n"
        "Include a short subject line and a call to action asking for a discovery call or security review. "
        "Keep the message under 220 words."
    )


async def _openai_generate(prompt: str) -> Dict[str, str]:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.6,
        "max_tokens": 400,
    }
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async with session.post(url, headers=headers, json=payload) as response:
            data = await response.json()
            content = data["choices"][0]["message"]["content"].strip()
            return {"subject": f"Security Review for {website}", "body": content}


def _build_template(business_name: str, website: str, industry: str, services: List[str]) -> Dict[str, str]:
    subject = f"Secure {business_name}'s website and protect customer trust"
    body = (
        f"Hello {business_name},\n\n"
        f"I reviewed {website} and I can help your {industry} business reduce risk from outdated plugins, missing HTTPS controls, and weak web application defenses. "
        f"My team specializes in:\n"
        + "\n".join(f"- {service}" for service in services[:3])
        + "\n\n"
        "If you want a fast security assessment and a custom recovery plan, I can share a short action plan within 24 hours. "
        "Would you be available for a quick call this week?\n\n"
        "Best regards,\n"
        "HexaLeads Cybersecurity Team"
    )
    return {"subject": subject, "body": body}


async def generate_pitch(
    business_name: str,
    website: str,
    industry: str = "business website",
    services: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Generate a personalized outreach subject and email body."""
    services = services or DEFAULT_SERVICES[:3]
    if OPENAI_API_KEY:
        try:
            prompt = _build_prompt(business_name, website, industry, services)
            result = await _openai_generate(prompt)
            logger.info("Generated AI pitch for %s", business_name)
            return result
        except Exception as exc:
            logger.warning("OpenAI pitch generation failed: %s", exc)
    logger.info("Using fallback pitch template for %s", business_name)
    return _build_template(business_name, website, industry, services)


async def run(
    business_name: str,
    website: str,
    industry: str = "business website",
    service: Optional[str] = None,
) -> Dict[str, str]:
    """Entry point for the pitch_generator package."""
    service_list = [service] if service else DEFAULT_SERVICES[:3]
    return await generate_pitch(business_name, website, industry, service_list)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads pitch generator")
    parser.add_argument("business_name", help="Business name for the pitch")
    parser.add_argument("website", help="Website URL for the pitch")
    parser.add_argument("--industry", default="business website", help="Industry to mention")
    parser.add_argument(
        "--service",
        default=None,
        help="Optional service to mention in the pitch",
    )
    args = parser.parse_args()
    pitch = asyncio.run(run(args.business_name, args.website, args.industry, args.service))
    print(pitch)

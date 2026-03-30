from __future__ import annotations

import asyncio
import os
import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from database.db import insert_outreach
from utils.logger import get_logger

logger = get_logger("outreach_bot")
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "noreply@hexaleads.local")


def _build_message(
    recipient: str,
    subject: str,
    body: str,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = EMAIL_FROM
    message["To"] = recipient
    message["Subject"] = subject
    message.set_content(body)
    return message


def _send_smtp(message: EmailMessage) -> None:
    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD:
        raise RuntimeError("SMTP configuration is missing in environment variables.")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(message)


async def send_email(
    lead_id: int,
    recipient: str,
    subject: str,
    body: str,
    approved: bool = False,
) -> Dict[str, Any]:
    """Create outreach record and optionally send the email."""
    outreach_id = insert_outreach(
        lead_id=lead_id,
        email_subject=subject,
        email_body=body,
        status="draft" if not approved else "sent",
        sent_at=None,
    )
    result: Dict[str, Any] = {
        "lead_id": lead_id,
        "recipient": recipient,
        "subject": subject,
        "outreach_id": outreach_id,
        "status": "draft",
    }
    message = _build_message(recipient, subject, body)

    if approved:
        try:
            await asyncio.to_thread(_send_smtp, message)
            result["status"] = "sent"
            result["sent_at"] = True
            logger.info("Email sent to %s for lead %s", recipient, lead_id)
        except Exception as exc:
            result["status"] = "failed"
            result["error"] = str(exc)
            logger.error("Failed to send email to %s: %s", recipient, exc)
    else:
        logger.info("Draft outreach saved for lead %s to %s", lead_id, recipient)
    return result


async def run(
    lead_id: int,
    recipient: str,
    subject: str,
    body: str,
    approve: bool = False,
) -> Dict[str, Any]:
    """Entry point for the outreach_bot package."""
    return await send_email(lead_id, recipient, subject, body, approved=approve)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads outreach bot")
    parser.add_argument("lead_id", type=int, help="Lead ID to attach outreach to")
    parser.add_argument("recipient", help="Recipient email address")
    parser.add_argument("subject", help="Email subject")
    parser.add_argument("body", help="Email body")
    parser.add_argument("--approve", action="store_true", help="Send email immediately")
    args = parser.parse_args()
    output = asyncio.run(run(args.lead_id, args.recipient, args.subject, args.body, args.approve))
    print(output)

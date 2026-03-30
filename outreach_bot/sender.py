"""
outreach_bot/sender.py

Send approved outreach emails and handle Telegram approval commands.
"""

from __future__ import annotations

import json
import logging
import os
import re
import smtplib
import ssl
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from config import get_config
from database.db import (
    get_all_leads,
    get_outreach_by_lead,
    insert_outreach,
    update_lead_status,
)

try:
    from telegram import Bot
    from telegram.error import TelegramError
except ImportError:  # pragma: no cover
    Bot = None  # type: ignore
    TelegramError = Exception

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "hello@hexacyberlab.com")
FROM_NAME = os.environ.get("FROM_NAME", "Jony | HexaCyberLab")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TRACKING_HOST = os.environ.get("TRACKING_HOST", "https://hexacyberlab.com")
UNSUBSCRIBE_LINK = os.environ.get("UNSUBSCRIBE_LINK", "https://hexacyberlab.com/unsubscribe")

config = get_config()
MAX_EMAILS_PER_DAY = int(config.get("max_emails_per_day", 50))
DEFAULT_FOLLOWUP_DAYS = int(config.get("follow_up_days", 4))
MAX_FOLLOWUPS = int(config.get("max_follow_ups", 2))

logger = logging.getLogger("outreach_bot.sender")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

PHONE_COMMAND_PATTERN = re.compile(r"^/(approve|reject|edit|status|leads|scan)\b", re.IGNORECASE)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_from_header() -> str:
    name = _normalize_text(FROM_NAME)
    return f"{name} <{FROM_EMAIL}>" if name else FROM_EMAIL


def _current_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime(value: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _find_lead_by_id(lead_id: int) -> Optional[Dict[str, Any]]:
    for row in get_all_leads():
        if int(row["id"]) == lead_id:
            return {key: row[key] for key in row.keys()}
    return None


def _build_email_body(body: str, lead_id: int) -> EmailMessage:
    footer = (
        f"\n\n---\n"
        f"Jony | HexaCyberLab | Web Security Expert | hexacyberlab.com\n"
        f"Unsubscribe: {UNSUBSCRIBE_LINK}"
    )
    plain_text = body.strip() + footer
    tracking_pixel = (
        f"<img src=\"{TRACKING_HOST}/track?lead_id={lead_id}&ts={int(time.time())}\" "
        f"alt=\"\" style=\"display:none;width:1px;height:1px;\">"
    )
    html_body = body.replace("\n", "<br>") + "<br><br>" + footer.replace("\n", "<br>") + tracking_pixel
    message = EmailMessage()
    message["From"] = _build_from_header()
    message.set_content(plain_text)
    message.add_alternative(html_body, subtype="html")
    return message


def _create_message(recipient: str, subject: str, body: str, lead_id: int) -> EmailMessage:
    msg = _build_email_body(body, lead_id)
    msg["To"] = recipient
    msg["Subject"] = subject
    return msg


def _smtp_send(message: EmailMessage) -> None:
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS:
        raise RuntimeError("SMTP credentials are missing in environment variables.")

    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30, context=ssl.create_default_context()) as smtp:
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(message)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(message)


def _count_sent_today() -> int:
    cutoff = datetime.now(timezone.utc).date()
    count = 0
    for lead in get_all_leads():
        outreach_rows = get_outreach_by_lead(int(lead["id"]))
        for outreach in outreach_rows:
            if outreach["status"] == "sent" and outreach["sent_at"]:
                parsed = _parse_datetime(_normalize_text(outreach["sent_at"]))
                if parsed is not None and parsed.date() == cutoff:
                    count += 1
    return count


def _get_latest_outreach_record(lead_id: int) -> Optional[Dict[str, Any]]:
    outreach_rows = get_outreach_by_lead(lead_id)
    if not outreach_rows:
        return None
    best = outreach_rows[0]
    return {key: best[key] for key in best.keys()}


def _should_send_outreach(outreach: Dict[str, Any]) -> bool:
    status = _normalize_text(outreach.get("status"))
    if status in {"draft", "scheduled", "scheduled_followup"}:
        if status.startswith("scheduled") and outreach.get("sent_at"):
            scheduled = _parse_datetime(_normalize_text(outreach["sent_at"]))
            if scheduled is not None and scheduled > datetime.now(timezone.utc):
                return False
        return True
    return False


def _send_message(recipient: str, subject: str, body: str, lead_id: int) -> bool:
    message = _create_message(recipient, subject, body, lead_id)
    _smtp_send(message)
    logger.info("Email sent to %s for lead %s", recipient, lead_id)
    return True


def _build_telegram_bot() -> Optional[Any]:
    if not TELEGRAM_BOT_TOKEN or Bot is None:
        return None
    try:
        return Bot(token=TELEGRAM_BOT_TOKEN)
    except Exception as exc:
        logger.warning("Unable to create Telegram bot: %s", exc)
        return None


def _send_telegram_message(chat_id: str, text: str) -> None:
    bot = _build_telegram_bot()
    if bot is None:
        logger.warning("Telegram bot not available; cannot send message.")
        return
    try:
        bot.send_message(chat_id=int(chat_id), text=text)
    except TelegramError as exc:
        logger.warning("Failed to send Telegram message to %s: %s", chat_id, exc)


def _format_pitch_notification(lead: Dict[str, Any], issue_count: int, severity: str, subject: str) -> str:
    preview = _normalize_text(subject)[:50]
    return (
        f"🎯 New Lead Ready for Outreach\n"
        f"Business: {_normalize_text(lead.get('business_name') or lead.get('website'))}\n"
        f"Website: {_normalize_text(lead.get('website'))}\n"
        f"Email: {_normalize_text(lead.get('email'))}\n"
        f"Issues Found: {issue_count} ({severity})\n"
        f"Pitch Preview: {preview}...\n\n"
        f"Reply:\n"
        f"✅ /approve_{lead.get('id')} → send email now\n"
        f"✏️ /edit_{lead.get('id')} <new text> → edit pitch first\n"
        f"❌ /reject_{lead.get('id')} → skip this lead"
    )


def notify_new_pitch(lead: Dict[str, Any], issue_count: int, severity: str, subject: str, chat_id: Optional[str] = None) -> None:
    message = _format_pitch_notification(lead, issue_count, severity, subject)
    if chat_id:
        _send_telegram_message(chat_id, message)
    else:
        logger.info("New pitch notification: %s", message)


def send_email(lead_id: int) -> bool:
    lead = _find_lead_by_id(lead_id)
    if not lead:
        logger.error("Lead %s not found.", lead_id)
        return False

    recipient = _normalize_text(lead.get("email"))
    if not recipient or "@" not in recipient:
        logger.error("Lead %s has no valid recipient email.", lead_id)
        return False

    if _count_sent_today() >= MAX_EMAILS_PER_DAY:
        logger.warning("Daily email limit reached (%s). Aborting send.", MAX_EMAILS_PER_DAY)
        return False

    outreach = _get_latest_outreach_record(lead_id)
    if outreach is None or not _should_send_outreach(outreach):
        logger.error("No draft or scheduled outreach available for lead %s.", lead_id)
        return False

    subject = _normalize_text(outreach.get("email_subject"))
    body = _normalize_text(outreach.get("email_body"))
    if not subject or not body:
        logger.error("Outreach entry for lead %s lacks subject or body.", lead_id)
        return False

    try:
        _send_message(recipient, subject, body, lead_id)
    except Exception as exc:
        logger.error("Failed to send email for lead %s: %s", lead_id, exc)
        return False

    sent_at = _current_utc_iso()
    try:
        insert_outreach(
            lead_id=lead_id,
            email_subject=subject,
            email_body=body,
            status="sent",
            sent_at=sent_at,
        )
        update_lead_status(lead_id=lead_id, status="sent")
    except Exception as exc:
        logger.warning("Failed to record sent outreach for lead %s: %s", lead_id, exc)
    return True


def schedule_followup(lead_id: int, days: int = DEFAULT_FOLLOWUP_DAYS) -> bool:
    lead = _find_lead_by_id(lead_id)
    if not lead:
        logger.error("Lead %s not found for follow-up scheduling.", lead_id)
        return False

    outreach_rows = get_outreach_by_lead(lead_id)
    followups = [r for r in outreach_rows if _normalize_text(r["status"]).startswith("followup") or _normalize_text(r["status"]).startswith("scheduled")]
    if len(followups) >= MAX_FOLLOWUPS:
        logger.warning("Lead %s already has %s follow-up(s).", lead_id, len(followups))
        return False

    subject = f"Quick follow-up on your website security audit"
    prev_body = _normalize_text(outreach_rows[0]["email_body"]) if outreach_rows else ""
    body = (
        f"Hi, I wanted to follow up on my earlier email about your website's security and performance. "
        f"If you have a moment, I can review the site again and share a free audit summary. "
        f"This is a soft follow-up based on the previous message.\n\n"
        f"{prev_body[:200]}"
    )
    scheduled_at = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        insert_outreach(
            lead_id=lead_id,
            email_subject=subject,
            email_body=body,
            status="scheduled_followup",
            sent_at=scheduled_at,
        )
        logger.info("Scheduled follow-up for lead %s in %s days.", lead_id, days)
        return True
    except Exception as exc:
        logger.error("Failed to schedule follow-up for lead %s: %s", lead_id, exc)
        return False


def _summarize_status() -> str:
    leads = get_all_leads()
    today = datetime.now(timezone.utc).date()
    leads_found = sum(1 for lead in leads if _parse_datetime(_normalize_text(lead.get("created_at"))) and _parse_datetime(_normalize_text(lead.get("created_at"))).date() == today)
    emails_sent = 0
    replies = 0
    for lead in leads:
        outreach_rows = get_outreach_by_lead(int(lead["id"]))
        for outreach in outreach_rows:
            if outreach["status"] == "sent" and outreach["sent_at"]:
                sent_ts = _parse_datetime(_normalize_text(outreach["sent_at"]))
                if sent_ts and sent_ts.date() == today:
                    emails_sent += 1
            if outreach["status"] == "replied":
                replies += 1
    return (
        f"Today's stats:\n"
        f"Leads found: {leads_found}\n"
        f"Emails sent: {emails_sent}\n"
        f"Replies: {replies}"
    )


def _top_high_priority_leads() -> str:
    high_leads: List[str] = []
    for lead in get_all_leads():
        if _normalize_text(lead.get("score_label")).upper() == "HIGH":
            name = _normalize_text(lead.get("business_name") or lead.get("website"))
            website = _normalize_text(lead.get("website"))
            score = _normalize_text(lead.get("score"))
            high_leads.append(f"{name} | {website} | score: {score}")
            if len(high_leads) >= 10:
                break
    if not high_leads:
        return "No high priority leads found."
    return "Top 10 HIGH priority leads:\n" + "\n".join(high_leads)


def _run_security_scan(url: str) -> str:
    try:
        from security_scanner.scanner import scan_website
        result = scan_website(url)
        return json.dumps(
            {
                "website": result.get("website"),
                "score": result.get("score"),
                "summary": result.get("summary"),
                "issues": [issue.get("detail") for issue in result.get("issues", [])[:3]],
            },
            indent=2,
        )
    except Exception as exc:
        logger.warning("Security scan failed for %s: %s", url, exc)
        return f"Security scan failed for {url}: {exc}"


def handle_telegram_command(command: str, chat_id: str) -> str:
    command = _normalize_text(command)
    if command.startswith("/approve_"):
        match = re.match(r"/approve_(\d+)", command)
        if not match:
            return "Invalid approve command. Use /approve_<lead_id>."
        lead_id = int(match.group(1))
        success = send_email(lead_id)
        return "Email sent successfully." if success else "Failed to send email. Check logs."

    if command.startswith("/reject_"):
        match = re.match(r"/reject_(\d+)", command)
        if not match:
            return "Invalid reject command. Use /reject_<lead_id>."
        lead_id = int(match.group(1))
        update_lead_status(lead_id=lead_id, status="rejected")
        return f"Lead {lead_id} marked as rejected."

    if command.startswith("/edit_"):
        match = re.match(r"/edit_(\d+)\s+(.+)", command)
        if not match:
            return "Invalid edit command. Use /edit_<lead_id> <new_body>."
        lead_id = int(match.group(1))
        new_body = match.group(2).strip()
        lead = _find_lead_by_id(lead_id)
        if not lead:
            return f"Lead {lead_id} not found."
        outreach = _get_latest_outreach_record(lead_id)
        subject = _normalize_text(outreach.get("email_subject")) if outreach else f"Quick note about {lead.get('business_name') or lead.get('website')}"
        try:
            _assemble_and_send_edit(lead, subject, new_body)
            return "Edited pitch sent successfully."
        except Exception as exc:
            logger.error("Failed to send edited pitch for lead %s: %s", lead_id, exc)
            return "Failed to send edited pitch. Check logs."

    if command.startswith("/status"):
        return _summarize_status()

    if command.startswith("/leads"):
        return _top_high_priority_leads()

    if command.startswith("/scan "):
        url = command[len("/scan "):].strip()
        if not url:
            return "Usage: /scan <url>"
        return _run_security_scan(url)

    return (
        "Available commands:\n"
        "/approve_<id> — send email now\n"
        "/reject_<id> — reject lead\n"
        "/edit_<id> <new body> — edit and send pitch\n"
        "/status — today's outreach stats\n"
        "/leads — top 10 HIGH priority leads\n"
        "/scan <url> — run security scan on demand"
    )


def _assemble_and_send_edit(lead: Dict[str, Any], subject: str, new_body: str) -> None:
    recipient = _normalize_text(lead.get("email"))
    if not recipient or "@" not in recipient:
        raise ValueError("Lead does not have a valid email address.")
    if _count_sent_today() >= MAX_EMAILS_PER_DAY:
        raise RuntimeError("Daily email limit reached.")
    _send_message(recipient, subject, new_body, int(lead["id"]))
    insert_outreach(
        lead_id=int(lead["id"]),
        email_subject=subject,
        email_body=new_body,
        status="sent",
        sent_at=_current_utc_iso(),
    )
    update_lead_status(lead_id=int(lead["id"]), status="sent")


if __name__ == "__main__":
    print(handle_telegram_command("/status", "0"))

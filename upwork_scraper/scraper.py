"""
upwork_scraper/scraper.py

Monitor Upwork jobs via RSS, generate proposals, save new jobs, and notify Telegram.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import feedparser
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from database.db import DB_PATH, insert_upwork_job

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

KEYWORDS = [
    "web security audit",
    "penetration testing",
    "WordPress security",
    "website hacked fix",
    "malware removal website",
    "website vulnerability",
    "web application security",
    "VAPT",
    "cybersecurity consultant",
    "website development",
    "fix hacked website",
]

SYSTEM_PROMPT = """
You are Jony from HexaCyberLab — a professional web security expert and web developer with 3+ years experience.
Write Upwork proposals that:
- Start with the client's EXACT problem (not generic opener)
- Mention 1-2 specific technical solutions you would use
- Include a relevant achievement or capability proof
- Keep to 150-200 words
- End with a question to engage the client
- Sound like a human expert, not a template
Never start with "I am writing to..."
Always start with the client's specific pain point.
"""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("UPWORK_TELEGRAM_CHAT_ID", "").strip()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
GPT4ALL_MODEL_PATH = os.environ.get("GPT4ALL_MODEL_PATH", "").strip()
GPT4ALL_DIR = os.environ.get("GPT4ALL_DIR", r"C:\Users\Public\GPT4All")

logger = logging.getLogger("upwork_scraper.scraper")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_rss_url(keyword: str) -> str:
    query = re.sub(r"\s+", "+", keyword.strip())
    return f"https://www.upwork.com/ab/feed/jobs/rss?q={query}&sort=recency"


def _parse_entry(entry: Any) -> Dict[str, Any]:
    title = _normalize_text(entry.get("title"))
    link = _normalize_text(entry.get("link"))
    budget = _normalize_text(entry.get("category"))
    description = _normalize_text(entry.get("summary"))
    if not description and entry.get("description"):
        description = _normalize_text(entry.get("description"))

    posted_time = ""
    published = entry.get("published_parsed") or entry.get("updated_parsed")
    if published:
        posted_time = datetime.fromtimestamp(time.mktime(published), tz=timezone.utc).isoformat()

    client_country = _extract_client_country(description)
    if not client_country:
        client_country = _extract_client_country(entry.get("title", ""))

    return {
        "job_title": title,
        "job_url": link,
        "budget": budget,
        "posted_time": posted_time,
        "description": description,
        "client_country": client_country,
    }


def _extract_client_country(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"Client Location:\s*([^<\n,]+)", text, re.IGNORECASE)
    if match:
        return _normalize_text(match.group(1))
    match = re.search(r"Location:\s*([^<\n,]+)", text, re.IGNORECASE)
    if match:
        return _normalize_text(match.group(1))
    return ""


def _is_recent(posted_time: str) -> bool:
    if not posted_time:
        return False
    try:
        posted_dt = datetime.fromisoformat(posted_time)
    except ValueError:
        return False
    return datetime.now(timezone.utc) - posted_dt <= timedelta(hours=24)


def _fetch_feed(keyword: str) -> List[Dict[str, Any]]:
    url = _build_rss_url(keyword)
    try:
        feed = feedparser.parse(url)
    except Exception as exc:
        logger.warning("Failed to parse Upwork RSS for '%s': %s", keyword, exc)
        return []

    jobs: List[Dict[str, Any]] = []
    for entry in feed.entries:
        job = _parse_entry(entry)
        if job["job_url"] and job["posted_time"] and _is_recent(job["posted_time"]):
            jobs.append(job)
    logger.info("Fetched %s recent jobs for keyword '%s'", len(jobs), keyword)
    return jobs


def _existing_job_urls() -> set[str]:
    if not DB_PATH.exists():
        return set()
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT job_url FROM upwork_jobs").fetchall()
        return { _normalize_text(row["job_url"]) for row in rows }


def _insert_job(job: Dict[str, Any], proposal: str) -> Optional[int]:
    try:
        row_id = insert_upwork_job(
            job_title=job["job_title"],
            job_url=job["job_url"],
            budget=job["budget"],
            client_country=job["client_country"],
            description=job["description"],
            proposal_draft=proposal,
            status="new",
        )
        return row_id
    except Exception as exc:
        logger.warning("Failed to insert Upwork job %s: %s", job.get("job_url"), exc)
        return None


def _build_proposal_prompt(job: Dict[str, Any]) -> str:
    title = job.get("job_title") or "Upwork job"
    description = job.get("description") or ""
    client_country = job.get("client_country") or ""
    budget = job.get("budget") or ""
    return (
        SYSTEM_PROMPT
        + "\n\n"
        + f"Job title: {title}\n"
        + f"Location: {client_country}\n"
        + f"Budget details: {budget}\n"
        + "Client requirements: \n"
        + description
        + "\n\n"
        + "Write a proposal that directly addresses the client's exact problem and includes 1-2 technical solutions."
    )


def _parse_proposal_response(text: str) -> str:
    return _normalize_text(text)


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
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=300,
            temperature=0.7,
        )
        choice = response.choices[0]
        text = choice.message.get("content") if hasattr(choice, "message") else choice.text
        return _parse_proposal_response(text)
    except Exception as exc:
        logger.warning("OpenAI call failed: %s", exc)
        return None


def _find_local_gpt4all_model() -> Optional[str]:
    candidates = [GPT4ALL_MODEL_PATH, os.path.join(GPT4ALL_DIR, "gpt4all-lora-unfiltered-q4_0.bin")]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _call_gpt4all(prompt: str) -> Optional[str]:
    try:
        from gpt4all import GPT4All
    except ImportError:
        logger.warning("GPT4All package is not installed, skipping local model backend.")
        return None
    model_path = _find_local_gpt4all_model()
    if not model_path:
        logger.warning("GPT4All model not found, skipping local model backend.")
        return None
    try:
        gpt = GPT4All(model=model_path)
        output = gpt.generate(prompt, max_tokens=300)
        if isinstance(output, str):
            return _parse_proposal_response(output)
        if isinstance(output, list):
            return _parse_proposal_response(" ".join(str(item) for item in output))
    except Exception as exc:
        logger.warning("GPT4All generation failed: %s", exc)
    return None


def _generate_proposal_text(prompt: str) -> str:
    openai_result = _call_openai(prompt)
    if openai_result:
        return openai_result
    gpt4all_result = _call_gpt4all(prompt)
    if gpt4all_result:
        return gpt4all_result
    return (
        f"I can help with the exact issue described in this job posting. "
        f"I would start by reviewing the current website security configuration, "
        f"apply targeted vulnerability scans, and implement hardening for WordPress and server headers. "
        f"I have delivered similar fixes for hacked sites and malware cleanup, and I can provide a fast, expert proposal. "
        f"Would you like me to start with a free site security analysis for this project?"
    )


def generate_proposal(job: Dict[str, Any]) -> str:
    prompt = _build_proposal_prompt(job)
    return _generate_proposal_text(prompt)


def notify_telegram(job: Dict[str, Any], proposal: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram notification skipped because no bot token or chat ID is configured.")
        return
    try:
        from telegram import Bot
    except ImportError:
        logger.warning("python-telegram-bot is not installed; skipping Telegram notification.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    preview = proposal[:240].replace("\n", " ")
    text = (
        f"📌 New Upwork job found\n"
        f"Title: {job.get('job_title')}\n"
        f"Budget: {job.get('budget') or 'N/A'}\n"
        f"Country: {job.get('client_country') or 'N/A'}\n"
        f"Proposal preview: {preview}...\n\n"
        f"Reply with /approve_upwork_<id> to copy the proposal to clipboard when the command handler is available."
    )
    try:
        bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=text)
    except Exception as exc:
        logger.warning("Failed to send Telegram notification: %s", exc)


def save_jobs(jobs: List[Dict[str, Any]]) -> int:
    saved = 0
    existing_urls = _existing_job_urls()
    for job in jobs:
        if _normalize_text(job.get("job_url")) in existing_urls:
            continue
        proposal = generate_proposal(job)
        row_id = _insert_job(job, proposal)
        if row_id:
            saved += 1
            existing_urls.add(_normalize_text(job["job_url"]))
            notify_telegram({**job, "id": row_id}, proposal)
    return saved


def scrape_jobs(keywords: list[str]) -> List[Dict[str, Any]]:
    new_jobs: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for keyword in keywords:
        jobs = _fetch_feed(keyword)
        for job in jobs:
            url = _normalize_text(job["job_url"])
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            new_jobs.append(job)

    saved_jobs: List[Dict[str, Any]] = []
    existing_urls = _existing_job_urls()
    for job in new_jobs:
        url = _normalize_text(job["job_url"])
        if url in existing_urls:
            continue
        proposal = generate_proposal(job)
        row_id = _insert_job(job, proposal)
        if row_id:
            existing_urls.add(url)
            saved_job = {**job, "proposal_draft": proposal, "id": row_id}
            saved_jobs.append(saved_job)
            notify_telegram(saved_job, proposal)

    logger.info("Scraped %s candidates and saved %s new jobs.", len(new_jobs), len(saved_jobs))
    return saved_jobs


def _scheduled_scrape() -> None:
    logger.info("Running scheduled Upwork scrape...")
    try:
        jobs = scrape_jobs(KEYWORDS)
        logger.info("Scheduled scrape found %s jobs.", len(jobs))
    except Exception as exc:
        logger.error("Scheduled Upwork scrape failed: %s", exc)


scheduler = BackgroundScheduler(job_defaults={"coalesce": True, "max_instances": 1}, timezone="UTC")
scheduler.add_job(_scheduled_scrape, "interval", hours=2, next_run_time=datetime.now(timezone.utc))
scheduler.start()

def _shutdown_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)

atexit.register(_shutdown_scheduler)

logger.info("Upwork scraper scheduler started with 2-hour polling interval.")


if __name__ == "__main__":
    scraped = scrape_jobs(KEYWORDS)
    print(f"Scraped {len(scraped)} jobs.")

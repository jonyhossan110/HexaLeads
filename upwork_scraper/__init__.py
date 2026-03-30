from __future__ import annotations

import asyncio
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import aiohttp
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from database.db import insert_upwork_job
from utils.logger import get_logger

logger = get_logger("upwork_scraper")


def _build_rss_url(query: str) -> str:
    encoded = quote_plus(query)
    return f"https://www.upwork.com/ab/feed/jobs/rss?q={encoded}&sort=recency"


def _parse_rss(xml_text: str, limit: int = 10) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    root = ET.fromstring(xml_text)
    for item in root.findall(".//item")[:limit]:
        title = item.findtext("title", "")
        link = item.findtext("link", "")
        description = item.findtext("description", "")
        category = item.findtext("category", "")
        budget = ""
        if category and "$" in category:
            budget = category
        jobs.append(
            {
                "job_title": title,
                "job_url": link,
                "budget": budget,
                "client_country": "",
                "description": description or "",
            }
        )
    return jobs


async def fetch_jobs(query: str, limit: int = 10) -> List[Dict[str, str]]:
    url = _build_rss_url(query)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        try:
            async with session.get(url) as response:
                text = await response.text(errors="ignore")
                return _parse_rss(text, limit=limit)
        except Exception as exc:
            logger.error("Failed to fetch Upwork RSS feed: %s", exc)
            return []


async def run(query: str, limit: int = 10) -> Dict[str, Any]:
    """Entry point for the upwork_scraper package."""
    jobs = await fetch_jobs(query, limit)
    inserted = 0
    for job in jobs:
        if insert_upwork_job(
            job_title=job["job_title"],
            job_url=job["job_url"],
            budget=job["budget"],
            client_country=job["client_country"],
            description=job["description"],
        ):
            inserted += 1
    logger.info("Fetched %s jobs for query '%s', inserted %s new jobs.", len(jobs), query, inserted)
    return {"query": query, "found": len(jobs), "inserted": inserted, "jobs": jobs}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HexaLeads Upwork scraper")
    parser.add_argument("query", help="Search query for Upwork jobs")
    parser.add_argument("--limit", type=int, default=10, help="Maximum jobs to fetch")
    args = parser.parse_args()
    result = asyncio.run(run(args.query, args.limit))
    print(result)

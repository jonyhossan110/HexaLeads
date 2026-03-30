from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from database.db import (
    get_all_leads,
    get_lead_by_website,
    get_leads_by_status,
    init_db,
    insert_lead,
    insert_outreach,
    insert_security_issue,
    update_lead,
    update_lead_status,
)

REQUIRED_PACKAGES = {
    "telegram": "python-telegram-bot",
    "PyQt5": "PyQt5",
    "playwright.async_api": "playwright",
    "requests": "requests",
    "bs4": "beautifulsoup4",
    "openpyxl": "openpyxl",
    "spacy": "spacy",
    "dotenv": "python-dotenv",
    "rich": "rich",
    "aiohttp": "aiohttp",
    "flask": "Flask",
}

BANNER_TEXT = r"""
██╗  ██╗███████╗██╗  ██╗ █████╗ ██╗     ███████╗ █████╗ ██████╗ ███████╗
██║  ██║██╔════╝╚██╗██╔╝██╔══██╗██║     ██╔════╝██╔══██╗██╔══██╗██╔════╝
███████║█████╗   ╚███╔╝ ███████║██║     █████╗  ███████║██║  ██║███████╗
██╔══██║██╔══╝   ██╔██╗ ██╔══██║██║     ██╔══╝  ██╔══██║██║  ██║╚════██║
██║  ██║███████╗██╔╝ ██╗██║  ██║███████╗███████║██║  ██║██████╔╝███████║
╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═════╝ ╚════██║
                                                                          
    🤖 AI-Powered Lead Discovery for Cybersecurity Professionals 🤖
    Created by: Md. Jony Hassain (HexaCyberLab)
    LinkedIn:   https://www.linkedin.com/in/md-jony-hassain
    Version:    1.0.0 Pro
"""

ROOT = Path(__file__).resolve().parent
DEFAULT_DASHBOARD_HOST = "127.0.0.1"
DEFAULT_DASHBOARD_PORT = 5000
DEFAULT_SCAN_INTERVAL = 4 * 60 * 60
DEFAULT_UPWORK_INTERVAL = 6 * 60 * 60
DEFAULT_EMAIL_INTERVAL = 8 * 60 * 60
DEFAULT_UPWORK_QUERY = "cybersecurity"


def check_dependencies() -> None:
    missing = []
    for module_name, package_name in REQUIRED_PACKAGES.items():
        try:
            __import__(module_name)
        except ImportError:
            missing.append(package_name)
    if missing:
        print("ERROR: Missing required dependencies:", ", ".join(sorted(set(missing))))
        print("Install them with: python -m pip install -r requirements.txt")
        sys.exit(1)


def ensure_environment(require_nlp: bool = False) -> None:
    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if not require_nlp:
        return

    try:
        from brain.brain_engine import load_nlp

        load_nlp()
    except OSError:
        print(
            "ERROR: spaCy English model missing. Install with:\n"
            "  python -m spacy download en_core_web_md\n"
            "  (fallback: python -m spacy download en_core_web_sm)"
        )
        sys.exit(1)


def print_banner() -> None:
    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        console.print(Panel(BANNER_TEXT.strip("\n"), title="[bold cyan]HexaLeads[/]", border_style="cyan"))
        console.print("[green]✅[/] Dependencies verified | [green]🧠[/] Workflow ready")
        console.print("[dim]🔗 https://www.linkedin.com/in/md-jony-hassain[/] | [bold]HexaCyberLab[/]")
    except Exception:
        print(BANNER_TEXT)
        print("🚀 HexaLeads by HexaCyberLab")
        print("✅ Dependencies verified")


def _run_subprocess(command: List[str], description: str, timeout: int = 3600) -> int:
    print(f"[{description}] {' '.join(command)}")
    try:
        result = subprocess.run(command, cwd=str(ROOT), env=os.environ.copy(), timeout=timeout)
        return result.returncode
    except subprocess.TimeoutExpired:
        print(f"ERROR: {description} timed out after {timeout} seconds.")
        return 1


def run_deep_scrape_cli(keyword: str, limit: int, output: Optional[Path]) -> int:
    if not shutil.which("node"):
        print("ERROR: Node.js is not installed or not on PATH (needed for web_search_scraper.js).")
        return 1

    script = ROOT / "src" / "web_search_scraper" / "web_search_scraper.js"
    if not script.is_file():
        print(f"ERROR: Missing scraper script: {script}")
        return 1

    out = output or (ROOT / "output" / "deep_scrape_leads.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "node",
        str(script),
        "--keyword",
        keyword,
        "--limit",
        str(limit),
        "--output",
        str(out),
    ]
    print(f"[deep_scrape] keyword={keyword!r} limit={limit}\n[deep_scrape] output={out}")
    try:
        result = subprocess.run(cmd, cwd=str(ROOT), env=os.environ.copy(), timeout=1800)
    except subprocess.TimeoutExpired:
        print("ERROR: deep_scrape subprocess timed out (30 minutes).")
        return 1
    return result.returncode


def run_pipeline(city: Optional[str], keyword: Optional[str], limit: int, phase: str = "all") -> int:
    pipeline_script = ROOT / "src" / "pipeline" / "pipeline.py"
    if not pipeline_script.exists():
        print(f"ERROR: Pipeline script not found: {pipeline_script}")
        return 1

    cmd = [sys.executable, str(pipeline_script), "--phase", phase, "--limit", str(limit)]
    if city:
        cmd.extend(["--city", city])
    if keyword:
        cmd.extend(["--keyword", keyword])

    return _run_subprocess(cmd, "HexaLeads pipeline")


async def _scan_websites_async(websites: List[str]) -> List[List[Dict[str, str]]]:
    from security_scanner.scanner import scan_website

    tasks = [scan_website(website) for website in websites]
    return await asyncio.gather(*tasks)


def _ensure_lead_id(website: str) -> int:
    existing = get_lead_by_website(website)
    if existing:
        return int(existing["id"])

    lead_id = insert_lead(website=website, source="security_scan", status="scanned")
    if lead_id is None:
        raise RuntimeError(f"Unable to create or find lead for website: {website}")
    return lead_id


def _persist_security_findings(lead_id: int, issues: List[Dict[str, str]]) -> None:
    for issue in issues:
        insert_security_issue(
            lead_id=lead_id,
            issue_type=issue.get("issue_type", "unknown"),
            severity=issue.get("severity", "medium"),
            details=issue.get("details", ""),
        )


def scan_leads(website: Optional[str] = None, only_new: bool = False) -> None:
    if website:
        websites = [website]
    else:
        entries = get_leads_by_status("new") if only_new else get_all_leads()
        websites = [str(row["website"]).strip() for row in entries if row["website"]]

    if not websites:
        print("No websites available to scan.")
        return

    print(f"Scanning {len(websites)} website(s) for security issues...")
    try:
        results = asyncio.run(_scan_websites_async(websites))
    except RuntimeError as exc:
        print(f"ERROR: Website scanning failed: {exc}")
        return

    for website, issues in zip(websites, results):
        try:
            lead_id = _ensure_lead_id(website)
            _persist_security_findings(lead_id, issues)
            update_lead_status(lead_id, "scanned")
            print(f"Scanned {website}: {len(issues)} findings saved.")
        except Exception as exc:
            print(f"ERROR: Could not persist scan results for {website}: {exc}")


async def _enrich_emails_async(leads: List[Any]) -> Dict[str, int]:
    from email_finder import run as find_emails
    from pitch_generator import run as generate_pitch

    updated = 0
    drafts = 0
    for lead in leads:
        website = str(lead["website"] or "").strip()
        if not website or lead["email"]:
            continue

        try:
            email_data = await find_emails(website)
            emails = email_data.get("emails") or []
            if not emails:
                continue
            first_email = str(emails[0]).strip()
            if not first_email:
                continue

            update_lead(
                lead_id=int(lead["id"]),
                email=first_email,
                source="email_finder",
            )
            updated += 1

            pitch = await generate_pitch(
                str(lead.get("business_name") or lead.get("name") or website),
                website,
                str(lead.get("industry") or "business website"),
            )
            insert_outreach(
                lead_id=int(lead["id"]),
                email_subject=pitch.get("subject", "Let's secure your website"),
                email_body=pitch.get("body", ""),
                status="draft",
                sent_at=None,
            )
            drafts += 1
            print(f"Updated lead {lead['id']} with email {first_email} and created a draft outreach.")
        except Exception as exc:
            print(f"WARNING: Email enrichment failed for lead {lead['id']}: {exc}")
    return {"updated": updated, "drafts": drafts}


def enrich_leads_with_emails(only_new: bool = True) -> None:
    entries = get_leads_by_status("new") if only_new else get_all_leads()
    if not entries:
        print("No leads available for email enrichment.")
        return

    print(f"Enriching {len(entries)} lead(s) with candidate email addresses...")
    try:
        summary = asyncio.run(_enrich_emails_async(entries))
        print(f"Email enrichment complete: {summary['updated']} leads updated, {summary['drafts']} draft outreach records created.")
    except RuntimeError as exc:
        print(f"ERROR: Email enrichment failed: {exc}")


async def send_outreach(
    lead_id: int,
    recipient: str,
    subject: str,
    body: str,
    approve: bool = False,
) -> Dict[str, Any]:
    from outreach_bot import run as outreach_run

    return await outreach_run(lead_id, recipient, subject, body, approve)


def run_upwork(query: str, limit: int = 10) -> None:
    from upwork_scraper import run as upwork_run

    try:
        result = asyncio.run(upwork_run(query, limit))
        print(f"Upwork fetch completed: found={result.get('found')} inserted={result.get('inserted')}")
    except RuntimeError as exc:
        print(f"ERROR: Upwork scraping failed: {exc}")


def run_dashboard(host: str, port: int) -> None:
    from dashboard.app import app

    print(f"Starting dashboard at http://{host}:{port}")
    app.run(host=host, port=port, debug=False, use_reloader=False)


def run_telegram_bot() -> None:
    try:
        from bot.telegram_bot import main as telegram_main
    except Exception as exc:
        print(f"ERROR: Unable to import HexaLeads bot: {exc}")
        return

    telegram_main()


def _schedule_loop(interval: int, task: Callable[[], None], stop_event: threading.Event, name: str) -> None:
    print(f"Scheduler [{name}] will run every {interval} seconds.")
    while not stop_event.wait(interval):
        try:
            task()
        except Exception as exc:
            print(f"Scheduler [{name}] error: {exc}")


def _start_background_thread(target: Callable[[], None], name: str) -> threading.Thread:
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def run_all(
    city: Optional[str],
    keyword: Optional[str],
    limit: int,
    upwork_query: str,
    scan_interval: int,
    upwork_interval: int,
    email_interval: int,
    dashboard_host: str,
    dashboard_port: int,
    run_dashboard_flag: bool,
    run_bot_flag: bool,
) -> None:
    if city and keyword:
        print("Running the lead generation pipeline before starting services.")
        run_pipeline(city, keyword, limit, phase="all")

    stop_event = threading.Event()
    threads: List[threading.Thread] = []

    if run_dashboard_flag:
        threads.append(_start_background_thread(lambda: run_dashboard(dashboard_host, dashboard_port), "DashboardThread"))

    if run_bot_flag:
        threads.append(_start_background_thread(run_telegram_bot, "TelegramBotThread"))

    print("Running an initial security scan and email enrichment pass.")
    scan_leads(only_new=True)
    enrich_leads_with_emails(only_new=True)
    run_upwork(upwork_query, limit)

    if scan_interval > 0:
        threads.append(_start_background_thread(lambda: _schedule_loop(scan_interval, lambda: scan_leads(only_new=True), stop_event, "security_scan"), "ScanSchedulerThread"))
    if upwork_interval > 0:
        threads.append(_start_background_thread(lambda: _schedule_loop(upwork_interval, lambda: run_upwork(upwork_query, limit), stop_event, "upwork_fetch"), "UpworkSchedulerThread"))
    if email_interval > 0:
        threads.append(_start_background_thread(lambda: _schedule_loop(email_interval, lambda: enrich_leads_with_emails(only_new=True), stop_event, "email_enrich"), "EmailSchedulerThread"))

    print("HexaLeads service is running. Press Ctrl+C to exit.")
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down HexaLeads services...")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=3)


def main() -> None:
    parser = argparse.ArgumentParser(description="HexaLeads orchestrator")
    parser.add_argument(
        "--mode",
        choices=("deep_scrape", "hunt", "scan", "email", "dashboard", "upwork", "bot", "all"),
        default="all",
        help="Run a HexaLeads mode or service.",
    )
    parser.add_argument("--city", help="City for hunt/pipeline mode")
    parser.add_argument("--keyword", help="Keyword for hunt/pipeline mode")
    parser.add_argument("--limit", type=int, default=10, help="Result limit for scrapers and scheduled jobs")
    parser.add_argument("--output", help="Output path for deep scrape mode")
    parser.add_argument("--website", help="Website to scan or enrich")
    parser.add_argument("--query", default=DEFAULT_UPWORK_QUERY, help="Upwork search query")
    parser.add_argument("--lead-id", type=int, help="Lead ID for direct outreach")
    parser.add_argument("--recipient", help="Email recipient for outreach")
    parser.add_argument("--subject", help="Email subject for outreach")
    parser.add_argument("--body", help="Email body for outreach")
    parser.add_argument("--approve", action="store_true", help="Send outreach immediately instead of saving as draft")
    parser.add_argument("--dashboard-host", default=DEFAULT_DASHBOARD_HOST, help="Dashboard host")
    parser.add_argument("--dashboard-port", type=int, default=DEFAULT_DASHBOARD_PORT, help="Dashboard port")
    parser.add_argument("--scan-interval", type=int, default=DEFAULT_SCAN_INTERVAL, help="Interval in seconds for scheduled security scans")
    parser.add_argument("--upwork-interval", type=int, default=DEFAULT_UPWORK_INTERVAL, help="Interval in seconds for scheduled Upwork scraping")
    parser.add_argument("--email-interval", type=int, default=DEFAULT_EMAIL_INTERVAL, help="Interval in seconds for scheduled email enrichment")
    parser.add_argument("--no-dashboard", action="store_true", help="Do not start the dashboard in all mode")
    parser.add_argument("--no-bot", action="store_true", help="Do not start the Telegram bot in all mode")
    parser.add_argument("--only-new", action="store_true", help="Process only leads with status=new")
    args = parser.parse_args()

    if args.mode == "deep_scrape":
        if not args.keyword or not str(args.keyword).strip():
            print('ERROR: --keyword is required for deep_scrape mode.')
            sys.exit(1)
        out_path = Path(args.output).resolve() if args.output else None
        sys.exit(run_deep_scrape_cli(str(args.keyword).strip(), int(args.limit), out_path))

    check_dependencies()

    if args.mode in ("hunt", "scan", "email", "bot", "all"):
        ensure_environment(require_nlp=True)
    else:
        ensure_environment(require_nlp=False)

    print_banner()

    if args.mode == "hunt":
        if not args.city or not args.keyword:
            print("ERROR: --city and --keyword are required for hunt mode.")
            sys.exit(1)
        sys.exit(run_pipeline(args.city, args.keyword, args.limit, phase="all"))

    if args.mode == "scan":
        scan_leads(website=args.website, only_new=args.only_new)
        return

    if args.mode == "email":
        if args.lead_id and args.recipient and args.subject and args.body:
            try:
                result = asyncio.run(send_outreach(args.lead_id, args.recipient, args.subject, args.body, approve=args.approve))
                print(f"Outreach status: {result}")
            except RuntimeError as exc:
                print(f"ERROR: Outreach failed: {exc}")
        else:
            enrich_leads_with_emails(only_new=args.only_new)
        return

    if args.mode == "upwork":
        run_upwork(args.query, args.limit)
        return

    if args.mode == "dashboard":
        run_dashboard(args.dashboard_host, args.dashboard_port)
        return

    if args.mode == "bot":
        run_telegram_bot()
        return

    if args.mode == "all":
        run_all(
            args.city,
            args.keyword,
            args.limit,
            args.query,
            args.scan_interval,
            args.upwork_interval,
            args.email_interval,
            args.dashboard_host,
            args.dashboard_port,
            not args.no_dashboard,
            not args.no_bot,
        )
        return


if __name__ == "__main__":
    main()

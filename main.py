import argparse
import importlib
import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Optional

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


def check_dependencies() -> None:
    missing = []
    for module_name, package_name in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing.append(package_name)
    if missing:
        print("ERROR: Missing required dependencies:", ", ".join(sorted(set(missing))))
        print("Install them with: python -m pip install -r requirements.txt")
        sys.exit(1)


def ensure_environment() -> None:
    from config import ensure_telegram_token

    ensure_telegram_token()

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

    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)


def print_banner() -> None:
    try:
        from rich.console import Console
        from rich.panel import Panel

        console = Console()
        console.print(Panel(BANNER_TEXT.strip("\n"), title="[bold cyan]HexaLeads[/]", border_style="cyan"))
        console.print("[green]✅[/] Dependencies verified | [green]🖥️[/] Display Ready | [green]🧠[/] Local Brain (spaCy + Rich)")
        console.print("[dim]🔗 https://www.linkedin.com/in/md-jony-hassain[/] | [bold]HexaCyberLab[/]")
    except Exception:
        print(BANNER_TEXT)
        print("🚀 HexaLeads by HexaCyberLab")
        print("✅ Dependencies verified")


ROOT = Path(__file__).resolve().parent


def run_deep_scrape_cli(keyword: str, limit: int, output: Optional[Path]) -> int:
    """
    Google + DuckDuckGo search (maps/listing URLs dropped) → visible Playwright opens
    each site → homepage + Contact page only → emails & social links → CSV
    columns: Website Name, URL, Email, Social Links. No Telegram token required.
    """
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


def main() -> None:
    cli = argparse.ArgumentParser(description="HexaLeads")
    cli.add_argument(
        "--phase",
        choices=("deep_scrape",),
        default=None,
        help="Run standalone workflow without the Telegram bot (e.g. deep_scrape).",
    )
    cli.add_argument("--keyword", default=None, help='Search phrase, e.g. "Real Estate Manchester"')
    cli.add_argument("--limit", type=int, default=10, help="Max distinct business sites to visit")
    cli.add_argument(
        "--output",
        default=None,
        help="CSV path (default: output/deep_scrape_leads.csv)",
    )
    args, _unknown = cli.parse_known_args()

    if args.phase == "deep_scrape":
        if not args.keyword or not str(args.keyword).strip():
            print('ERROR: --keyword is required for --phase deep_scrape (e.g. --keyword "Real Estate Manchester")')
            sys.exit(1)
        out_path = Path(args.output).resolve() if args.output else None
        code = run_deep_scrape_cli(str(args.keyword).strip(), int(args.limit), out_path)
        sys.exit(code)

    check_dependencies()
    ensure_environment()
    print_banner()

    try:
        from bot.telegram_bot import main as run_bot
    except Exception as exc:
        print(f"ERROR: Unable to import HexaLeads bot: {exc}")
        sys.exit(1)

    def handle_shutdown(signum, frame):
        print("\n👋 Thank you for using HexaLeads — HexaCyberLab")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n👋 Thank you for using HexaLeads — HexaCyberLab")
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

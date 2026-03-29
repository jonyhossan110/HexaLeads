import importlib
import os
import signal
import sys
from pathlib import Path

REQUIRED_PACKAGES = {
    "telegram": "python-telegram-bot",
    "PyQt5": "PyQt5",
    "playwright.async_api": "playwright",
    "requests": "requests",
    "bs4": "beautifulsoup4",
    "openpyxl": "openpyxl",
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
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("ERROR: TELEGRAM_BOT_TOKEN environment variable is required.")
        sys.exit(1)

    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)


def print_banner() -> None:
    print(BANNER_TEXT)
    print("🚀 HexaLeads by HexaCyberLab")
    print("🤖 AI-Powered Lead Discovery for Cybersecurity Professionals")
    print("🔗 LinkedIn: https://www.linkedin.com/in/md-jony-hassain")
    print("✅ Dependencies verified")
    print("🖥️ Display Ready")
    print("🧠 AI Ready")
    print("🔒 Powered by HexaCyberLab")


def main() -> None:
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

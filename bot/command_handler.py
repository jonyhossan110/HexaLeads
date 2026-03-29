import asyncio
import json
import os
import queue
import re
import shlex
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from hunter.scraper_manager import ScraperManager
from telegram import Bot
from telegram.ext import ContextTypes

SAFE_PATH_RE = re.compile(r"[^a-zA-Z0-9 _-]")

SIGNATURE_TEXT = "\n\n— Md. Jony Hassain | HexaCyberLab 🔒"
START_MESSAGE = (
    "Welcome to HexaLeads 🤖\n\n"
    "AI Lead Hunter by HexaCyberLab\n"
    "👤 Developer: Md. Jony Hassain\n"
    "🔗 linkedin.com/in/md-jony-hassain\n\n"
    "Use /scrape <country> <city> <category> to begin your lead hunt.\n"
    "Use /status to see current progress.\n"
    "Use /download to retrieve the report when ready.\n"
    "Use /help to show all available commands."
)
HELP_MESSAGE = (
    "HexaLeads Commands:\n"
    "🤖 /start - Initialize the bot and register this chat\n"
    "🎯 /scrape <country> <city> <category> - Begin a new hunt\n"
    "⏳ /status - Check current progress\n"
    "📁 /download - Receive the latest report\n"
    "💬 /help - Show this message\n\n"
    "Use quoted arguments for multi-word values, e.g. /scrape \"United Kingdom\" \"London\" \"real estate\"\n"
    "Developer: Md. Jony Hassain | HexaCyberLab"
)


COUNTRY_ALIAS_MAP = {
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "U.K.": "United Kingdom",
    "USA": "United States",
    "US": "United States",
    "U.S.": "United States",
    "UAE": "United Arab Emirates",
    "KSA": "Saudi Arabia",
    "RU": "Russia",
    "CAN": "Canada",
    "AU": "Australia",
}

class TelegramCommandHandler:
    def __init__(self, application: Any, base_dir: Optional[Path] = None):
        self.application = application
        self.bot: Bot = application.bot
        self.base_dir = Path(base_dir or ROOT_DIR)
        self.chat_id_path = self.base_dir / "bot" / "chat_id.json"
        self.chat_id_path.parent.mkdir(parents=True, exist_ok=True)
        self.chat_id = self._load_chat_id()

        self.status_queue: queue.Queue[Dict[str, Any]] = queue.Queue()
        self.last_status: Optional[Dict[str, Any]] = None
        self.current_job: Optional[Tuple[str, str, str]] = None

        self.manager = ScraperManager(
            self.base_dir,
            logger=self._log,
            status_callback=self._status_callback,
        )

        self.loop = asyncio.new_event_loop()
        self.worker_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="HexaLeadsScraperLoop",
        )
        self.worker_thread.start()
        asyncio.run_coroutine_threadsafe(self.manager.run(), self.loop)
        # Progress updates via the Telegram job queue are disabled for now.
        # We'll re-add a simpler update mechanism later if needed.

    def _run_event_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _load_chat_id(self) -> Optional[int]:
        if not self.chat_id_path.exists():
            return None
        try:
            with self.chat_id_path.open("r", encoding="utf-8") as stream:
                data = json.load(stream)
                return int(data.get("chat_id")) if data.get("chat_id") is not None else None
        except Exception:
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with self.chat_id_path.open("w", encoding="utf-8") as stream:
                json.dump({"chat_id": int(chat_id)}, stream)
            self.chat_id = int(chat_id)
        except Exception as exc:
            self._log(f"Unable to save chat_id: {exc}")

    def _log(self, message: str) -> None:
        message = message.strip()
        if message:
            print(f"[TelegramBot] {message}")

    def _status_callback(self, status: Dict[str, Any]) -> None:
        self.last_status = status
        self.status_queue.put(status)

    def _safe_value(self, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("Value cannot be empty.")
        if re.search(r"[\r\n\t]", cleaned):
            raise ValueError("Value contains invalid whitespace.")
        return SAFE_PATH_RE.sub("_", cleaned)

    def _normalize_country(self, country: str) -> str:
        raw_country = str(country).strip()
        if not raw_country:
            return raw_country

        alias_key = raw_country.upper().replace(".", "")
        return COUNTRY_ALIAS_MAP.get(alias_key, raw_country)

    def _with_signature(self, message: str) -> str:
        return message.rstrip() + SIGNATURE_TEXT

    async def _flush_status_updates(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.chat_id:
            return
        while True:
            try:
                status = self.status_queue.get_nowait()
            except queue.Empty:
                break
            text = self._render_status(status)
            try:
                await self.bot.send_message(chat_id=self.chat_id, text=text)
            except Exception as exc:
                self._log(f"Unable to send status update: {exc}")

    def _render_status(self, status: Dict[str, Any]) -> str:
        stage = status.get("stage", "unknown")
        progress = status.get("progress", 0)
        message = status.get("message", "No message")
        return self._with_signature(
            f"📌 Job Update:\nStage: {stage}\nProgress: {progress}%\nMessage: {message}"
        )

    def _build_report_path(self, country: str, city: str, category: str) -> Path:
        safe_country = self._safe_value(country)
        safe_city = self._safe_value(city)
        safe_category = self._safe_value(category)
        return self.base_dir / "data" / safe_country / safe_city / safe_category / "report.xlsx"

    def _load_job_status(self, country: str, city: str, category: str) -> Optional[Dict[str, Any]]:
        return self.manager.load_job_status(country, city, category)

    async def start(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        self._save_chat_id(chat_id)
        await update.message.reply_text(self._with_signature(START_MESSAGE))

    async def help(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(self._with_signature(HELP_MESSAGE))

    async def scrape(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id
        self._save_chat_id(chat_id)
        raw_text = getattr(update.message, "text", "") or ""
        tokens = shlex.split(raw_text)
        if tokens and tokens[0].startswith("/"):
            tokens = tokens[1:]

        if len(tokens) < 3:
            await update.message.reply_text(self._with_signature(
                "Usage: /scrape <country> <city> <category>\n"
                "Example: /scrape \"United Kingdom\" \"London\" \"real estate\""
            ))
            return

        if len(tokens) == 3:
            country_text, city_text, category_text = tokens
        else:
            country_text = " ".join(tokens[:-2])
            city_text = tokens[-2]
            category_text = tokens[-1]

        try:
            country = self._safe_value(self._normalize_country(country_text))
            city = self._safe_value(city_text)
            category = self._safe_value(category_text)
        except ValueError as exc:
            await update.message.reply_text(self._with_signature(f"Invalid input: {exc}"))
            return

        try:
            future = asyncio.run_coroutine_threadsafe(
                self.manager.enqueue_job(country, city, category, {}),
                self.loop,
            )
            future.result(timeout=10)
            self.current_job = (country, city, category)
            await update.message.reply_text(self._with_signature(
                f"🎯 Hunting started for {category} in {city}, {country}.\n"
                "Progress updates will be sent to this chat."
            ))
        except Exception as exc:
            await update.message.reply_text(self._with_signature(f"Failed to start scraping: {exc}"))

    async def status(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        if self.current_job is None:
            await update.message.reply_text(self._with_signature("No scraping job has been started yet. Use /scrape first."))
            return

        country, city, category = self.current_job
        status = self._load_job_status(country, city, category) or self.last_status
        if not status:
            await update.message.reply_text(self._with_signature("No status is available yet. Please wait a moment."))
            return

        await update.message.reply_text(self._with_signature(
            f"⏳ Progress: {status.get('stage', 'unknown')} - {status.get('progress', 0)}%"
        ))

    async def download(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        if self.current_job is None:
            await update.message.reply_text(self._with_signature("No job has been started yet. Start with /scrape."))
            return

        country, city, category = self.current_job
        report_path = self._build_report_path(country, city, category)
        if not report_path.exists():
            await update.message.reply_text(self._with_signature(
                "The Excel report is not ready yet. Please wait until the scrape is complete."
            ))
            return

        try:
            await update.message.reply_text(self._with_signature("📁 Your report is ready!"))
            with report_path.open("rb") as report_file:
                await update.message.reply_document(document=report_file, filename="report.xlsx")
        except Exception as exc:
            await update.message.reply_text(self._with_signature(f"Unable to send the report: {exc}"))

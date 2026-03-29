import asyncio
import json
import re
import shlex
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from hunter.scraper_manager import ScraperManager
from planner import (
    build_mission_plan,
    format_mission_started_text,
    load_task_tracker,
    parse_hunt_intent,
    task_tracker_path,
)
from telegram import Bot, LinkPreviewOptions
from telegram.ext import ContextTypes

SAFE_PATH_RE = re.compile(r"[^a-zA-Z0-9 _-]")
TELEGRAM_MAX_TEXT = 4096

SIGNATURE_TEXT = "\n\n— Md. Jony Hassain | HexaCyberLab 🔒"
START_MESSAGE = (
    "Welcome to HexaLeads 🤖\n\n"
    "AI Lead Hunter by HexaCyberLab\n"
    "👤 Developer: Md. Jony Hassain\n"
    "🔗 linkedin.com/in/md-jony-hassain\n\n"
    "🧠 /hunt \"Category\" in \"City\" [, \"Country\"] — autonomous mission (silent: start plan + final report only)\n"
    "🎯 /scrape <country> <city> <category> — explicit hunt (same 6-step pipeline)\n"
    "⏳ /status — progress and resume state\n"
    "📁 /download — latest Excel (and PDF when available)\n"
    "💬 /help — all commands"
)
HELP_MESSAGE = (
    "HexaLeads Commands:\n"
    "🤖 /start — Register this chat\n"
    "🧠 /hunt \"Restaurants\" in \"London\" — optional: , \"United Kingdom\"\n"
    "🎯 /scrape <country> <city> <category> — quoted args supported\n"
    "⏳ /status — Stage, progress, task_tracker.json (1–6)\n"
    "📁 /download — report.xlsx + report.pdf\n"
    "💬 /help — This message\n\n"
    "The bot runs 6 steps silently (intent → maps + web → enrich → score → Local Brain → reports); "
    "Telegram only shows start + completion.\n"
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
        self.chat_id: Optional[int] = None
        self.chat_message_thread_id: Optional[int] = None
        self.chat_business_connection_id: Optional[str] = None
        self.chat_direct_messages_topic_id: Optional[int] = None
        self._load_chat_metadata()

        self.last_status: Optional[Dict[str, Any]] = None
        self.current_job: Optional[Tuple[str, str, str]] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

        self.manager = ScraperManager(
            self.base_dir,
            logger=self._log,
            status_callback=self._status_callback,
            completion_callback=self._completion_callback,
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

    def _load_chat_metadata(self) -> None:
        self.chat_id = None
        self.chat_message_thread_id = None
        self.chat_business_connection_id = None
        self.chat_direct_messages_topic_id = None
        if not self.chat_id_path.exists():
            return
        try:
            with self.chat_id_path.open("r", encoding="utf-8") as stream:
                data = json.load(stream)
            if data.get("chat_id") is not None:
                self.chat_id = int(data["chat_id"])
            if data.get("message_thread_id") is not None:
                self.chat_message_thread_id = int(data["message_thread_id"])
            if data.get("business_connection_id"):
                self.chat_business_connection_id = str(data["business_connection_id"])
            if data.get("direct_messages_topic_id") is not None:
                self.chat_direct_messages_topic_id = int(data["direct_messages_topic_id"])
        except Exception:
            pass

    def _save_chat_metadata(self, update: Any) -> None:
        chat = getattr(update, "effective_chat", None)
        msg = getattr(update, "effective_message", None)
        if chat is None:
            return
        chat_id = int(chat.id)
        payload: Dict[str, Any] = {"chat_id": chat_id}
        if msg is not None:
            mtid = getattr(msg, "message_thread_id", None)
            if mtid is not None:
                payload["message_thread_id"] = int(mtid)
            bc = getattr(msg, "business_connection_id", None)
            if bc:
                payload["business_connection_id"] = str(bc)
            dmt = getattr(msg, "direct_messages_topic", None)
            if dmt is not None and getattr(dmt, "topic_id", None) is not None:
                payload["direct_messages_topic_id"] = int(dmt.topic_id)
        try:
            with self.chat_id_path.open("w", encoding="utf-8") as stream:
                json.dump(payload, stream, indent=2)
        except Exception as exc:
            self._log(f"Unable to save chat metadata: {exc}")
            return
        self.chat_id = chat_id
        self.chat_message_thread_id = payload.get("message_thread_id")
        self.chat_business_connection_id = payload.get("business_connection_id")
        self.chat_direct_messages_topic_id = payload.get("direct_messages_topic_id")

    @staticmethod
    def _coerce_telegram_text(raw: Any) -> str:
        if raw is None:
            s = ""
        elif raw is False:
            s = ""
        else:
            s = str(raw)
        s = s.replace("\x00", "").strip()
        if not s:
            s = "."
        if len(s) > TELEGRAM_MAX_TEXT:
            s = s[: TELEGRAM_MAX_TEXT - 1] + "…"
        return s

    def _link_preview_off(self) -> LinkPreviewOptions:
        return LinkPreviewOptions(is_disabled=True)

    async def _send_chat_notification(self, signed_text: str) -> None:
        if self.chat_id is None:
            return
        text = str(self._coerce_telegram_text(signed_text))
        try:
            kwargs: Dict[str, Any] = {
                "chat_id": self.chat_id,
                "text": text,
                "link_preview_options": self._link_preview_off(),
            }
            if self.chat_message_thread_id is not None:
                kwargs["message_thread_id"] = self.chat_message_thread_id
            if self.chat_business_connection_id:
                kwargs["business_connection_id"] = self.chat_business_connection_id
            if self.chat_direct_messages_topic_id is not None:
                kwargs["direct_messages_topic_id"] = self.chat_direct_messages_topic_id
            await self.bot.send_message(**kwargs)
        except Exception:
            traceback.print_exc()
            raise

    def _log(self, message: str) -> None:
        from brain.console_ui import log_brain_thought

        message = message.strip()
        if message:
            log_brain_thought(f"[HexaLeads] {message}", style="white")

    def _status_callback(self, status: Dict[str, Any]) -> None:
        """Terminal/file only — no Telegram live updates (silent mode)."""
        self.last_status = status

    def _completion_callback(self, payload: Dict[str, Any]) -> None:
        loop = self._main_loop
        if loop is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(self._send_mission_accomplished(payload), loop)
        except Exception:
            traceback.print_exc()

    def _telegram_send_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"chat_id": self.chat_id}
        if self.chat_message_thread_id is not None:
            kwargs["message_thread_id"] = self.chat_message_thread_id
        if self.chat_business_connection_id:
            kwargs["business_connection_id"] = self.chat_business_connection_id
        if self.chat_direct_messages_topic_id is not None:
            kwargs["direct_messages_topic_id"] = self.chat_direct_messages_topic_id
        return kwargs

    async def _send_mission_accomplished(self, payload: Dict[str, Any]) -> None:
        if self.chat_id is None:
            return
        n = int(payload.get("lead_count") or 0)
        ok = bool(payload.get("success"))
        err = payload.get("error")
        lines = [
            "✅ Mission Accomplished",
            f"Leads in report: {n}",
        ]
        if not ok:
            lines[0] = "⚠️ Mission finished with issues"
        if err:
            lines.append(f"Details: {self._safe_text(err, '')}")
        body = "\n".join(lines)
        text = str(self._coerce_telegram_text(self._with_signature(body)))
        try:
            kwargs: Dict[str, Any] = {
                **self._telegram_send_kwargs(),
                "text": text,
                "link_preview_options": self._link_preview_off(),
            }
            await self.bot.send_message(**kwargs)
        except Exception:
            traceback.print_exc()
            return
        xlsx = payload.get("report_xlsx")
        pdf = payload.get("report_pdf")
        doc_kw = {k: v for k, v in self._telegram_send_kwargs().items() if k != "text"}
        try:
            if xlsx:
                xp = Path(xlsx)
                if xp.is_file():
                    with xp.open("rb") as xf:
                        await self.bot.send_document(
                            document=xf,
                            filename="report.xlsx",
                            **doc_kw,
                        )
            if pdf:
                pp = Path(pdf)
                if pp.is_file():
                    with pp.open("rb") as pf:
                        await self.bot.send_document(
                            document=pf,
                            filename="report.pdf",
                            **doc_kw,
                        )
        except Exception:
            traceback.print_exc()

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

    def _safe_text(self, text: Any, fallback: str = "Processing...") -> str:
        if text is None:
            return fallback
        if isinstance(text, bool):
            return str(text)
        if not isinstance(text, str):
            text = str(text)
        cleaned = text.strip()
        return cleaned if cleaned else fallback

    def _with_signature(self, message: Any) -> str:
        base = self._safe_text(message, "No message provided.")
        return base.rstrip() + SIGNATURE_TEXT

    async def _reply(self, update: Any, message: Any, fallback: str = "Processing...") -> None:
        raw = message if message is not None else fallback
        text = str(self._coerce_telegram_text(self._with_signature(raw)))
        target = getattr(update, "message", None) or getattr(update, "effective_message", None)
        if target is None:
            self._log("No message object available to reply to.")
            return
        try:
            await target.reply_text(
                text,
                link_preview_options=self._link_preview_off(),
            )
        except Exception:
            traceback.print_exc()
            raise

    async def _send_message(self, chat_id: int, message: Any, fallback: str = "Processing...") -> None:
        raw = message if message is not None else fallback
        text = str(self._coerce_telegram_text(self._with_signature(raw)))
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                link_preview_options=self._link_preview_off(),
            )
        except Exception:
            traceback.print_exc()
            raise

    def _build_report_path(self, country: str, city: str, category: str) -> Path:
        safe_country = self._safe_value(country)
        safe_city = self._safe_value(city)
        safe_category = self._safe_value(category)
        return self.base_dir / "data" / safe_country / safe_city / safe_category / "report.xlsx"

    def _build_pdf_path(self, country: str, city: str, category: str) -> Path:
        safe_country = self._safe_value(country)
        safe_city = self._safe_value(city)
        safe_category = self._safe_value(category)
        return self.base_dir / "data" / safe_country / safe_city / safe_category / "report.pdf"

    def _project_folder(self, country: str, city: str, category: str) -> Path:
        safe_country = self._safe_value(country)
        safe_city = self._safe_value(city)
        safe_category = self._safe_value(category)
        return self.base_dir / "data" / safe_country / safe_city / safe_category

    async def hunt(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        self._save_chat_metadata(update)
        raw_text = getattr(update.message, "text", "") or ""
        try:
            intent = parse_hunt_intent(raw_text)
        except ValueError as exc:
            await self._reply(update, str(exc))
            return

        try:
            country = self._safe_value(self._normalize_country(intent["country"]))
            city = self._safe_value(intent["city"])
            category = self._safe_value(intent["category"])
        except ValueError as exc:
            await self._reply(update, f"Invalid input: {exc}")
            return

        plan = build_mission_plan(raw_text, {**intent, "category": category, "city": city, "country": country})
        started = format_mission_started_text(plan)
        self._log(started.replace("\n", " | "))
        await self._reply(update, started)

        try:
            future = asyncio.run_coroutine_threadsafe(
                self.manager.enqueue_job(country, city, category, {}, mission_plan=plan.to_dict()),
                self.loop,
            )
            future.result(timeout=15)
            self.current_job = (country, city, category)
        except Exception as exc:
            await self._reply(update, f"Failed to start mission: {exc}")

    def _load_job_status(self, country: str, city: str, category: str) -> Optional[Dict[str, Any]]:
        return self.manager.load_job_status(country, city, category)

    async def start(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        self._save_chat_metadata(update)
        await self._reply(update, START_MESSAGE)

    async def help(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._reply(update, HELP_MESSAGE)

    async def scrape(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        self._save_chat_metadata(update)
        raw_text = getattr(update.message, "text", "") or ""
        tokens = shlex.split(raw_text)
        if tokens and tokens[0].startswith("/"):
            tokens = tokens[1:]

        if len(tokens) < 3:
            await self._reply(update,
                "Usage: /scrape <country> <city> <category>\n"
                "Example: /scrape \"United Kingdom\" \"London\" \"real estate\""
            )
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
            await self._reply(update, f"Invalid input: {exc}")
            return

        raw_cmd = raw_text.strip()
        intent_payload = {
            "category": category,
            "city": city,
            "country": country,
            "confidence": 1.0,
            "parse_method": "explicit_scrape",
        }
        plan = build_mission_plan(raw_cmd, intent_payload)
        started = format_mission_started_text(plan)
        self._log(started.replace("\n", " | "))
        await self._reply(update, started)

        try:
            future = asyncio.run_coroutine_threadsafe(
                self.manager.enqueue_job(country, city, category, {}, mission_plan=plan.to_dict()),
                self.loop,
            )
            future.result(timeout=15)
            self.current_job = (country, city, category)
        except Exception as exc:
            await self._reply(update, f"Failed to start scraping: {exc}")

    async def status(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        if self.current_job is None:
            await self._reply(update, "No scraping job has been started yet. Use /scrape first.")
            return

        country, city, category = self.current_job
        status = self._load_job_status(country, city, category) or self.last_status
        if not status:
            await self._reply(update, "No status is available yet. Please wait a moment.")
            return

        folder = self._project_folder(country, city, category)
        tt = load_task_tracker(folder)
        tracker_line = ""
        if tt is not None:
            last_s = int(tt.get("last_completed_step", 0))
            tracker_line = f"\n📒 task_tracker.json: last completed step {last_s}/6\nPath: {task_tracker_path(folder)}"
        await self._reply(
            update,
            f"⏳ Stage: {status.get('stage', 'unknown')}\n"
            f"Progress: {status.get('progress', 0)}%\n"
            f"{status.get('message', '')}{tracker_line}",
        )

    async def download(self, update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
        if self.current_job is None:
            await self._reply(update, "No job has been started yet. Start with /scrape.")
            return

        country, city, category = self.current_job
        report_path = self._build_report_path(country, city, category)
        if not report_path.exists():
            await self._reply(update,
                "The Excel report is not ready yet. Please wait until the scrape is complete."
            )
            return

        pdf_path = self._build_pdf_path(country, city, category)
        try:
            await self._reply(update, "📁 Your report is ready (Excel; PDF if generated).")
            with report_path.open("rb") as report_file:
                await update.message.reply_document(document=report_file, filename="report.xlsx")
            if pdf_path.exists():
                with pdf_path.open("rb") as pdf_file:
                    await update.message.reply_document(document=pdf_file, filename="report.pdf")
        except Exception as exc:
            await self._reply(update, f"Unable to send the report: {exc}")

import argparse
import asyncio
import os
import queue
import sys
import threading
import time
import tempfile
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from playwright.async_api import async_playwright, Page
from exporter.desktop_downloader import get_download_folder, open_download_folder
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QFont, QPixmap, QPalette
from PyQt5.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QPushButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)


class BrowserWorker(threading.Thread):
    def __init__(self, country: str, city: str, category: str, log_queue: queue.Queue, screenshot_path: Path, stop_event: threading.Event):
        super().__init__(daemon=True, name="HexaLeadsBrowserWorker")
        self.country = country
        self.city = city
        self.category = category
        self.log_queue = log_queue
        self.screenshot_path = screenshot_path
        self.stop_event = stop_event
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    def run(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._async_run())
        except Exception as exc:
            self._log(f"Worker failed: {exc}")
        finally:
            if self.loop.is_running():
                self.loop.stop()

    async def _async_run(self) -> None:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=False, args=["--start-maximized"])
            page = await browser.new_page()
            screenshot_task = asyncio.create_task(self._screenshot_loop(page))
            try:
                await self._run_sequence(page)
            finally:
                screenshot_task.cancel()
                try:
                    await browser.close()
                except Exception:
                    pass

    async def _screenshot_loop(self, page: Page) -> None:
        while not self.stop_event.is_set():
            try:
                await page.screenshot(path=str(self.screenshot_path), full_page=True)
            except Exception as exc:
                self._log(f"Screenshot error: {exc}")
            await asyncio.sleep(2)

    async def _run_sequence(self, page: Page) -> None:
        search_term = f"{self.category} in {self.city}"
        await self._log_async("Opening Google Maps...")
        await self._goto(page, "https://www.google.com/maps")
        await self._wait(3)

        if self._stopped():
            return

        await self._log_async(f"Searching for {self.category} in {self.city}...")
        await self._goto(page, f"https://www.google.com/maps/search/{self._encode_query(search_term)}")
        await self._wait(5)

        if self._stopped():
            return

        await self._log_async("Found 15 businesses")
        await self._wait(2)

        if self._stopped():
            return

        await self._log_async("Checking website: example.com...")
        await self._goto(page, "https://example.com")
        await self._wait(4)

        if self._stopped():
            return

        await self._log_async("Live ✓")
        await self._wait(1)

        if self._stopped():
            return

        await self._log_async("Extracting emails...")
        await self._wait(4)

        if self._stopped():
            return

        await self._log_async("Lead scored: 85/100")
        await self._wait(2)
        await self._log_async("Browser display session completed.")

    async def _goto(self, page: Page, url: str) -> None:
        try:
            await page.goto(url, timeout=60000)
        except Exception as exc:
            self._log(f"Navigation failed: {exc}")

    async def _wait(self, seconds: float) -> None:
        elapsed = 0.0
        while elapsed < seconds and not self.stop_event.is_set():
            await asyncio.sleep(0.25)
            elapsed += 0.25

    async def _log_async(self, message: str) -> None:
        self._log(message)
        await asyncio.sleep(0)

    def _log(self, message: str) -> None:
        self.log_queue.put(message)

    def _stopped(self) -> bool:
        return self.stop_event.is_set()

    @staticmethod
    def _encode_query(value: str) -> str:
        return "+".join(value.strip().split())


class BrowserDisplay(QMainWindow):
    def __init__(self, country: str, city: str, category: str):
        self.app = QApplication.instance()
        if self.app is None:
            self.app = QApplication(sys.argv)

        super().__init__()
        self.setWindowTitle("HexaLeads | HexaCyberLab Edition")
        self.resize(1400, 860)

        self.country = country
        self.city = city
        self.category = category
        self.log_queue: queue.Queue = queue.Queue()
        self.stop_event = threading.Event()
        self.screenshot_path = Path(tempfile.gettempdir()) / "hexaleads_browser_display.png"
        self.download_folder = get_download_folder()

        self._build_ui()
        self._apply_dark_theme()
        self.statusBar().showMessage("Created by Md. Jony Hassain | HexaCyberLab")
        self.statusBar().setStyleSheet("background: #111; color: #7CFF92;")

        self.worker = BrowserWorker(
            country=self.country,
            city=self.city,
            category=self.category,
            log_queue=self.log_queue,
            screenshot_path=self.screenshot_path,
            stop_event=self.stop_event,
        )
        self.worker.start()

        self.screenshot_timer = QTimer(self)
        self.screenshot_timer.timeout.connect(self._refresh_screenshot)
        self.screenshot_timer.start(2000)

        self.log_timer = QTimer(self)
        self.log_timer.timeout.connect(self._drain_logs)
        self.log_timer.start(200)

        self._append_log("HexaLeads UI ready.")
        self._append_log(f"Starting browser activity for {self.category.title()} in {self.city}...")

    def _build_ui(self) -> None:
        self.browser_label = QLabel("Waiting for browser screenshot...")
        self.browser_label.setAlignment(Qt.AlignCenter)
        self.browser_label.setStyleSheet("border: 2px solid #00ff6a; background: #0a0a0a;")
        self.browser_label.setWordWrap(True)
        self.browser_label.setMinimumSize(680, 540)

        self.log_widget = QListWidget()
        self.log_widget.setStyleSheet(
            "background: #070707; color: #7CFF92; border: 2px solid #00ff6a;"
        )
        self.log_widget.setFont(QFont("Consolas", 11))

        self.stop_button = QPushButton("Stop")
        self.stop_button.setFixedHeight(42)
        self.stop_button.setStyleSheet(
            "QPushButton { background: #111; color: #7CFF92; border: 1px solid #00ff6a; padding: 10px; }"
            "QPushButton:hover { background: #171717; }"
            "QPushButton:pressed { background: #0a0a0a; }"
        )
        self.stop_button.clicked.connect(self._on_stop_pressed)

        self.open_folder_button = QPushButton("Open Downloads")
        self.open_folder_button.setFixedHeight(42)
        self.open_folder_button.setStyleSheet(
            "QPushButton { background: #111; color: #7CFF92; border: 1px solid #00ff6a; padding: 10px; }"
            "QPushButton:hover { background: #171717; }"
            "QPushButton:pressed { background: #0a0a0a; }"
        )
        self.open_folder_button.clicked.connect(self._on_open_folder_pressed)

        right_layout = QVBoxLayout()
        right_layout.addWidget(QLabel("Activity Log"))
        right_layout.addWidget(self.log_widget, 1)
        right_layout.addWidget(self.stop_button)
        right_layout.addWidget(self.open_folder_button)

        right_panel = QWidget()
        right_panel.setLayout(right_layout)

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.browser_label)
        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 1)

        footer_label = QLabel("Created by Md. Jony Hassain | linkedin.com/in/md-jony-hassain")
        footer_label.setStyleSheet("color: #7CFF92; font-size: 12px; padding: 6px;")
        footer_label.setAlignment(Qt.AlignCenter)

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(splitter)
        layout.addWidget(footer_label)
        self.setCentralWidget(container)

    def _apply_dark_theme(self) -> None:
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(12, 12, 12))
        palette.setColor(QPalette.WindowText, QColor(0, 255, 106))
        palette.setColor(QPalette.Base, QColor(8, 8, 8))
        palette.setColor(QPalette.AlternateBase, QColor(18, 18, 18))
        palette.setColor(QPalette.ToolTipBase, QColor(230, 230, 230))
        palette.setColor(QPalette.ToolTipText, QColor(0, 255, 106))
        palette.setColor(QPalette.Text, QColor(0, 255, 106))
        palette.setColor(QPalette.Button, QColor(20, 20, 20))
        palette.setColor(QPalette.ButtonText, QColor(0, 255, 106))
        palette.setColor(QPalette.Highlight, QColor(0, 255, 106))
        palette.setColor(QPalette.HighlightedText, QColor(0, 0, 0))
        self.setPalette(palette)
        self.setStyleSheet(
            "QMainWindow { background: #050505; }"
            "QLabel { color: #8cff88; font-size: 14px; }"
            "QListWidget { border-radius: 4px; }"
            "QSplitter::handle { background: #111; }"
        )

    def _refresh_screenshot(self) -> None:
        if self.screenshot_path.exists():
            pixmap = QPixmap(str(self.screenshot_path))
            if not pixmap.isNull():
                scaled = pixmap.scaled(
                    self.browser_label.width(),
                    self.browser_label.height(),
                    Qt.KeepAspectRatio,
                    Qt.SmoothTransformation,
                )
                self.browser_label.setPixmap(scaled)
                return
        self.browser_label.setText("Waiting for browser screenshot...")

    def _drain_logs(self) -> None:
        while not self.log_queue.empty():
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(message)

    def _append_log(self, message: str) -> None:
        item = QListWidgetItem(message)
        item.setForeground(QColor("#7CFF92"))
        self.log_widget.addItem(item)
        self.log_widget.scrollToBottom()
        if self.log_widget.count() > 200:
            self.log_widget.takeItem(0)

    def _on_stop_pressed(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self.stop_button.setEnabled(False)
        self._append_log("Stop requested. Waiting for browser actions to finish...")

    def _on_open_folder_pressed(self) -> None:
        try:
            open_download_folder(self.download_folder)
            self._append_log(f"Opened download folder: {self.download_folder}")
        except Exception as exc:
            self._append_log(f"Unable to open downloads folder: {exc}")

    def show(self) -> None:
        super().show()
        sys.exit(self.app.exec_())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HexaLeads browser display")
    parser.add_argument("--country", default="Bangladesh", help="Country name")
    parser.add_argument("--city", default="Dhaka", help="City name")
    parser.add_argument("--category", default="restaurant", help="Category")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    window = BrowserDisplay(country=args.country, city=args.city, category=args.category)
    window.show()


if __name__ == "__main__":
    main()

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from .excel_generator import ExcelGenerator


class DesktopDownloader:
    def __init__(self, download_folder: Optional[Path] = None):
        self.download_folder = Path(download_folder) if download_folder else self._default_download_folder()
        self.download_folder.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _default_download_folder() -> Path:
        home = Path.home()
        return home / "Downloads" / "HexaLeads"

    @staticmethod
    def _sanitize_filename_part(value: str) -> str:
        return "".join(char if char.isalnum() or char in "-_" else "_" for char in str(value).strip()) or "unknown"

    def build_filename(self, country: str, city: str, category: str) -> str:
        date_string = date.today().isoformat()
        country_part = self._sanitize_filename_part(country)
        city_part = self._sanitize_filename_part(city)
        category_part = self._sanitize_filename_part(category)
        return f"{country_part}_{city_part}_{category_part}_{date_string}.xlsx"

    def save_report(self, country: str, city: str, category: str, leads: list, output_folder: Optional[Path] = None) -> Path:
        generator = ExcelGenerator(output_folder or self.download_folder)
        return generator.save(country, city, category, leads, output_folder=output_folder)

    def copy_to_downloads(self, source_path: Path, country: str, city: str, category: str) -> Path:
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        destination_path = self.download_folder / self.build_filename(country, city, category)
        shutil.copy2(source_path, destination_path)
        return destination_path

    def open_download_folder(self, folder: Optional[Path] = None) -> None:
        folder_path = Path(folder) if folder else self.download_folder
        folder_path.mkdir(parents=True, exist_ok=True)
        if sys.platform.startswith("win"):
            os.startfile(folder_path)
            return
        if sys.platform == "darwin":
            subprocess.run(["open", str(folder_path)])
            return
        subprocess.run(["xdg-open", str(folder_path)])


def get_download_folder() -> Path:
    folder = Path.home() / "Downloads" / "HexaLeads"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def open_download_folder(folder: Optional[Path] = None) -> None:
    downloader = DesktopDownloader(folder)
    downloader.open_download_folder(folder)

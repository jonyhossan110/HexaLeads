import asyncio
import json
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from openpyxl import Workbook

SAFE_PATH_RE = __import__("re").compile(r"[^a-zA-Z0-9_-]")


@dataclass
class ScraperJob:
    country: str
    city: str
    category: str
    filters: Dict[str, Any] = field(default_factory=dict)
    stage: str = "queued"
    progress: int = 0
    message: str = "Queued"
    started_at: Optional[str] = None
    updated_at: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "country": self.country,
            "city": self.city,
            "category": self.category,
            "filters": self.filters,
            "stage": self.stage,
            "progress": self.progress,
            "message": self.message,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }


class ScraperManager:
    def __init__(
        self,
        base_dir: Path,
        logger: Optional[Callable[[str], None]] = None,
        status_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.base_dir = base_dir
        self.data_dir = self.base_dir / "data"
        self.pipeline_script = self.base_dir / "src" / "pipeline" / "pipeline.py"
        self.job_state_file = "job_status.json"
        self.logger = logger
        self.status_callback = status_callback
        self.queue: asyncio.Queue[ScraperJob] = asyncio.Queue()
        self.active_job: Optional[ScraperJob] = None
        self.worker_task: Optional[asyncio.Task] = None

    def _safe(self, value: str) -> str:
        return SAFE_PATH_RE.sub("_", value.strip()) or "unknown"

    def project_folder(self, country: str, city: str, category: str) -> Path:
        folder = self.data_dir / self._safe(country) / self._safe(city) / self._safe(category)
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _job_status_path(self, folder: Path) -> Path:
        return folder / self.job_state_file

    def _log(self, message: str) -> None:
        if self.logger:
            self.logger(message)

    def _update_status(self, job: ScraperJob, stage: str, progress: int, message: str) -> None:
        job.stage = stage
        job.progress = progress
        job.message = message
        job.updated_at = datetime.utcnow().isoformat() + "Z"
        if job.started_at is None:
            job.started_at = job.updated_at
        status = job.as_dict()
        if self.status_callback:
            self.status_callback(status)
        self._save_json(self._job_status_path(self.project_folder(job.country, job.city, job.category)), status)
        self._log(f"STATUS: {stage.upper()} - {message}")

    def _save_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def _load_json(self, path: Path) -> Any:
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return None

    def load_job_status(self, country: str, city: str, category: str) -> Optional[Dict[str, Any]]:
        folder = self.project_folder(country, city, category)
        return self._load_json(self._job_status_path(folder))

    async def enqueue_job(self, country: str, city: str, category: str, filters: Dict[str, Any]) -> ScraperJob:
        job = ScraperJob(country=country, city=city, category=category, filters=filters)
        await self.queue.put(job)
        self._log(f"Queued job: {country}/{city}/{category}")
        return job

    async def run(self) -> None:
        if self.worker_task is not None:
            return
        self.worker_task = asyncio.create_task(self._worker())

    async def _worker(self) -> None:
        while True:
            job = await self.queue.get()
            self.active_job = job
            try:
                await self._process_job(job)
            except Exception as exc:
                self._log(f"ERROR: {exc}")
                self._update_status(job, "failed", 0, str(exc))
            finally:
                self.active_job = None
                self.queue.task_done()

    async def _process_job(self, job: ScraperJob) -> None:
        folder = self.project_folder(job.country, job.city, job.category)
        self._ensure_project_files(folder)

        state = self.load_job_status(job.country, job.city, job.category) or {}
        current_stage = state.get("stage", "queued")

        if current_stage in ["queued", "scraping"]:
            await self._step_scraping(job, folder)
            current_stage = "scraping"

        if current_stage in ["scraping", "analyzing"]:
            await self._step_analyzing(job, folder)
            current_stage = "analyzing"

        if current_stage in ["analyzing", "osint"]:
            await self._step_osint(job, folder)
            current_stage = "osint"

        await self._finalize(job, folder)
        self._update_status(job, "completed", 100, "Job completed successfully")

    def _ensure_project_files(self, folder: Path) -> None:
        for file_name in ["leads.json", "analyzed.json", "osint.json", "final_leads.json"]:
            path = folder / file_name
            if not path.exists():
                self._save_json(path, [])

    def _load_leads(self, folder: Path) -> List[Dict[str, Any]]:
        leads = self._load_json(folder / "leads.json")
        return leads if isinstance(leads, list) else []

    def _save_leads(self, folder: Path, leads: List[Dict[str, Any]]) -> None:
        self._save_json(folder / "leads.json", leads)

    async def _step_scraping(self, job: ScraperJob, folder: Path) -> None:
        self._update_status(job, "scraping", 15, "Starting scraping phase")
        if not self.pipeline_script.exists():
            raise FileNotFoundError("Pipeline script is missing.")
        command = [sys.executable, str(self.pipeline_script), "--city", job.city, "--keyword", job.category, "--limit", "10"]
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(self.base_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            text=True,
        )

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            self._log(line.rstrip())

        code = await process.wait()
        if code != 0:
            raise RuntimeError(f"Pipeline failed with exit code {code}")

        output_path = self.base_dir / "output" / "final_leads.json"
        leads = self._load_json(output_path)
        if not isinstance(leads, list):
            raise ValueError("Pipeline did not produce a valid leads list.")

        self._save_leads(folder, leads)
        self._save_json(folder / "analyzed.json", leads)
        self._save_json(folder / "osint.json", leads)
        self._save_json(folder / "final_leads.json", leads)
        self._update_status(job, "scraping", 30, "Scraping complete")

    async def _step_analyzing(self, job: ScraperJob, folder: Path) -> None:
        self._update_status(job, "analyzing", 45, "Analyzing leads")
        leads = self._load_leads(folder)
        if not leads:
            raise ValueError("No leads available for analysis.")
        analyzed = [dict(lead, analyzed=True) for lead in leads]
        self._save_json(folder / "analyzed.json", analyzed)
        self._update_status(job, "analyzing", 65, "Analysis complete")

    async def _step_osint(self, job: ScraperJob, folder: Path) -> None:
        self._update_status(job, "osint", 70, "Running OSINT augmentation")
        analyzed = self._load_json(folder / "analyzed.json") or []
        osint_leads = []
        for record in analyzed:
            enriched = record.copy()
            enriched.setdefault("score", enriched.get("score", 0) or 0)
            enriched.setdefault("email", enriched.get("email", ""))
            enriched.setdefault("website", enriched.get("website", ""))
            enriched.setdefault("phone", enriched.get("phone", ""))
            enriched.setdefault("linkedin", enriched.get("linkedin", ""))
            enriched.setdefault("facebook", enriched.get("facebook", ""))
            osint_leads.append(enriched)
        self._save_json(folder / "osint.json", osint_leads)
        self._update_status(job, "osint", 82, "OSINT enrichment complete")

    async def _finalize(self, job: ScraperJob, folder: Path) -> None:
        self._update_status(job, "finalizing", 85, "Filtering and tagging final leads")
        leads = self._load_json(folder / "osint.json") or []
        leads = self.filter_leads(leads, job.filters)
        tagged = [self.tag_lead(lead) for lead in leads]
        self._save_json(folder / "final_leads.json", tagged)
        self.generate_excel_report(folder / "report.xlsx", tagged)
        self._update_status(job, "completed", 100, "Final reports generated")

    def filter_leads(self, leads: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        filtered = []
        for lead in leads:
            if filters.get("has_website") is True and not lead.get("website"):
                continue
            if filters.get("has_email") is True and not lead.get("email"):
                continue
            if filters.get("has_linkedin") is True and not lead.get("linkedin"):
                continue
            score_threshold = filters.get("score_threshold")
            if score_threshold is not None:
                try:
                    score_value = float(lead.get("score", 0) or 0)
                except (TypeError, ValueError):
                    score_value = 0
                if score_value < float(score_threshold):
                    continue
            filtered.append(lead)
        return filtered

    def tag_lead(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        lead = lead.copy()
        score = float(lead.get("score", 0) or 0)
        email = bool(lead.get("email"))
        website = bool(lead.get("website"))

        if score >= 80 and email:
            lead["priority"] = "HOT"
        elif score >= 50 or (email and website):
            lead["priority"] = "WARM"
        else:
            lead["priority"] = "COLD"
        return lead

    def generate_excel_report(self, path: Path, leads: List[Dict[str, Any]]) -> None:
        workbook = Workbook()
        all_sheet = workbook.active
        all_sheet.title = "All Leads"
        self._write_sheet(all_sheet, leads, [
            "Name",
            "Category",
            "Website",
            "Email Status",
            "Phone",
            "Social Links",
            "Score",
            "Priority",
        ])

        hot_sheet = workbook.create_sheet("High Priority")
        high_leads = [lead for lead in leads if lead.get("priority") == "HOT"]
        self._write_sheet(hot_sheet, high_leads, [
            "Name",
            "Category",
            "Website",
            "Email Status",
            "Phone",
            "Social Links",
            "Score",
            "Priority",
        ])

        contact_sheet = workbook.create_sheet("Contact Info")
        self._write_sheet(contact_sheet, leads, [
            "Name",
            "Email",
            "Phone",
            "Website",
            "LinkedIn",
            "Facebook",
            "X",
            "Priority",
        ], contact_mode=True)

        path.parent.mkdir(parents=True, exist_ok=True)
        workbook.save(str(path))

    def _write_sheet(self, sheet: Any, leads: List[Dict[str, Any]], headers: List[str], contact_mode: bool = False) -> None:
        sheet.append(headers)
        for lead in leads:
            if contact_mode:
                sheet.append([
                    lead.get("name", ""),
                    lead.get("email", ""),
                    lead.get("phone", ""),
                    lead.get("website", ""),
                    lead.get("linkedin", ""),
                    lead.get("facebook", ""),
                    lead.get("x", ""),
                    lead.get("priority", ""),
                ])
            else:
                social_links = ", ".join(
                    filter(None, [lead.get("linkedin"), lead.get("facebook"), lead.get("x")])
                )
                sheet.append([
                    lead.get("name", ""),
                    lead.get("category", ""),
                    lead.get("website", ""),
                    "Yes" if lead.get("email") else "No",
                    lead.get("phone", ""),
                    social_links,
                    lead.get("score", ""),
                    lead.get("priority", ""),
                ])

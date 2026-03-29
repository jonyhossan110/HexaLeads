"""
Local Brain: mission planning, intent parsing (spaCy + regex fallback),
and task_tracker.json for short-term memory / resume.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

TASK_TRACKER_FILENAME = "task_tracker.json"

MISSION_STEPS: List[Dict[str, Any]] = [
    {
        "id": 1,
        "title": "Intent Analysis — validate category, city, and country.",
        "emoji": "🧠",
    },
    {
        "id": 2,
        "title": "Scrape Maps — Google Maps & sources (visible browser).",
        "emoji": "🗺️",
    },
    {
        "id": 3,
        "title": "Deep Web Search — analyzer + OSINT enrichment.",
        "emoji": "🌐",
    },
    {
        "id": 4,
        "title": "AI Filter — scoring pipeline & lead ranking.",
        "emoji": "🤖",
    },
    {
        "id": 5,
        "title": "Local Brain — spaCy quality gate + LeadAnalyzer.",
        "emoji": "🔬",
    },
    {
        "id": 6,
        "title": "Report Generation — Excel & PDF export.",
        "emoji": "📊",
    },
]

# Hints when the user omits country (spaCy / fallback).
CITY_COUNTRY_HINTS = {
    "london": "United Kingdom",
    "manchester": "United Kingdom",
    "birmingham": "United Kingdom",
    "dhaka": "Bangladesh",
    "chittagong": "Bangladesh",
    "new york": "United States",
    "los angeles": "United States",
    "paris": "France",
    "berlin": "Germany",
    "toronto": "Canada",
    "sydney": "Australia",
}

_QUOTED_PAIR_RE = re.compile(
    r'["\']([^"\']+)["\']\s+in\s+["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_IN_TAIL_RE = re.compile(
    r"^(.+?)\s+in\s+(.+)$",
    re.IGNORECASE | re.DOTALL,
)
_EXTRA_COUNTRY_RE = re.compile(r',\s*["\']?([^"\'\n,]+?)["\']?\s*$')


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_nlp():
    try:
        from brain.brain_engine import load_nlp

        return load_nlp()
    except Exception:
        return None


def _normalize_city_country(
    city: str,
    country_hint: Optional[str],
    nlp: Any,
) -> Tuple[str, str]:
    city_clean = city.strip()
    if "," in city_clean and country_hint is None:
        parts = [p.strip() for p in city_clean.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return parts[0], parts[1]
    if country_hint:
        return city_clean, country_hint.strip()

    lowered = city_clean.lower()
    if lowered in CITY_COUNTRY_HINTS:
        return city_clean, CITY_COUNTRY_HINTS[lowered]

    if nlp is not None:
        doc = nlp(city_clean)
        for ent in doc.ents:
            if ent.label_ == "GPE":
                # e.g. "London, UK" as one string
                parts = [p.strip() for p in re.split(r",|/", city_clean) if p.strip()]
                if len(parts) >= 2:
                    return parts[0], parts[-1]
                return city_clean, str(ent.text)

    return city_clean, CITY_COUNTRY_HINTS.get(lowered, "Unknown")


def _parse_with_regex(command_body: str) -> Optional[Tuple[str, str, Optional[str]]]:
    text = command_body.strip()
    m = _QUOTED_PAIR_RE.search(text)
    if m:
        category, location = m.group(1).strip(), m.group(2).strip()
        rest = text[m.end() :].strip()
        country: Optional[str] = None
        if rest.startswith(","):
            cm = _EXTRA_COUNTRY_RE.match(rest)
            if cm:
                country = cm.group(1).strip()
        return category, location, country

    m2 = _IN_TAIL_RE.match(text)
    if m2:
        category = m2.group(1).strip().strip("\"'")
        tail = m2.group(2).strip()
        if "," in tail:
            loc_parts = [p.strip() for p in tail.split(",", 1)]
            return (
                category,
                loc_parts[0],
                loc_parts[1] if len(loc_parts) > 1 else None,
            )
        return category, tail, None

    return None


def parse_hunt_intent(command_text: str) -> Dict[str, Any]:
    """
    Parse Telegram body like: /hunt "Restaurants" in "London"
    or /hunt "Restaurants" in "London", "United Kingdom"
    Returns a dict with keys: category, city, country, confidence, parse_method
    """
    raw = command_text.strip()
    if raw.startswith("/"):
        parts = raw.split(maxsplit=1)
        body = parts[1] if len(parts) > 1 else ""
    else:
        body = raw

    nlp = _get_nlp()
    category = ""
    city_guess = ""
    country_guess: Optional[str] = None
    confidence = 0.5
    parse_method = "regex"

    reg = _parse_with_regex(body)
    if reg:
        category, city_guess, country_guess = reg
        confidence = 0.85
    elif nlp is not None:
        doc = nlp(body)
        parse_method = "spacy"
        for token in doc:
            if token.lower_ == "in" and token.i > 0:
                category = doc[: token.i].text.strip().strip("\"'")
                city_guess = doc[token.i + 1 :].text.strip().strip("\"'")
                if "," in city_guess:
                    bits = [b.strip() for b in city_guess.split(",", 1)]
                    city_guess = bits[0]
                    country_guess = bits[1] if len(bits) > 1 else None
                confidence = 0.75
                break
        if not category or not city_guess:
            reg = _parse_with_regex(body)
            if reg:
                category, city_guess, country_guess = reg
                parse_method = "regex+spacy"
                confidence = 0.8

    if not category or not city_guess:
        raise ValueError(
            'Could not parse hunt command. Try: /hunt "Restaurants" in "London" '
            'or /hunt "Restaurants" in "London", "United Kingdom"'
        )

    city, country = _normalize_city_country(city_guess, country_guess, nlp)
    if country == "Unknown":
        raise ValueError(
            "Could not infer country from the location. "
            'Add an explicit country, e.g. /hunt "Restaurants" in "London", "United Kingdom".'
        )

    return {
        "category": category,
        "city": city,
        "country": country,
        "confidence": confidence,
        "parse_method": parse_method,
    }


@dataclass
class MissionPlan:
    intent: str
    category: str
    city: str
    country: str
    command_raw: str
    steps: List[Dict[str, Any]] = field(default_factory=lambda: [dict(s) for s in MISSION_STEPS])
    created_at: str = field(default_factory=_utc_now_iso)
    parse_meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "intent": self.intent,
            "category": self.category,
            "city": self.city,
            "country": self.country,
            "command_raw": self.command_raw,
            "steps": self.steps,
            "created_at": self.created_at,
            "parse_meta": self.parse_meta,
        }


def build_mission_plan(command_raw: str, intent_payload: Dict[str, Any]) -> MissionPlan:
    return MissionPlan(
        intent="hunt_leads",
        category=intent_payload["category"],
        city=intent_payload["city"],
        country=intent_payload["country"],
        command_raw=command_raw,
        parse_meta={
            "confidence": intent_payload.get("confidence"),
            "parse_method": intent_payload.get("parse_method"),
        },
    )


def format_mission_started_text(plan: MissionPlan) -> str:
    """Single user-facing start message: title + 6-step plan (silent mode)."""
    body = format_mission_plan_text(plan)
    return f"🚀 Mission Started\n\n{body}"


def format_mission_plan_text(plan: MissionPlan) -> str:
    lines = [
        "📋 Mission Plan (6 steps)",
        f"Target: {plan.category} in {plan.city}, {plan.country}",
        "",
        "Pipeline: Intent Analysis → Scrape Maps → Deep Web Search → AI Filter → Local Brain → Reports",
        "",
        "Steps:",
    ]
    for step in plan.steps:
        em = step.get("emoji") or "•"
        lines.append(f"  {em} Step {step['id']}: {step['title']}")
    return "\n".join(lines)


def step_meta(step_id: int) -> Dict[str, Any]:
    if step_id < 1 or step_id > len(MISSION_STEPS):
        return {"id": step_id, "title": "Working", "emoji": "🔄"}
    return dict(MISSION_STEPS[step_id - 1])


def format_step_in_progress(step_id: int, detail: str = "") -> str:
    m = step_meta(step_id)
    short = m["title"].split("—")[0].strip()
    base = f"🔄 {m['emoji']} Step {step_id}: {short} in progress..."
    return f"{base}\n{detail}".strip() if detail else base


def format_step_completed(step_id: int, detail: str = "") -> str:
    m = step_meta(step_id)
    base = f"✅ {m['emoji']} Step {step_id} complete — {m['title'].split('—')[0].strip()}"
    return f"{base}\n{detail}".strip() if detail else base


def task_tracker_path(project_folder: Path) -> Path:
    return project_folder / TASK_TRACKER_FILENAME


def load_task_tracker(project_folder: Path) -> Optional[Dict[str, Any]]:
    path = task_tracker_path(project_folder)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_task_tracker(project_folder: Path, payload: Dict[str, Any]) -> None:
    project_folder.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = _utc_now_iso()
    path = task_tracker_path(project_folder)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def init_task_tracker(project_folder: Path, mission: Dict[str, Any]) -> Dict[str, Any]:
    state = {
        "mission_plan": mission,
        "last_completed_step": 0,
        "step_notes": {},
    }
    save_task_tracker(project_folder, state)
    return state


def mark_step_completed(
    project_folder: Path,
    step_id: int,
    detail_message: str = "",
) -> Dict[str, Any]:
    state = load_task_tracker(project_folder) or {}
    last = int(state.get("last_completed_step", 0))
    if step_id > last:
        state["last_completed_step"] = step_id
    notes = dict(state.get("step_notes") or {})
    notes[str(step_id)] = detail_message
    state["step_notes"] = notes
    save_task_tracker(project_folder, state)
    return state


def resume_from_step(project_folder: Path) -> int:
    """Return next step id to run (1..6), or 7 when all steps are done."""
    state = load_task_tracker(project_folder)
    if not state:
        return 1
    last = int(state.get("last_completed_step", 0))
    if last >= 6:
        return 7
    return last + 1

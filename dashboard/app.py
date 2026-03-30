from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, redirect, render_template, request, url_for

from database.db import (
    DB_PATH,
    get_all_leads,
    get_outreach_by_lead,
    get_security_issues,
    update_lead_status,
)

try:
    from outreach_bot.sender import send_email
except ImportError:  # pragma: no cover
    send_email = None  # type: ignore

try:
    from planner import build_mission_plan, parse_hunt_intent
except ImportError:  # pragma: no cover
    build_mission_plan = None  # type: ignore
    parse_hunt_intent = None  # type: ignore

try:
    from security_scanner.scanner import scan_website
except ImportError:  # pragma: no cover
    scan_website = None  # type: ignore

app = Flask(__name__, template_folder="templates")


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _normalize_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def _get_upwork_jobs() -> List[Dict[str, Any]]:
    if not DB_PATH.exists():
        return []
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM upwork_jobs ORDER BY found_at DESC")
        return [_row_to_dict(row) for row in cursor.fetchall()]


def _get_lead_by_id(lead_id: int) -> Optional[Dict[str, Any]]:
    for row in get_all_leads():
        if int(row["id"]) == lead_id:
            return _row_to_dict(row)
    return None


def _build_stats() -> Dict[str, int]:
    leads = [ _row_to_dict(row) for row in get_all_leads() ]
    total = len(leads)
    high = sum(1 for lead in leads if _normalize_text(lead.get("score_label")).upper() == "HIGH")
    emails_sent = 0
    replies = 0
    for lead in leads:
        outreach_rows = get_outreach_by_lead(int(lead["id"]))
        for outreach in outreach_rows:
            status = _normalize_text(outreach["status"]).lower()
            if status == "sent":
                emails_sent += 1
            if status == "replied":
                replies += 1
    return {
        "total_leads": total,
        "high_priority": high,
        "emails_sent": emails_sent,
        "replies_received": replies,
    }


def _filter_leads(leads: List[Dict[str, Any]], filters: Dict[str, str]) -> List[Dict[str, Any]]:
    filtered = []
    for lead in leads:
        if filters["status"] and _normalize_text(lead.get("status")).lower() != filters["status"].lower():
            continue
        if filters["score_label"] and _normalize_text(lead.get("score_label")).lower() != filters["score_label"].lower():
            continue
        if filters["country"] and _normalize_text(lead.get("country")).lower() != filters["country"].lower():
            continue
        if filters["source"] and _normalize_text(lead.get("source")).lower() != filters["source"].lower():
            continue
        filtered.append(lead)
    return filtered


def _sort_leads(leads: List[Dict[str, Any]], sort_key: str) -> List[Dict[str, Any]]:
    if sort_key == "score":
        return sorted(leads, key=lambda lead: int(lead.get("score") or 0), reverse=True)
    if sort_key == "created_at":
        return sorted(
            leads,
            key=lambda lead: _normalize_text(lead.get("created_at")),
            reverse=True,
        )
    return leads


def _build_filter_options(leads: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    statuses = sorted({ _normalize_text(lead.get("status")) for lead in leads if lead.get("status") })
    score_labels = sorted({ _normalize_text(lead.get("score_label")) for lead in leads if lead.get("score_label") })
    countries = sorted({ _normalize_text(lead.get("country")) for lead in leads if lead.get("country") })
    sources = sorted({ _normalize_text(lead.get("source")) for lead in leads if lead.get("source") })
    return {
        "statuses": statuses,
        "score_labels": score_labels,
        "countries": countries,
        "sources": sources,
    }


def _latest_outreach_summary(lead_id: int) -> Optional[Dict[str, Any]]:
    outreach_rows = get_outreach_by_lead(lead_id)
    if not outreach_rows:
        return None
    latest = outreach_rows[0]
    return _row_to_dict(latest)


def _lead_issue_summary(lead_id: int) -> Dict[str, Any]:
    issues = [ _row_to_dict(issue) for issue in get_security_issues(lead_id) ]
    return {
        "issues": issues,
        "severity": ", ".join({ issue.get("severity", "") for issue in issues if issue.get("severity") }),
        "count": len(issues),
    }


@app.route("/")
def home() -> str:
    stats = _build_stats()
    recent_leads = _sort_leads([_row_to_dict(row) for row in get_all_leads()], "created_at")[:20]
    return render_template(
        "index.html",
        page="home",
        stats=stats,
        recent_leads=recent_leads,
    )


@app.route("/leads")
def leads() -> str:
    leads = [_row_to_dict(row) for row in get_all_leads()]
    filters = {
        "status": request.args.get("status", ""),
        "score_label": request.args.get("score_label", ""),
        "country": request.args.get("country", ""),
        "source": request.args.get("source", ""),
    }
    sort_key = request.args.get("sort", "")
    filtered = _filter_leads(leads, filters)
    sorted_leads = _sort_leads(filtered, sort_key)
    options = _build_filter_options(leads)
    return render_template(
        "index.html",
        page="leads",
        leads=sorted_leads,
        filters=filters,
        options=options,
        sort_key=sort_key,
    )


@app.route("/leads/<int:lead_id>")
def lead_detail(lead_id: int) -> str:
    lead = _get_lead_by_id(lead_id)
    if not lead:
        return render_template("index.html", page="not_found", message="Lead not found."), 404
    security = _lead_issue_summary(lead_id)
    outreach_history = [ _row_to_dict(row) for row in get_outreach_by_lead(lead_id) ]
    pitch_preview = outreach_history[0].get("email_body") if outreach_history else "No pitch available yet."
    return render_template(
        "index.html",
        page="detail",
        lead=lead,
        security=security,
        outreach_history=outreach_history,
        pitch_preview=pitch_preview,
    )


@app.route("/scan")
def scan_page() -> str:
    return render_template("index.html", page="scan")


@app.route("/upwork")
def upwork() -> str:
    jobs = _get_upwork_jobs()
    return render_template("index.html", page="upwork", jobs=jobs)


@app.route("/api/stats")
def api_stats() -> Any:
    return jsonify(_build_stats())


@app.route("/api/approve/<int:lead_id>", methods=["POST"])
def api_approve(lead_id: int) -> Any:
    if send_email is None:
        return jsonify({"error": "Email sender not available."}), 500
    success = send_email(lead_id)
    if not success:
        return jsonify({"error": "Failed to send email."}), 500
    return jsonify({"status": "sent"})


@app.route("/api/reject/<int:lead_id>", methods=["POST"])
def api_reject(lead_id: int) -> Any:
    update_lead_status(lead_id=lead_id, status="rejected")
    return jsonify({"status": "rejected"})


@app.route("/api/scan", methods=["POST"])
def api_scan() -> Any:
    payload = request.get_json() or request.form
    target = _normalize_text(payload.get("url") or payload.get("target") or payload.get("website"))
    if not target:
        return jsonify({"error": "URL or domain is required."}), 400
    if scan_website is None:
        return jsonify({"error": "Scanner backend unavailable."}), 500
    result = scan_website(target)
    return jsonify(result)


@app.route("/api/run-hunt", methods=["POST"])
def api_run_hunt() -> Any:
    payload = request.get_json() or {}
    command = _normalize_text(payload.get("command") or "")
    if not command:
        return jsonify({"status": "started", "message": "Hunt pipeline trigger not configured. Provide a command to parse."})
    if parse_hunt_intent is None or build_mission_plan is None:
        return jsonify({"status": "error", "message": "Hunt backend unavailable."}), 500
    try:
        intent = parse_hunt_intent(command)
        plan = build_mission_plan(command, intent)
        return jsonify({"status": "started", "plan": plan.to_dict()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5555, debug=True)

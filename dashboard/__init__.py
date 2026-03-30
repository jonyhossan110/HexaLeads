from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from database.db import get_all_leads, get_outreach_by_lead, get_security_issues
from utils.logger import get_logger

logger = get_logger("dashboard")

TEMPLATE_LEADS = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HexaLeads Dashboard</title>
  <style>body{font-family:Arial,Helvetica,sans-serif;margin:20px;}table{border-collapse:collapse;width:100%;}th,td{border:1px solid #ddd;padding:8px;}th{background:#333;color:#fff;}</style>
</head>
<body>
  <h1>HexaLeads Dashboard</h1>
  <p><a href="/outreach">Outreach</a> | <a href="/health">Health</a></p>
  <table>
    <tr><th>ID</th><th>Business</th><th>Website</th><th>Status</th><th>Score</th><th>Source</th></tr>
    {% for lead in leads %}
    <tr>
      <td>{{ lead.id }}</td>
      <td>{{ lead.business_name or '-' }}</td>
      <td><a href="{{ lead.website }}" target="_blank">{{ lead.website }}</a></td>
      <td>{{ lead.status }}</td>
      <td>{{ lead.score }}</td>
      <td>{{ lead.source }}</td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
"""

TEMPLATE_HEALTH = """
<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>HexaLeads Health</title></head>
<body>
  <h1>Health Check</h1>
  <p>Dashboard is running.</p>
  <p>Database path: {{ db_path }}</p>
</body>
</html>
"""


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

    @app.route("/")
    def leads() -> Any:
        leads = get_all_leads()
        return render_template_string(TEMPLATE_LEADS, leads=leads)

    @app.route("/lead/<int:lead_id>")
    def lead_details(lead_id: int) -> Any:
        leads = [lead for lead in get_all_leads() if int(lead["id"]) == lead_id]
        if not leads:
            return jsonify({"error": "Lead not found."}), 404
        lead = leads[0]
        issues = get_security_issues(lead_id)
        outreach = get_outreach_by_lead(lead_id)
        return jsonify(
            lead={k: lead[k] for k in lead.keys()},
            security_issues=[dict(issue) for issue in issues],
            outreach=[dict(item) for item in outreach],
        )

    @app.route("/outreach")
    def outreach() -> Any:
        leads = get_all_leads()
        rows = []
        for lead in leads:
            outreach = get_outreach_by_lead(int(lead["id"]))
            rows.append({
                "lead_id": lead["id"],
                "website": lead["website"],
                "outreach_count": len(outreach),
            })
        return jsonify(rows)

    @app.route("/health")
    def health() -> Any:
        return render_template_string(
            TEMPLATE_HEALTH,
            db_path=str(Path(__file__).resolve().parents[1] / "leads.db"),
        )

    return app


def run(host: str = "127.0.0.1", port: int = 5000, debug: bool = False) -> None:
    app = create_app()
    logger.info("Starting HexaLeads dashboard on %s:%s", host, port)
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    run()

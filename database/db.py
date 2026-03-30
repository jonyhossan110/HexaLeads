"""
database/db.py — HexaLeads SQLite database module
Owner: Md. Jony Hassain | HexaCyberLab

Provides:
  - init_db()               : Create all tables if they don't exist
  - insert_lead()           : Insert a new lead record
  - update_lead_status()    : Update the status (and optional score) of a lead
  - get_leads_by_status()   : Fetch all leads with a given status
  - get_all_leads()         : Fetch every lead in the database
  - insert_security_issue() : Log a security finding against a lead
  - insert_outreach()       : Create an outreach (email) record for a lead

Auto-initialises the database on import.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable, Optional

# ──────────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────────

ROOT_DIR: Path = Path(__file__).resolve().parent.parent
DB_PATH: Path = ROOT_DIR / "leads.db"


# ──────────────────────────────────────────────────────────────────────────────
# Connection helper
# ──────────────────────────────────────────────────────────────────────────────

@contextmanager
def _get_conn() -> Generator[sqlite3.Connection, None, None]:
    """Yield a SQLite connection with row_factory set to Row."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Enable foreign-key enforcement
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
-- ── leads ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS leads (
    id             INTEGER  PRIMARY KEY AUTOINCREMENT,
    business_name  TEXT,
    website        TEXT     UNIQUE,
    email          TEXT,
    phone          TEXT,
    address        TEXT,
    country        TEXT,
    industry       TEXT,
    source         TEXT,      -- google_maps / linkedin / web_search / upwork
    status         TEXT     NOT NULL DEFAULT 'new',
                             -- new / contacted / replied / converted / rejected
    score          INTEGER  NOT NULL DEFAULT 0,
    score_label    TEXT,      -- HIGH / MEDIUM / LOW
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME
);

-- ── security_issues ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS security_issues (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER  REFERENCES leads(id) ON DELETE CASCADE,
    issue_type  TEXT,    -- ssl_expired / outdated_cms / missing_headers /
                         -- open_admin / exposed_files / weak_password_page
    severity    TEXT,    -- critical / high / medium / low
    details     TEXT,
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── outreach ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outreach (
    id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    lead_id       INTEGER  REFERENCES leads(id) ON DELETE CASCADE,
    email_subject TEXT,
    email_body    TEXT,
    status        TEXT,    -- draft / approved / sent / bounced / replied
    sent_at       DATETIME,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── upwork_jobs ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS upwork_jobs (
    id             INTEGER  PRIMARY KEY AUTOINCREMENT,
    job_title      TEXT,
    job_url        TEXT     UNIQUE,
    budget         TEXT,
    client_country TEXT,
    description    TEXT,
    proposal_draft TEXT,
    status         TEXT     NOT NULL DEFAULT 'new',
                             -- new / proposal_sent / hired / rejected
    found_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create all tables if they don't already exist."""
    with _get_conn() as conn:
        conn.executescript(_SCHEMA_SQL)
    print(f"[DB] Initialised → {DB_PATH}")


# ── Leads ─────────────────────────────────────────────────────────────────────

def insert_lead(
    business_name: Optional[str] = None,
    website:       Optional[str] = None,
    email:         Optional[str] = None,
    phone:         Optional[str] = None,
    address:       Optional[str] = None,
    country:       Optional[str] = None,
    industry:      Optional[str] = None,
    source:        Optional[str] = None,
    status:        str = "new",
    score:         int = 0,
    score_label:   Optional[str] = None,
) -> Optional[int]:
    """
    Insert a new lead.  If the website already exists the row is ignored
    (INSERT OR IGNORE) and None is returned; otherwise the new row id is
    returned.
    """
    sql = """
        INSERT OR IGNORE INTO leads
            (business_name, website, email, phone, address, country,
             industry, source, status, score, score_label)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _get_conn() as conn:
        cur = conn.execute(sql, (
            business_name, website, email, phone, address, country,
            industry, source, status, score, score_label,
        ))
        return cur.lastrowid if cur.lastrowid else None


def update_lead_status(
    lead_id:     int,
    status:      str,
    score:       Optional[int] = None,
    score_label: Optional[str] = None,
) -> None:
    """Update the status of a lead and optionally refresh its score."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if score is not None and score_label is not None:
        sql = """
            UPDATE leads
            SET status = ?, score = ?, score_label = ?, updated_at = ?
            WHERE id = ?
        """
        params: tuple[Any, ...] = (status, score, score_label, now, lead_id)
    elif score is not None:
        sql = """
            UPDATE leads
            SET status = ?, score = ?, updated_at = ?
            WHERE id = ?
        """
        params = (status, score, now, lead_id)
    else:
        sql = """
            UPDATE leads
            SET status = ?, updated_at = ?
            WHERE id = ?
        """
        params = (status, now, lead_id)

    with _get_conn() as conn:
        conn.execute(sql, params)


def update_lead(
    lead_id: Optional[int] = None,
    website: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None,
    country: Optional[str] = None,
    industry: Optional[str] = None,
    source: Optional[str] = None,
    status: Optional[str] = None,
    score: Optional[int] = None,
    score_label: Optional[str] = None,
) -> None:
    """Update lead fields by lead_id or website."""
    if lead_id is None and website is None:
        raise ValueError("lead_id or website is required to update a lead.")

    fields: list[str] = []
    params: list[Any] = []

    if email is not None:
        fields.append("email = ?")
        params.append(email)
    if phone is not None:
        fields.append("phone = ?")
        params.append(phone)
    if address is not None:
        fields.append("address = ?")
        params.append(address)
    if country is not None:
        fields.append("country = ?")
        params.append(country)
    if industry is not None:
        fields.append("industry = ?")
        params.append(industry)
    if source is not None:
        fields.append("source = ?")
        params.append(source)
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if score is not None:
        fields.append("score = ?")
        params.append(score)
    if score_label is not None:
        fields.append("score_label = ?")
        params.append(score_label)

    if not fields:
        return

    fields.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    if lead_id is not None:
        where_clause = "id = ?"
        params.append(lead_id)
    else:
        where_clause = "website = ?"
        params.append(website)

    sql = f"UPDATE leads SET {', '.join(fields)} WHERE {where_clause}"
    with _get_conn() as conn:
        conn.execute(sql, tuple(params))


def get_leads_by_status(status: str) -> list[sqlite3.Row]:
    """Return all leads that match the given status."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM leads WHERE status = ? ORDER BY score DESC, created_at DESC",
            (status,),
        )
        return cur.fetchall()


def get_all_leads() -> list[sqlite3.Row]:
    """Return every lead in the database, highest score first."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM leads ORDER BY score DESC, created_at DESC"
        )
        return cur.fetchall()


def get_lead_by_website(website: str) -> Optional[sqlite3.Row]:
    """Return the first lead row matching the website, if any."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM leads WHERE website = ? LIMIT 1",
            (website,),
        )
        return cur.fetchone()


# ── Security Issues ───────────────────────────────────────────────────────────

def insert_security_issue(
    lead_id:    int,
    issue_type: str,
    severity:   str,
    details:    Optional[str] = None,
) -> int:
    """
    Log a security finding against a lead.
    Returns the new row id.
    """
    sql = """
        INSERT INTO security_issues (lead_id, issue_type, severity, details)
        VALUES (?, ?, ?, ?)
    """
    with _get_conn() as conn:
        cur = conn.execute(sql, (lead_id, issue_type, severity, details))
        return cur.lastrowid  # type: ignore[return-value]


def get_security_issues(lead_id: int) -> list[sqlite3.Row]:
    """Return all security issues linked to a lead."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM security_issues WHERE lead_id = ? ORDER BY detected_at DESC",
            (lead_id,),
        )
        return cur.fetchall()


# ── Outreach ──────────────────────────────────────────────────────────────────

def insert_outreach(
    lead_id:       int,
    email_subject: Optional[str] = None,
    email_body:    Optional[str] = None,
    status:        str = "draft",
    sent_at:       Optional[str] = None,
) -> int:
    """
    Create an outreach record for a lead.
    Returns the new row id.
    """
    sql = """
        INSERT INTO outreach (lead_id, email_subject, email_body, status, sent_at)
        VALUES (?, ?, ?, ?, ?)
    """
    with _get_conn() as conn:
        cur = conn.execute(sql, (lead_id, email_subject, email_body, status, sent_at))
        return cur.lastrowid  # type: ignore[return-value]


def get_outreach_by_lead(lead_id: int) -> list[sqlite3.Row]:
    """Return all outreach records for a given lead."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM outreach WHERE lead_id = ? ORDER BY created_at DESC",
            (lead_id,),
        )
        return cur.fetchall()


# ── Upwork Jobs ───────────────────────────────────────────────────────────────

def insert_upwork_job(
    job_title:      Optional[str] = None,
    job_url:        Optional[str] = None,
    budget:         Optional[str] = None,
    client_country: Optional[str] = None,
    description:    Optional[str] = None,
    proposal_draft: Optional[str] = None,
    status:         str = "new",
) -> Optional[int]:
    """
    Insert a new Upwork job.  Duplicate URLs are ignored and None is returned.
    """
    sql = """
        INSERT OR IGNORE INTO upwork_jobs
            (job_title, job_url, budget, client_country, description, proposal_draft, status)
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
    """
    with _get_conn() as conn:
        cur = conn.execute(sql, (
            job_title, job_url, budget, client_country,
            description, proposal_draft, status,
        ))
        return cur.lastrowid if cur.lastrowid else None


def get_upwork_jobs_by_status(status: str) -> list[sqlite3.Row]:
    """Return all Upwork jobs with the given status."""
    with _get_conn() as conn:
        cur = conn.execute(
            "SELECT * FROM upwork_jobs WHERE status = ? ORDER BY found_at DESC",
            (status,),
        )
        return cur.fetchall()


# ──────────────────────────────────────────────────────────────────────────────
# Auto-init on import
# ──────────────────────────────────────────────────────────────────────────────

init_db()

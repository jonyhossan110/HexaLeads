from __future__ import annotations

import os
import sys
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from database.db import get_all_leads, get_security_issues, update_lead_status


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_lower(value: Any) -> str:
    return _normalize_text(value).lower()


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value_str = str(value).strip()
    if not value_str:
        return None
    digits = re.findall(r"-?\d+", value_str)
    if not digits:
        return None
    try:
        return int(digits[0])
    except ValueError:
        return None


def _has_real_email(email: Any) -> bool:
    email_text = _normalize_lower(email)
    if "@" not in email_text:
        return False
    local = email_text.split("@", 1)[0]
    if local in {"info", "noreply", "no-reply"}:
        return False
    return True


def _has_ecommerce(lead: Dict[str, Any]) -> bool:
    keywords = ["ecommerce", "shopify", "woocommerce", "magento", "bigcommerce", "checkout", "cart", "store", "product"]
    fields = [lead.get("industry"), lead.get("website"), lead.get("description"), lead.get("notes"), lead.get("category")]
    for field in fields:
        if not field:
            continue
        normalized = _normalize_lower(field)
        if any(keyword in normalized for keyword in keywords):
            return True
    return False


def _has_international_business(lead: Dict[str, Any]) -> bool:
    country = _normalize_lower(lead.get("country"))
    if country and country not in {"bangladesh", "bd"}:
        return True
    return False


def _has_decision_maker(lead: Dict[str, Any]) -> bool:
    if _normalize_text(lead.get("decision_maker_name")):
        return True
    if _normalize_text(lead.get("contact_name")) and _normalize_text(lead.get("contact_name")) != _normalize_text(lead.get("business_name")):
        return True
    linkedin = _normalize_text(lead.get("linkedin")) or _normalize_text(lead.get("linkedin_url"))
    if linkedin:
        return True
    return False


def _is_very_large_enterprise(lead: Dict[str, Any]) -> bool:
    size_fields = [lead.get("employee_count"), lead.get("company_size"), lead.get("size"), lead.get("organization_size")]
    for value in size_fields:
        size = _to_int(value)
        if size is not None and size >= 500:
            return True
    business_name = _normalize_lower(lead.get("business_name"))
    if any(token in business_name for token in ["inc", "ltd", "corp", "corporation", "enterprise", "group"]):
        return False
    return False


def _get_review_count(lead: Dict[str, Any]) -> int:
    for key in ["google_maps_reviews", "reviews_count", "review_count", "reviews"]:
        value = lead.get(key)
        count = _to_int(value)
        if count is not None:
            return count
    return 0


def _get_pagespeed_score(lead: Dict[str, Any]) -> Optional[int]:
    for key in ["pagespeed_score", "page_speed_score", "pagespeed"]:
        score = _to_int(lead.get(key))
        if score is not None:
            return score
    return None


def _get_domain_age_days(lead: Dict[str, Any]) -> Optional[int]:
    for key in ["domain_age_days", "domain_age", "age_days"]:
        value = lead.get(key)
        age = _to_int(value)
        if age is not None:
            return age
    for key in ["domain_registered_on", "domain_registration_date", "registration_date"]:
        value = _normalize_text(lead.get(key))
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            try:
                parsed = datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                continue
        age = (datetime.now(timezone.utc) - parsed.replace(tzinfo=timezone.utc)).days
        if age >= 0:
            return age
    return None


def _extract_security_factors(security_issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    factors = {
        "ssl_risk": False,
        "outdated_cms": False,
        "missing_headers_count": 0,
        "exposed_admin": False,
        "server_version_exposed": False,
        "http_not_redirect": False,
        "robots_sensitive": False,
        "critical_issues": 0,
    }
    for issue in security_issues:
        issue_type = _normalize_lower(issue.get("issue_type") or issue.get("type"))
        severity = _normalize_lower(issue.get("severity", ""))
        if issue_type in {"ssl_expired", "ssl_expiring", "ssl_invalid", "ssl_missing_certificate", "ssl_missing_expiry", "ssl_unparsable_expiry"}:
            factors["ssl_risk"] = True
        if issue_type in {"outdated_wordpress", "detected_cms"} and "wordpress" in _normalize_lower(issue.get("detail")):
            factors["outdated_cms"] = True
        if issue_type.startswith("missing_"):
            factors["missing_headers_count"] += 1
        if issue_type in {"exposed_admin_panel", "insecure_admin_access", "exposed_admin_or_xmlrpc"}:
            factors["exposed_admin"] = True
        if issue_type == "server_version_exposed":
            factors["server_version_exposed"] = True
        if issue_type == "http_not_redirect_to_https":
            factors["http_not_redirect"] = True
        if issue_type == "sensitive_robots_disclosure":
            factors["robots_sensitive"] = True
        if severity == "critical":
            factors["critical_issues"] += 1
    return factors


def _security_score(security_issues: List[Dict[str, Any]]) -> tuple[int, List[str]]:
    factors = _extract_security_factors(security_issues)
    score = 0
    notes: List[str] = []
    if factors["ssl_risk"]:
        score += 20
        notes.append("SSL expired or expiring soon.")
    if factors["outdated_cms"]:
        score += 15
        notes.append("Outdated CMS detected.")
    if factors["missing_headers_count"] >= 3:
        score += 10
        notes.append("Three or more security headers are missing.")
    if factors["exposed_admin"]:
        score += 10
        notes.append("Exposed admin panel detected.")
    if factors["server_version_exposed"]:
        score += 5
        notes.append("Server version is exposed in headers.")
    if factors["http_not_redirect"]:
        score += 5
        notes.append("HTTP does not redirect to HTTPS.")
    if factors["robots_sensitive"]:
        score += 5
        notes.append("robots.txt exposes sensitive paths.")
    return min(score, 50), notes


def _business_quality_score(lead: Dict[str, Any]) -> tuple[int, List[str]]:
    score = 0
    notes: List[str] = []
    if _has_real_email(lead.get("email")):
        score += 10
        notes.append("Lead has a real business email.")
    if _get_review_count(lead) >= 10:
        score += 10
        notes.append("Business has 10+ Google Maps reviews.")
    if _has_ecommerce(lead):
        score += 5
        notes.append("Website appears to support e-commerce.")
    if _has_international_business(lead):
        score += 5
        notes.append("Business appears to be international.")
    if _has_decision_maker(lead):
        score += 5
        notes.append("Decision-maker contact signal is available.")
    if _is_very_large_enterprise(lead):
        score -= 10
        notes.append("Lead appears to be a very large enterprise.")
    return max(score, 0), notes


def _opportunity_score(lead: Dict[str, Any], security_issues: List[Dict[str, Any]]) -> tuple[int, List[str]]:
    score = 0
    notes: List[str] = []
    factors = _extract_security_factors(security_issues)
    if factors["critical_issues"] >= 2:
        score += 10
        notes.append("Multiple critical security issues detected.")
    pagespeed = _get_pagespeed_score(lead)
    if pagespeed is not None and pagespeed < 50:
        score += 5
        notes.append("Site is slow based on PageSpeed metrics.")
    domain_age = _get_domain_age_days(lead)
    if domain_age is not None and domain_age <= 180:
        score += 5
        notes.append("Domain was recently registered.")
    return min(score, 20), notes


def _label_from_score(score: int) -> str:
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    return "LOW"


def _build_recommendation(score: int, breakdown: Dict[str, Any]) -> str:
    if score >= 70:
        return (
            "Contact immediately: this lead has urgent security exposure and strong business signals. "
            "Use a data-driven pitch highlighting vulnerability remediation and international growth."
        )
    if score >= 40:
        return (
            "Contact this week: this lead is a promising opportunity with enough risk or business quality to prioritize. "
            "Use a consultative pitch focused on website improvement and security."
        )
    return (
        "Low priority: this lead is not urgent today. "
        "Keep on file and revisit if new signals appear."
    )


def calculate_score(lead: Dict[str, Any], security_issues: List[Dict[str, Any]]) -> Dict[str, Any]:
    security, security_notes = _security_score(security_issues)
    business_quality, business_notes = _business_quality_score(lead)
    opportunity, opportunity_notes = _opportunity_score(lead, security_issues)

    total = min(100, security + business_quality + opportunity)
    label = _label_from_score(total)
    breakdown = {
        "security_score": security,
        "business_quality_score": business_quality,
        "opportunity_score": opportunity,
        "total": total,
        "security_notes": security_notes,
        "business_quality_notes": business_notes,
        "opportunity_notes": opportunity_notes,
    }
    recommendation = _build_recommendation(total, breakdown)
    return {
        "score": total,
        "label": label,
        "breakdown": breakdown,
        "recommendation": recommendation,
    }


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    return {key: row[key] for key in row.keys()}


def batch_score(lead_ids: List[int]) -> List[Dict[str, Any]]:
    all_leads = get_all_leads()
    lead_map = {int(row["id"]): _row_to_dict(row) for row in all_leads if row["id"] is not None}
    results: List[Dict[str, Any]] = []

    for lead_id in lead_ids:
        lead_data = lead_map.get(int(lead_id))
        if not lead_data:
            continue
        security_rows = get_security_issues(int(lead_id))
        security_issues = [_row_to_dict(issue) for issue in security_rows]
        score_result = calculate_score(lead_data, security_issues)
        update_lead_status(
            lead_id=int(lead_id),
            status=lead_data.get("status") or "new",
            score=score_result["score"],
            score_label=score_result["label"],
        )
        results.append({"lead_id": int(lead_id), **score_result})
    return results


def rescore_all() -> List[Dict[str, Any]]:
    all_leads = get_all_leads()
    lead_ids = [int(row["id"]) for row in all_leads if row["id"] is not None]
    return batch_score(lead_ids)


if __name__ == "__main__":
    report = rescore_all()
    print({"rescore_count": len(report), "results": report})

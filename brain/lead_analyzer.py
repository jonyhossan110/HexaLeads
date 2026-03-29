import re
from datetime import datetime
from typing import Any, Dict, List, Optional

BIG_COMPANY_TERMS = ["ltd", "inc", "corp", "corporation", "llc"]


class LeadAnalyzer:
    def __init__(self, target_category: str):
        self.target_category = target_category.lower().strip()

    def analyze(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        name = str(lead.get("name", "")).strip()
        category = str(lead.get("category", "")).strip().lower()
        website = str(lead.get("website", "")).strip()
        facebook = str(lead.get("facebook", "")).strip()
        year_established = lead.get("year_established") or lead.get("founded")
        website_status = str(lead.get("website_status", "")).strip().lower()
        outdated = bool(lead.get("outdated", False))

        reasons: List[str] = []
        score = 0

        if self._is_big_company(name, category):
            return self._cold_response(
                name,
                score=0,
                reasons=["Skipped because the business appears to be a larger company or corporation."],
            )

        age_bonus, age_reason = self._score_business_age(year_established)
        score += age_bonus
        reasons.append(age_reason)

        website_bonus, website_reason = self._score_website(website, website_status, outdated)
        score += website_bonus
        reasons.append(website_reason)

        social_bonus, social_reason = self._score_social(facebook)
        score += social_bonus
        reasons.append(social_reason)

        category_bonus, category_reason = self._score_category(category)
        score += category_bonus
        reasons.append(category_reason)

        small_business_bonus, small_business_reason = self._score_small_business(lead)
        score += small_business_bonus
        if small_business_bonus:
            reasons.append(small_business_reason)

        score = max(0, min(100, score))
        label = self._label(score)
        recommendation = self._recommendation(label)

        return {
            "name": name,
            "score": score,
            "label": label,
            "recommendation": recommendation,
            "reasons": [reason for reason in reasons if reason],
            "website_status": website_status or ("none" if not website else "unknown"),
            "target_category": self.target_category,
        }

    @staticmethod
    def _is_big_company(name: str, category: str) -> bool:
        text = f"{name} {category}".lower()
        for term in BIG_COMPANY_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", text):
                return True
        return False

    @staticmethod
    def _score_business_age(year_established: Optional[Any]) -> (int, str):
        if year_established is None:
            return 10, "No founding year available, moderate age score."
        try:
            year = int(str(year_established).strip())
            age = datetime.utcnow().year - year
            if age <= 2:
                return 20, "Young business detected, high opportunity."
            if age <= 6:
                return 10, "Recent business age, good potential."
            return 0, "Established business, lower freshness score."
        except ValueError:
            return 10, "Invalid founding year format, moderate age assumption."

    @staticmethod
    def _score_website(website: str, website_status: str, outdated: bool) -> (int, str):
        if not website:
            return 50, "No website available, high urgency for digital presence."
        if website_status == "dead":
            return 30, "Website is dead, opportunity for website repair."
        if website_status == "live":
            if outdated:
                return 10, "Website is live but outdated."
            return 5, "Website is live, but still may need optimization."
        return 10, "Website status is uncertain, treat as moderate opportunity."

    @staticmethod
    def _score_social(facebook: str) -> (int, str):
        if not facebook:
            return 20, "No Facebook profile detected; lead may need stronger social presence."
        return 0, "Social presence detected on Facebook."

    def _score_category(self, category: str) -> (int, str):
        if category and category == self.target_category:
            return 10, "Category matches the target search exactly."
        return 0, "Category does not match exactly."

    @staticmethod
    def _score_small_business(lead: Dict[str, Any]) -> (int, str):
        location_count = lead.get("location_count")
        has_llc = bool(re.search(r"\bLLC\b", str(lead.get("name", "")), re.IGNORECASE))
        if has_llc:
            return 0, "LLC structure detected, deprioritized slightly."
        if isinstance(location_count, int) and location_count == 1:
            return 10, "Single-location small business detected."
        if location_count is None:
            return 5, "Business appears small based on available data."
        return 0, "Business location profile does not indicate a small single-location operator."

    @staticmethod
    def _label(score: int) -> str:
        if score >= 70:
            return "🔥 HOT LEAD"
        if score >= 40:
            return "🟡 WARM LEAD"
        return "❌ COLD"

    @staticmethod
    def _recommendation(label: str) -> str:
        if label == "🔥 HOT LEAD":
            return "Urgent outreach recommended; website improvements are a priority."
        if label == "🟡 WARM LEAD":
            return "Potential follow-up; validate the lead before outreach."
        return "Skip this lead for now and focus on higher-priority prospects."

    @staticmethod
    def _cold_response(name: str, score: int, reasons: List[str]) -> Dict[str, Any]:
        return {
            "name": name,
            "score": score,
            "label": "❌ COLD",
            "recommendation": "Skip because this lead is likely a larger company or otherwise not a small local target.",
            "reasons": reasons,
        }


def batch_analyze(leads: List[Dict[str, Any]], target_category: str) -> List[Dict[str, Any]]:
    analyzer = LeadAnalyzer(target_category)
    return [analyzer.analyze(lead) for lead in leads]

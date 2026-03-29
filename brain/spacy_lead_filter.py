"""
Compatibility shim — Local Brain NLP lives in brain_engine (spaCy md/sm).
"""
from __future__ import annotations

from brain.brain_engine import verify_lead_text

__all__ = ["verify_lead_text"]

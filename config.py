"""
HexaLeads configuration loader.
Loads non-secret settings from config.json and secret API keys from .env.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from dotenv import dotenv_values, load_dotenv

ROOT_DIR = Path(__file__).resolve().parent
CONFIG_JSON_PATH = ROOT_DIR / "config.json"
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


def _read_json_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as stream:
        data = json.load(stream)
    if not isinstance(data, dict):
        raise ValueError(f"Configuration file {path} must contain a JSON object.")
    return data


def get_config() -> Dict[str, Any]:
    """Return non-secret settings from config.json."""
    return _read_json_config(CONFIG_JSON_PATH)


def get_env() -> Dict[str, str]:
    """Return secret environment variables loaded from .env."""
    values = dotenv_values(ENV_PATH)
    if values is None:
        return {}
    return {key: str(value) for key, value in values.items() if value is not None}


def get_env_value(key: str, default: str | None = None) -> str | None:
    """Return a single environment value from the current process."""
    return os.environ.get(key, default)


def ensure_telegram_token() -> str:
    """Return the Telegram bot token from environment or raise if missing."""
    token = get_env_value("TELEGRAM_BOT_TOKEN") or get_env_value("TELEGRAM_TOKEN")
    if token is None or not str(token).strip():
        raise RuntimeError(
            "Missing Telegram bot token. Set TELEGRAM_BOT_TOKEN in .env or your environment."
        )
    return str(token).strip()


if __name__ == "__main__":
    config = get_config()
    env = get_env()
    print("Config:", json.dumps(config, indent=2))
    print("Env keys:", sorted(env.keys()))

"""
Persistent configuration: load Telegram token from `.env` (python-dotenv) or `config.json`,
prompt once on first run, then save for future sessions.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / ".env"
CONFIG_JSON_PATH = ROOT_DIR / "config.json"


def _write_env_token(token: str) -> None:
    token = token.strip()
    if not token:
        return
    line = f"TELEGRAM_BOT_TOKEN={token}"
    if ENV_PATH.exists():
        text = ENV_PATH.read_text(encoding="utf-8")
        if re.search(r"^\s*TELEGRAM_BOT_TOKEN\s*=", text, re.MULTILINE):
            new_lines = []
            for ln in text.splitlines():
                if re.match(r"^\s*TELEGRAM_BOT_TOKEN\s*=", ln):
                    new_lines.append(line)
                else:
                    new_lines.append(ln)
            ENV_PATH.write_text("\n".join(new_lines) + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
        else:
            ENV_PATH.write_text(text.rstrip() + "\n" + line + "\n", encoding="utf-8")
    else:
        ENV_PATH.write_text(line + "\n", encoding="utf-8")


def _write_config_json_token(token: str) -> None:
    data: dict = {}
    if CONFIG_JSON_PATH.exists():
        try:
            data = json.loads(CONFIG_JSON_PATH.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    if not isinstance(data, dict):
        data = {}
    data["TELEGRAM_BOT_TOKEN"] = token.strip()
    CONFIG_JSON_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def ensure_telegram_token() -> str:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore

    if load_dotenv and ENV_PATH.exists():
        load_dotenv(ENV_PATH)

    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()

    if not token and CONFIG_JSON_PATH.exists():
        try:
            raw = json.loads(CONFIG_JSON_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                token = str(raw.get("TELEGRAM_BOT_TOKEN") or "").strip()
        except Exception:
            pass
        if token:
            os.environ["TELEGRAM_BOT_TOKEN"] = token

    if not token:
        print("No Telegram bot token found. Get one from @BotFather on Telegram.")
        token = input("Enter TELEGRAM_BOT_TOKEN (saved to .env and config.json): ").strip()
        if not token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required to run HexaLeads.")
        os.environ["TELEGRAM_BOT_TOKEN"] = token
        _write_env_token(token)
        _write_config_json_token(token)
    else:
        if not ENV_PATH.exists() or "TELEGRAM_BOT_TOKEN" not in (ENV_PATH.read_text(encoding="utf-8") if ENV_PATH.exists() else ""):
            _write_env_token(token)
        if not CONFIG_JSON_PATH.exists():
            _write_config_json_token(token)

    return token

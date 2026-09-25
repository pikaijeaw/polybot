#!/usr/bin/env python3
"""Telegram notifications — the one place every bot/watchdog sends alerts
and hourly summaries through."""

import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# Real env vars always win over .env — load_dotenv() defaults to not overriding.
load_dotenv(Path(__file__).resolve().parent / ".env")


def send_telegram_message(text: str, bot_token: str | None = None, chat_id: str | None = None) -> bool:
    """Posts text to a Telegram chat via the Bot API. Reads credentials from
    the arguments or, if omitted, TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env
    vars. Returns False (and prints the text to stderr instead) if no
    credentials are configured, OR if the API call itself fails (bad
    token/chat id, network error, rate limit, etc.) — this is a
    best-effort notification, never worth crashing the caller's trading
    loop over."""
    bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print(
            "Telegram not configured (set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID); printing summary instead:\n" + text,
            file=sys.stderr,
        )
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"Telegram send failed ({e}); printing summary instead:\n{text}", file=sys.stderr)
        return False

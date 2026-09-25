---
name: new-polybot-script
description: Use when adding a new Python script to this Polymarket trading bot project (PolyBot) — e.g. "make a script that...", "add a tool to...". Captures the established conventions for .env resolution, cross-module imports, and how scripts here get verified. Not for editing existing scripts, only for creating new ones.
---

# Adding a new script to PolyBot

Before writing, check whether an existing script already does most of what's needed (`oracle_lag_strategy.py` for signals, `paper_trading/paper_trader.py` for simulated fills, `notify.py` for Telegram) — extend rather than duplicate where it fits.

## 1. Paper only

This repo is paper-trading only — all wallet, CLOB-auth, and order-placement code was removed on purpose. Don't add a script that signs orders or sends transactions unless the user explicitly asks for live trading back; if they do, it's a design decision to raise with them, not a script to write quietly.

## 2. Loading .env / credentials

Don't invent a new loading path. If the script needs `TELEGRAM_BOT_TOKEN` etc., either import them from wherever they're already loaded (e.g. `notify.py` already calls `load_dotenv()` at import time — use `notify.send_telegram_message` rather than posting to Telegram yourself) or add your own at the top of the file, resolved relative to the project root regardless of cwd:
```python
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")  # adjust .parent count if the script lives in a subdirectory
```
Real environment variables must still take precedence — `load_dotenv()`'s default (no `override=True`) already does this; don't change it.

## 3. If the script lives in a subdirectory (like `paper_trading/`)

Root-level modules (`btc_price_feed.py`, `oracle_lag_strategy.py`, etc.) aren't importable by default from a subdirectory. Insert the project root onto `sys.path` before importing them — copy the pattern from `paper_trading/paper_trader.py`:
```python
import sys
from pathlib import Path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))  # for importing siblings in the same subdirectory

import oracle_lag_strategy as strategy  # etc.
```

## 4. Verifying it works

There's no unit test suite in this repo — new scripts are verified by actually running them against the real APIs (Binance, Polymarket Gamma/CLOB), not mocks:

- Run it live against the real public endpoints and inspect the output.
- Clean up any state files your test run created (e.g. `paper_trading/paper_state.json`) before finishing, unless the user is actively using them.

## 5. New dependencies

If you `pip install` something new into `.venv`, update the tracked manifest:
```
.venv/bin/pip freeze > requirements.txt
```

## 6. Docstring conventions

Every script here starts with a module docstring covering: what it does, why any non-obvious design choice was made (not just what the code does — see `btc_5m_market_finder.py`'s docstring for the canonical example of explaining a gotcha inline), and a `Usage:` block with real example invocations. Match this style rather than leaving the top of the file undocumented.

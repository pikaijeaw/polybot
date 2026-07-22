---
name: new-polybot-script
description: Use when adding a new Python script to this Polymarket trading bot project (PolyBot) — e.g. "make a script that...", "add a tool to...". Captures the established conventions for credential handling, dry-run safety on money-touching actions, .env resolution, cross-module imports, and how scripts here get verified. Not for editing existing scripts, only for creating new ones.
---

# Adding a new script to PolyBot

Before writing, check whether an existing script already does most of what's needed (`oracle_lag_strategy.py` for signals, `order_executor.py` for orders, `preflight_check.py` for read-only checks) — extend rather than duplicate where it fits.

## 1. Does this script touch real money or send a real transaction?

If yes (places an order, sends an on-chain tx, moves funds) — this is the required pattern, modeled on `order_executor.py` and `approve_usdc.py`:

- Default to a **dry run**: build and print exactly what would happen, no network mutation. Print something like `mode={'LIVE' if live else 'DRY RUN'}` so it's unambiguous in the output.
- Add a `--live` flag gating any real submission.
- Even with `--live`, prompt for a **typed "yes"** per action unless `--yes` is also passed:
  ```python
  def confirm_or_abort(summary: str, auto_yes: bool) -> bool:
      print(summary)
      if auto_yes:
          return True
      try:
          answer = input("Type 'yes' to submit this transaction: ")
      except EOFError:
          answer = ""
      if answer.strip().lower() != "yes":
          print("Not confirmed; aborting.", file=sys.stderr)
          return False
      return True
  ```
- **Never accept a private key as a CLI argument.** Reuse `order_executor.py`'s credential loading instead of reimplementing it:
  ```python
  from order_executor import load_private_key, DEFAULT_CHAIN_ID
  # add these same flags: --private-key-env (default POLYMARKET_PRIVATE_KEY), --keystore
  private_key = load_private_key(args)  # call this exactly once — --keystore prompts for a
                                          # password, and calling it twice double-prompts
  ```
  If you also need a `ClobClient`, construct it directly with the already-loaded `private_key` rather than calling `order_executor.build_client(args)`, which calls `load_private_key` again internally.
- If the script name/purpose could be confused with an existing `--live` flag that means something *less* dangerous (e.g. `oracle_lag_strategy.py --live` just means "use live data, still read-only"), say so explicitly in the new script's docstring to avoid the ambiguity biting someone later.

If the script is read-only (checks, discovery, monitoring) — no `--live`/`--yes` needed, model it on `preflight_check.py` or `btc_5m_market_finder.py` instead.

## 2. Loading .env / credentials

Don't invent a new loading path. If the script needs `POLYMARKET_PRIVATE_KEY`, `TELEGRAM_BOT_TOKEN`, etc., either import them from wherever they're already loaded (e.g. `order_executor.py` already calls `load_dotenv()` at import time) or add your own at the top of the file, resolved relative to the project root regardless of cwd:
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

- For read-only scripts: just run it live and inspect the output.
- For anything that spends money or signs transactions: generate a disposable throwaway wallet with zero balance and run the **real** flow against it —
  ```python
  from eth_account import Account
  acct = Account.create()  # never funded, never reused
  ```
  This confirms the full request/signing/submission path reaches the real API and fails for the *expected* reason (insufficient balance, geoblock, etc.) rather than silently masking a bug with a mock. Confirm the dry-run path separately (prints the right thing, sends nothing), then confirm `--live` reaches the network and fails cleanly (not a stack trace) when underfunded.
- Clean up any state files your test run created (e.g. `paper_trading/paper_state.json`) before finishing, unless the user is actively using them.

## 5. New dependencies

If you `pip install` something new into `.venv`, update the tracked manifest:
```
.venv/bin/pip freeze > requirements.txt
```

## 6. Docstring conventions

Every script here starts with a module docstring covering: what it does, why any non-obvious design choice was made (not just what the code does — see `btc_5m_market_finder.py`'s docstring for the canonical example of explaining a gotcha inline), and a `Usage:` block with real example invocations. Match this style rather than leaving the top of the file undocumented.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PolyBot: a Python bot for trading Polymarket's recurring 5-minute "Bitcoin Up or Down" markets, built around an "oracle lag" edge — Binance spot prices move continuously and lead the Chainlink-based oracle these markets resolve against, so watching Binance gives an early read on which way a window will close before Polymarket's own order book has repriced.

Not a git repository yet. No `pyproject.toml`/`setup.py` — dependencies live only in `.venv`, tracked via `requirements.txt` (see below).

## Architecture

Root-level scripts, each independently runnable, several importing from each other as plain modules:

- **`btc_price_feed.py`** — real-time BTC price from Binance's public WebSocket (trade/bookTicker/kline), with a REST-polling fallback if the socket drops.
- **`btc_5m_market_finder.py`** — discovers the live/upcoming Polymarket 5-min BTC markets. No API key needed.
- **`oracle_lag_strategy.py`** — the strategy engine: Brownian-motion probability model + quarter-Kelly position sizing + hourly Telegram summaries. Imports the two scripts above. `--live` runs against real feeds but **only prints signals — it never places orders.**
- **`order_executor.py`** — places/cancels real CLOB orders via `py_clob_client`. Owns credential loading (`load_private_key`, `build_client`) that other scripts reuse.
- **`preflight_check.py`** — read-only pre-trade checks (wallet balances, CLOB auth levels, USDC allowances). Imports credential loading from `order_executor.py`.
- **`approve_usdc.py`** — sends real on-chain `approve()` transactions granting Polymarket's exchange contracts a USDC allowance. Also imports from `order_executor.py`.
- **`performance_tracker.py`** — schemas + `PerformanceTracker` class for three append-only CSV logs (`signals.csv`, `trades.csv`, `executions.csv`). Library only; wired in by `paper_trader.py` and `live_trader.py`.
- **`pidfile.py`** — tiny single-instance-protection helper (`claim_or_exit`, `pid_is_alive`, `read_pid`). Any long-running bot process should claim a pidfile on startup so a process-manager (or a human) can reliably tell "is this already running?" without holding a live subprocess handle — see `paper_trader.py` and `live_trader.py`. `pid_is_alive` treats a zombie (exited, not yet reaped by its parent) as **not** alive — `os.kill(pid, 0)` alone would report a zombie as running, which is wrong for every caller here. Also owns the **autorestart marker** (`write_autorestart_marker` / `clear_autorestart_marker` / `read_autorestart_marker`) used by `watchdog.py` — see below.
- **`watchdog.py`** — polls whether `paper_trader.py` / `live_trader.py` are alive and restarts + Telegram-alerts on an unexpected crash. See the dedicated section below.
- **`createwallet/create_polygon_wallet.py`** — standalone, generates a new Polygon wallet.
- **`paper_trading/`** — the paper-trading bot, run as two independent processes on purpose (restarting one never interrupts the other):
  - `paper_trader.py` runs the same live pipeline as `oracle_lag_strategy.py --live` but simulates fills against a virtual bankroll — no wallet, no CLOB auth, no funds at risk. Claims a pidfile (`paper_trader.pid`) on startup.
  - `web_dashboard.py` — the Flask web UI (`:5000` by default). Reads `paper_trader.py`'s state file/trades log from disk only; never imports or talks to the bot process directly. Also doubles as the **bot control plane** — see below.
  - `dashboard.py` — read-only terminal dashboard alternative (`rich`-based), reused by `web_dashboard.py` for its price/wallet-watching logic.
  - Because this subdirectory imports root-level modules, these scripts insert the project root onto `sys.path` before importing — preserve this pattern (see any of their top-of-file comments) if you add another script here or another subdirectory like it.
- **`live_trading/live_trader.py`** — the real-money counterpart to `paper_trader.py`. See the dedicated section below before touching this file; its safety model is deliberately different from the rest of the codebase.

Run any script with `.venv/bin/python <script>.py --help` to see its flags — usage examples are in each script's module docstring, not repeated here.

## Bot control plane (`web_dashboard.py`)

`paper_trading/web_dashboard.py` owns start/stop/status for both `paper_trader.py` and `live_trading/live_trader.py`, spawning each as a subprocess (`/api/bot/start`, `/api/bot/stop`, `/api/bot/status`) and exposing a read-only `/api/preflight`. It tells whether a bot is running by checking that bot's **pidfile** (`pidfile.py`), not by holding onto the `subprocess.Popen` handle — a dashboard restart would otherwise lose track of a still-running bot (or worse, spawn a duplicate on top of it). Starting `mode=live` always re-runs `preflight_check.py`'s full check set first and refuses with HTTP 412 if it isn't READY; there is no way to skip that from the dashboard. Keep `--host` at its `127.0.0.1` default for this script specifically — these endpoints can start a live-money bot, so don't expose them beyond localhost without treating that as a real access-control decision. `_bot_status` opportunistically calls `.poll()` on any `bot_processes` handle it holds, so a crashed child doesn't linger as a zombie between explicit stop calls.

## Watchdog (`watchdog.py`)

Independent process (its own pidfile) that polls whether `paper_trader.py` / `live_trader.py` are alive and, if one has gone down unexpectedly, restarts it with its last-known args and sends a Telegram alert via `oracle_lag_strategy.send_telegram_message`. Deliberately separate from `web_dashboard.py` — a watchdog crash shouldn't take a bot down, and a bot's ability to be resurrected shouldn't depend on the dashboard being up.

The one thing this has to get right: telling "crashed" apart from "the user meant to stop this," especially once it's watching `live_trader.py` — resurrecting a bot someone deliberately stopped would undo the whole point of the Stop button. Both bots write an **autorestart marker** (`pidfile.write_autorestart_marker`) right after claiming their pidfile, and clear it **only** from the `except asyncio.CancelledError` branch in `main()` — i.e. only on a signal-triggered graceful shutdown (Ctrl+C, or the dashboard's Stop button sending SIGTERM). Any other exception path leaves the marker in place, since that's a real crash. So watchdog.py's rule is simple: marker present + pidfile dead → crashed, restart it; marker absent → intentionally stopped, leave it down. Do **not** move the marker-clearing call into a generic `finally`/`atexit` block — those also fire after an uncaught exception, which would erase the evidence a crash needs to be detected.

`--max-restarts-per-hour` (default 5) stops the watchdog from restart-looping a bot that immediately crashes every time (e.g. a live bot that's lost its USDC allowance will pass its own preflight-fail exit every restart) — past the limit it sends one "needs manual attention" alert and stops trying, rather than repeatedly hammering the CLOB and spamming Telegram.

## The safety pattern — required for any new money-touching script

`order_executor.py` and `approve_usdc.py` each independently implement the same pattern for anything that spends money or sends a real transaction. Follow it for any new script in this category:

- Default to a **dry run** that prints exactly what would happen (`mode=DRY RUN`), with no network mutation.
- Require an explicit **`--live`** flag to actually submit/broadcast.
- Even with `--live`, require a **typed "yes" confirmation** per action unless **`--yes`** is also passed.
- Never accept a private key via CLI argument (shell history, `ps` exposure) — only via `--private-key-env VAR` (env var name) or `--keystore PATH` (encrypted, password-prompted). Reuse `order_executor.load_private_key` rather than reimplementing this.
- API/session credentials are derived fresh each run, never cached to disk.

**Naming collision to watch for**: `oracle_lag_strategy.py --live` means "run against live data feeds" and is still read-only (no orders placed). `order_executor.py --live` and `approve_usdc.py --live` mean "actually submit a real transaction." Same flag name, very different consequences — don't assume `--live` is safe just because it is in one script.

### The one deliberate exception: `live_trading/live_trader.py`

This script *is* live trading by definition (there's no separate `--live` flag) — it's meant to run unattended from the dashboard's Start button, so a per-trade `input()` confirmation would just hang forever on a closed stdin. Confirmation is replaced with caps, not removed:

- **`preflight_check.py` must return READY before this process starts trading at all** — reused directly via `preflight_check.run_all_checks()`, not reimplemented, and not skippable via any flag. If you add a new preflight section, `live_trader.py`'s gate picks it up automatically.
- **`--max-stake-usd`** is a hard per-trade cap enforced through `order_executor.check_market_order` (the same risk check `order_executor.py` itself uses) — Kelly sizing is clamped down to this, never allowed to exceed it.
- **`--max-trades-per-hour` / `--max-trades-per-day`** (`RateLimiter`) is a circuit breaker independent of signal quality.
- **`--max-daily-loss-usd`** (`DailyLossGuard`) is an *optional*, off-by-default cumulative stop-loss — the three caps above bound risk per trade and per hour but not total daily drawdown.
- Order submission reuses `order_executor`'s `create_market_order`/`post_order` call directly (the exact signing path `order_executor.py` itself already exercises) rather than reimplementing signing — see `submit_live_market_order()`. It deliberately does **not** go through `order_executor.place_market_order`, whose `confirm_or_abort()` calls `input()`.
- Scope limit: `live_trader.py` tracks and reports positions (polling Gamma resolution, same as `paper_trader.py`) but does **not** redeem/claim winning conditional-token positions on-chain — that's still a separate manual step. `pnl_usd` there is an accounting estimate, not a confirmed on-chain settlement.

If you extend this script, keep it unattended-safe: no `input()`, no prompt that blocks on stdin, and no way to bypass the preflight gate.

## Testing convention

There is no unit test suite (no `pytest` usage despite `pytest` being installed, no `test_*.py` files). The established convention instead: **verify by actually running the script against real endpoints** — Binance, Polymarket's Gamma/CLOB APIs — not mocked tests. For anything that touches real funds, verify with a disposable throwaway wallet (`eth_account.Account.create()`) that has zero balance, so you can confirm the full request/signing/submission path reaches the real API and fails for the *expected* reason (e.g. insufficient balance, geoblock) rather than a bug in the code. Keep following this convention for new scripts unless the user asks for real unit tests.

`oracle_lag_strategy.py --selftest` is the one exception — a synthetic, no-network sanity check of the probability/Kelly math.

## Dependencies

Track new packages in `requirements.txt` — after `pip install`-ing something new into `.venv`, run:
```
.venv/bin/pip freeze > requirements.txt
```

## Linting

`ruff` is configured (`ruff.toml`) — run `.venv/bin/ruff check .` to lint, `.venv/bin/ruff format .` to format. A `PostToolUse` hook (`.claude/settings.json`) auto-runs both on any `.py` file Claude writes or edits. `jq` isn't installed in this environment, so the hook parses its stdin JSON with `python3 -c` instead — keep that in mind if you touch the hook command.

## Key gotchas

- **5-minute market discovery** (`btc_5m_market_finder.py`): Polymarket appears to batch pre-create a full day's worth of future 5-min windows in advance. Asking Gamma for "most recently created" markets surfaces *tomorrow's* freshly-created placeholder windows, not today's genuinely-about-to-close one. The fix already in place: compute the expected slug (`btc-updown-5m-<epoch>`, epoch always a multiple of 300) directly from wall-clock time and look it up, rather than sorting/filtering a list. Also: don't trust Gamma's `closed` flag for these fast-cycling markets — it lags actual resolution; status is derived from epoch vs. wall clock instead.
- **Zero-drift probability model** (`oracle_lag_strategy.py`): the Brownian-motion model deliberately assumes zero drift. Over a 5-minute horizon, real trend is swamped by noise, and zero drift keeps the estimate a pure martingale with no built-in directional bias — the actual edge comes from seeing the price before the market/oracle does, not from predicting direction.
- **Quarter-Kelly default**: `kelly_multiplier=0.25` is deliberate, not conservative-for-its-own-sake — the probability model is an approximation, not a known-true probability, so full Kelly would be overconfident. `max_position_pct` is a separate hard cap independent of what Kelly suggests.
- **CLOB clock skew**: `preflight_check.py` checks skew against the CLOB server explicitly because Level-2 auth signatures are timestamp-sensitive (PASS ≤10s, WARN ≤60s, FAIL >60s). If CLOB auth is failing mysteriously, check this first.
- **Allowances are read live, not computed locally**: `preflight_check.py` and `approve_usdc.py` both fetch current USDC spender allowances from `get_balance_allowance` rather than hardcoding exchange contract addresses — the addresses returned by that live endpoint don't match what's bundled in `py_clob_client`'s local config, so hardcoding would be wrong.
- **Trade settlement source of truth**: `paper_trader.py` prefers Polymarket's own Gamma API resolution (`outcomePrices`) over the bot's own price-based guess. Only falls back to comparing last-known price to the window anchor after `SETTLE_FALLBACK_AFTER` (120s) with no Gamma resolution — and flags that fallback explicitly (`resolution_source: "price-fallback"` vs `"gamma"`) so it's never silently conflated with a real settlement.
- **`trades.csv` is a post-trade blotter, not a lifecycle log**: one row per trade, appended only once it resolves (entry+fill+exit+resolution together). Open positions are visible live via `paper_state.json` instead — don't try to append an "OPEN" row to `trades.csv` for a new trade type; follow the existing pattern.
- **`.env`** is loaded automatically by scripts that need it (via `python-dotenv`, resolved relative to the project root regardless of cwd) — real environment variables still take precedence over it.
- **`resolve_market_outcome()` lives in `btc_5m_market_finder.py`**, not in `paper_trader.py`/`live_trader.py` — both bots import and call the same function so settlement logic can't silently drift between them. Add any future settlement-source changes there once, not per-bot.
- **Pidfile-based single-instance protection**: both bots call `pidfile.claim_or_exit()` at startup and exit immediately if another instance is already alive per their pidfile. This is deliberate for `paper_trader.py` too (not just `live_trader.py`) — two paper bots sharing one state file would corrupt it just as surely as two live bots sharing one wallet would double-trade it.

## Subdirectory CLAUDE.md

`paper_trading/` could get its own `CLAUDE.md` if instructions specific to that subsystem grow — ask if useful before adding one.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

PolyBot: Python paper-trading bots for Polymarket — two on the web dashboard (Arbitrage (Pair Cost) on the 5-minute "Bitcoin Up or Down" markets, and Copy Trading of another account) plus the standalone Gabagool arbitrage bot on the 15-minute series.

**Paper trading only.** All wallet, CLOB-auth, and order-placement code was deliberately removed — it's in git history if ever needed. Don't reintroduce order placement unless the user explicitly asks.

The directional strategies this repo started with, the watchdog and the terminal dashboard were removed on 2026-10-13 at the user's request ("not use this strategy"). Don't bring them back, or build on their ideas, unless asked.

No `pyproject.toml`/`setup.py` — dependencies live only in `.venv`, tracked via `requirements.txt` (see below).

## Architecture

Root-level modules, importable by the bots as plain modules:

- **`btc_5m_market_finder.py`** — discovers the live/upcoming Polymarket 5-min BTC markets, reads CLOB order books (`fetch_order_book`, `fetch_best_ask`) and resolves outcomes (`resolve_market_outcome`). No API key needed.
- **`btc_15m_market_finder.py`** — same for the 15-min series (`btc-updown-15m-<n>`, 900s windows). Reuses `resolve_market_outcome`/`fetch_order_book` from the 5m finder by import.
- **`gabagool_strategy.py`** — `GabagoolEngine`: YES/NO order-book arbitrage on the 15-min series, ported from [minculusofia-wq/Apex-Predator](https://github.com/minculusofia-wq/Apex-Predator). Buys both legs at once, only on a crossed book. Also owns the fee formula (`fee_for_fill`, `FEE_RATE_CRYPTO`) the other bots import.
- **`performance_tracker.py`** — `PerformanceTracker` (append-only `signals.csv`/`trades.csv`, plus `runs.jsonl`), `append_jsonl`, and `LOG_ROOT`. Library; every dashboard bot logs through it. Its columns were trimmed on 2026-10-13 to what the current bots fill.
- **`notify.py`** — `send_telegram_message()`, the single Telegram sender. Loads `.env` itself. Best-effort: prints to stderr instead of raising if unconfigured or the API call fails. Only `gabagool_paper_trader.py` uses it now.
- **`pidfile.py`** — single-instance protection (`claim_or_exit`, `pid_is_alive`, `read_pid`). Any long-running bot should claim a pidfile on startup. `pid_is_alive` treats a zombie as **not** alive — `os.kill(pid, 0)` alone would report it as running. Also has autorestart-marker helpers; nothing reads the marker now (only `gabagool_paper_trader.py` writes it).

**`paper_trading/`**:

- `pair_cost_paper_trader.py` — "Arbitrage (Pair Cost)" on the 5-min series, engine and loop in one file. Unlike Gabagool it **accumulates**: from a balanced position it opens a leg on a side whose ask is `<= --entry-price`, then hedges the lagging side only while fee-inclusive `avg_up + avg_down <= 1 - --min-margin`. Legs are sized in **shares**: `--clip-usd` is the budget for a whole pair, so the first leg is that many shares (a pair never completes above $1/share). Sizing the first leg in dollars let a 6¢ leg demand a hedge 14x its size. An unhedged leg is a real directional bet. **Fills are simulated as real Fill-And-Kill limit orders** (`fill_from_book`): after `--latency-ms` the book is re-read and the order takes only ask levels at or under the decision price, up to their sizes, rejecting orders under the CLOB's `min_order_size` (5 shares, verified on `/book`) and cutting sizes to 0.01 shares; a miss is a `MISS` event. A gap under 5 shares can't be hedged and counts as balanced. Don't go back to filling at the decision-time ask — that is what made paper results better than a real account could get. Settles on Gamma only, no price fallback — unequal share counts make the payout outcome-dependent. No Telegram summary. `--selftest` checks the leg-in/hedge/refuse rules.
- `pair_cost_v2_paper_trader.py` — Arbitrage v2: subclasses v1 (`PairCostV2PaperTrader(PairCostPaperTrader)`) and adds one rule in front of it — when balanced and Up+Down can be bought together for under $1 **after fees** (by `--min-margin`), buy both at once (`price_pair`, sized by `gabagool_strategy.size_arbitrage`). Both legs are simulated as FAK orders via v1's `fill_from_book`; if one comes up short the gap is an ordinary unhedged leg left to v1's hedge rule (`PAIR` event with `risk_free: false`). Otherwise falls back to v1's legging; `--pair-only` disables v1's leg opening by passing `entry_price=-1` (hedges still run). Own state/pid/log folder (`arbitrage_pair_cost_v2`), dashboard mode `arb2`. A crossed book is rare: a 2-minute live sample on 2026-10-14 had the asks under $1 in 8/124 checks, profitable after fees in 1 — expect `--pair-only` to sit idle a lot.
- `copy_paper_trader.py` — paper copy trading: polls Polymarket's public data API (`data-api.polymarket.com/activity?user=<wallet>&type=TRADE`) for one account (`--follow` username / profile URL / 0x wallet, default `gabagool22`) and mirrors fills made **after startup**, scaled by `--ratio`, on any market. Fills at the live best ask/bid when the trade is seen (not the leader's price), skipped past `--max-slippage`; scaled fills accumulate per token until worth $1. Has its own generic Gamma resolver (`fetch_resolution`, per-outcome-index payout) because `resolve_market_outcome` only understands Up/Down markets. Applies the 0.07 crypto fee rate to every market (`--fee-rate`). gabagool22 itself last traded 2026-02-20, so the default sits idle. `--selftest` checks the accounting.
- `gabagool_paper_trader.py` — the Gabagool loop (BTC 15-min). Own state file (`gabagool_state.json`), trades log and pidfile, all beside the script; not on the dashboard and not in `log_collector/`. Run it directly.
- `web_dashboard.py` + `templates/dashboard.html` — the Flask web UI (`:5000`), one panel per bot, and the **bot control plane** — see below.
- Scripts here insert the project root onto `sys.path` before importing root-level modules — preserve this pattern if you add another script here.

Run any script with `.venv/bin/python <script>.py --help` to see its flags — usage examples are in each script's module docstring.

## Bot control plane (`web_dashboard.py`)

Three bots, `arb` (`pair_cost_paper_trader.py`), `arb2` (`pair_cost_v2_paper_trader.py`) and `copy` (`copy_paper_trader.py`), registered in `BOT_SCRIPTS` / `ARGS_BUILDERS` / `LOG_FOLDERS` (`PAPER_STRATEGIES` is derived from `BOT_SCRIPTS`). Each is spawned as its own subprocess (`/api/bot/start`, `/api/bot/stop`, `/api/bot/status`). Each script's own state/pid/log defaults are unique, so the dashboard passes no path overrides — a new bot must keep that property or get explicit paths.

- Liveness comes from the bot's **pidfile**, not the `subprocess.Popen` handle — a dashboard restart would otherwise lose track of a running bot or spawn a duplicate. `_bot_status` also `.poll()`s any handle it holds so a crashed child doesn't linger as a zombie.
- `/api/data` returns every bot's state in one payload (`bots.arb`, `bots.arb2`, `bots.copy`). The page reads only this.
- **Shared state shape.** Every dashboard bot writes a state file with `starting_bankroll`, `equity`, `stats{trades,wins,losses,win_rate,realized_pnl,total_staked,return_pct}`, `open_positions` (a **list** of `{market_slug, side, entry_price, stake_usd, end_epoch?}`), `recent_closed` (same plus `status` `WON`/`LOST`, `pnl_usd`) and `engine{market_slug?, rows}` where `rows` is `[label, value]` pairs the panel prints as-is. One renderer (`renderPanel`), one CSV export and one reset route serve all bots because of this.
- Adding a bot to the page: a panel with ids `<mode>-dot`, `-status-text`, `-engine`, `-stats`, `-orders`, `-start-btn`, `-stop-btn`, `-reset-btn`, `-reset-confirm-row`, `-reset-confirm-input`, `-msg`; the mode in the JS `BOTS` array; a `paramsFor` branch. A missing id breaks status for every panel.
- `/api/<strategy>/reset` **archives** (moves, never deletes) that bot's state file and its whole log folder into `log_collector/archive/<folder>_<UTC date>_<time>/`; 409 while the bot is running. `/api/killswitch` stops every bot, then does the same for each.
- Keep `--host` at its `127.0.0.1` default — these endpoints start/stop processes and move state, so don't expose them beyond localhost without treating that as a real access-control decision.

## Logs (`log_collector/`)

Every dashboard bot writes all of its logs into `log_collector/<folder>/` at the project root — **read `log_collector/README.md` first**, it lists the folders, files and event types. Folder names differ from the dashboard mode keys (`web_dashboard.LOG_FOLDERS`): `arb`→`arbitrage_pair_cost`, `arb2`→`arbitrage_pair_cost_v2`, `copy`→`copy_trading`. The root is defined once, `performance_tracker.LOG_ROOT`. State and pid files are not logs and stay in `paper_trading/`.

Things the README doesn't say: all JSON lines go through `performance_tracker.append_jsonl` (stamps `ts`/`ts_iso`); `runs.jsonl` is written by `PerformanceTracker.__init__`; the dashboard's CSV export reads only `OPEN`/`CLOSE` lines from `events.jsonl` (`load_trade_history`), so new event types can be added freely; `trades.csv` prices and stakes are fee-inclusive. `signals.csv` is large — analyse it with a script, don't read it whole. `paper_trading/archive/` (local, gitignored) holds only runs of the removed bots from before 2026-10-12, measured on the wrong window (see the slug gotcha) — ignore it.

## Testing convention

There is no unit test suite (no `test_*.py` files). The established convention instead: **verify by actually running the script against real endpoints** — Polymarket's Gamma/CLOB/data APIs — not mocked tests. Keep following this convention for new scripts unless the user asks for real unit tests.

`paper_trading/pair_cost_paper_trader.py --selftest`, `paper_trading/pair_cost_v2_paper_trader.py --selftest`, `paper_trading/copy_paper_trader.py --selftest` and `gabagool_strategy.py --selftest` are the exceptions — synthetic, no-network checks of the leg-in/hedge rules, the copy accounting, and the fee-aware arbitrage sizing. A short live run proves the plumbing; settlement needs a market that has resolved (Gamma lands 6-10+ min after close), so test it by seeding a position on an already-resolved slug.

## Dependencies

Track new packages in `requirements.txt` — after `pip install`-ing something new into `.venv`, run:
```
.venv/bin/pip freeze > requirements.txt
```

## Linting

`ruff` is configured (`ruff.toml`) — run `.venv/bin/ruff check .` to lint, `.venv/bin/ruff format .` to format. A `PostToolUse` hook (`.claude/settings.json`) auto-runs both on any `.py` file Claude writes or edits. `jq` isn't installed in this environment, so the hook parses its stdin JSON with `python3 -c` instead — keep that in mind if you touch the hook command.

## Key gotchas

- **Slug number = window START, `Market.epoch` = window CLOSE** (`btc_5m_market_finder.py`, `btc_15m_market_finder.py`): in `btc-updown-5m-<n>`, `<n>` is when the window opens (Gamma: `eventStartTime == n`, `endDate == n + 300`; same for 15m with 900). `Market.epoch` adds the window length, so it is the close time callers use as `end_epoch`. Until 2026-10-12 the finders read `<n>` as the close; any data from before that date was measured on the wrong window.
- **Market discovery**: Polymarket batch pre-creates a full day's worth of future windows. Asking Gamma for "most recently created" markets surfaces *tomorrow's* placeholders, so the finders compute the expected slug from wall-clock time and look it up. Don't trust Gamma's `closed` flag for these fast-cycling markets — it lags actual resolution; status is derived from epoch vs. wall clock.
- **Settlement source of truth is Gamma** (`outcomePrices`, queried with `closed=true` — without that filter Gamma drops a slug within ~10 minutes of its close). Resolution was measured landing 6-10+ min after close. `resolve_market_outcome()` lives in `btc_5m_market_finder.py` so Up/Down settlement can't drift between bots; add settlement-source changes there once.
- **`trades.csv` is a post-trade blotter, not a lifecycle log**: one row per trade, appended only once it resolves. Open positions are visible live in the bot's state file.
- **`.env`** is loaded automatically by scripts that need it (via `python-dotenv`, resolved relative to the project root regardless of cwd) — real environment variables still take precedence over it.
- **Pidfile-based single-instance protection**: every bot calls `pidfile.claim_or_exit()` at startup. Two instances sharing one state file would corrupt it.
- **Paper fills use the live CLOB book, not Gamma**: Gamma's `bestAsk` was measured lagging the real book, and `1 - up_bid` is only an approximation of Down's real cost.
- **Order book ask ordering** (`/book` from `clob.polymarket.com`): `asks` comes back sorted **descending** (worst price first, best/lowest price last) — the opposite of `bids`. `gabagool_strategy._sorted_asks` re-sorts explicitly; use it or `min()`, never `asks[0]`.
- **The fee formula is load-bearing, not a footnote**: `fee = shares * fee_rate * price * (1 - price)`, taker-only, `fee_rate = 0.07` for crypto-category markets — verified against `docs.polymarket.com/trading/fees.md` and the live `/fee-rate` endpoint. It is symmetric around 50¢ and *maximized* there, which is exactly where arbitrage looks best — a fee-blind "sum < $0.975" filter approves guaranteed losses. If the fee schedule changes, update `FEE_RATE_CRYPTO` and the bots' `--fee-rate` defaults.
- **Copying one leg of a two-sided trader is not the same trade**: many accounts worth following buy both outcomes of a market. `copy_paper_trader.py` decides each fill independently, so a slippage skip on one side turns the leader's hedge into a naked bet.

## Subdirectory CLAUDE.md

`paper_trading/` could get its own `CLAUDE.md` if instructions specific to that subsystem grow — ask if useful before adding one.

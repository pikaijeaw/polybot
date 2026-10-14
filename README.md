# PolyBot

Paper-trading bots for Polymarket, with one web dashboard to start, stop and watch them.

- **Arbitrage (Pair Cost)** — on the recurring 5-minute "Bitcoin Up or Down" markets, buys Up and Down at different moments so their average combined cost (fees included) stays under the $1 a matched pair pays.
- **Arbitrage (Pair Cost) v2** — the same bot, but whenever Up and Down can be bought together for under $1 after fees it buys both at once, which locks the profit with no risk. Otherwise it legs in like v1, or with "Risk-free pairs only" it waits.
- **Copy Trading** — follows one Polymarket account and mirrors its trades, scaled down and filled at live prices, on any market.
- **Gabagool** — buys both sides at once on the 15-minute BTC markets, only when the order book is crossed. Run from the command line; not on the dashboard.

Scripts are independently runnable and import from each other as plain modules — there's no package/build step, just a `.venv`.

## Status

Not affiliated with Polymarket or Binance. Educational/personal-use project. Paper trading only — no wallet, no order placement, no funds at risk.

## How it works

- **`btc_5m_market_finder.py`** / **`btc_15m_market_finder.py`** locate the live Polymarket BTC market by computing its slug directly from wall-clock time, read order books, and look up how a market resolved — see [Key gotchas](#key-gotchas).
- **`paper_trading/pair_cost_paper_trader.py`** — the Arbitrage (Pair Cost) bot.
- **`paper_trading/pair_cost_v2_paper_trader.py`** — Arbitrage v2 (`--pair-only` for risk-free pairs only).
- **`paper_trading/copy_paper_trader.py`** — the Copy Trading bot (`--follow <username | profile URL | 0x wallet>`).
- **`paper_trading/gabagool_paper_trader.py`** + **`gabagool_strategy.py`** — the Gabagool bot.
- **`paper_trading/web_dashboard.py`** — the dashboard.
- **`performance_tracker.py`** writes each bot's logs; **`notify.py`** sends Telegram messages; **`pidfile.py`** stops a bot from being started twice.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in only what you need — see below
```

Every script supports `--help` for its full flag list and has a usage example in its module docstring.

### `.env`

Copy `.env.example` to `.env` and fill in what you need. Nothing is required — the only variables are optional Telegram credentials:

| Variable | Needed for |
|---|---|
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Gabagool's hourly summary, sent via `notify.py`. |

`.env` is gitignored; `.env.example` is the shareable template — never put real secrets in it.

## Running it

```bash
.venv/bin/python paper_trading/web_dashboard.py   # http://127.0.0.1:5000
```
Start and stop the bots from their panels. The dashboard and the bots are separate processes — the dashboard only reads each bot's state file from disk, so restarting one never interrupts the other.

A bot can also be run directly, and its panel still shows it:
```bash
.venv/bin/python paper_trading/pair_cost_paper_trader.py --bankroll 100
.venv/bin/python paper_trading/copy_paper_trader.py --follow <account> --ratio 0.1
```

## Running in the background

Run all commands from the project root. `nohup setsid` detaches the process so it keeps running after you close the terminal. `-u` writes the log live instead of in buffered chunks.

**Easiest: run the dashboard in the background, then start bots from its buttons.** Bots started from the dashboard inherit its detached session, so they keep running after the terminal closes too.
```bash
mkdir -p log_collector
nohup setsid .venv/bin/python -u paper_trading/web_dashboard.py > log_collector/dashboard.log 2>&1 < /dev/null &
echo $! > paper_trading/web_dashboard.pid        # the dashboard has no pidfile of its own — save it
```
Then open http://127.0.0.1:5000. The bots keep running if you stop the dashboard; restart it any time.

**Check / watch / stop:**
```bash
cat paper_trading/pair_cost_paper_trader.pid              # running? (file only exists while it is)
tail -f log_collector/arbitrage_pair_cost/bot.log         # live log — Ctrl+C stops watching, not the bot
kill $(cat paper_trading/pair_cost_paper_trader.pid)      # graceful stop (the Stop button does the same)
kill $(cat paper_trading/web_dashboard.pid)               # stop the dashboard
```
Stop by PID as above, not with `pkill -f web_dashboard`: `-f` also matches any shell whose command line contains that text, and can kill your own terminal.

**Logs for analysis**: every bot writes into its own sub-folder of `log_collector/` at the project root. [log_collector/README.md](log_collector/README.md) lists the folders and what each file holds. The dashboard's Reset moves a bot's folder to `log_collector/archive/` and never deletes it.

**WSL note:** background processes only live as long as the WSL VM. Closing every WSL terminal lets Windows shut the VM down after a short idle, which kills the bots. Keep at least one WSL window open while the bots run.

## Key gotchas

- **Market discovery**: Polymarket pre-creates a full day of future 5-minute windows in advance, so sorting Gamma by "recently created" surfaces tomorrow's placeholder markets, not today's live one. The finders compute the expected slug directly from wall-clock time instead. The number in the slug is the window's **start**; it closes 300 seconds later (900 for the 15-minute series).
- **Fees decide arbitrage**: Polymarket's taker fee is `shares * 0.07 * price * (1 - price)` on crypto markets, largest near 50¢. Both arbitrage bots count it in every cost.
- **Pidfile-based single-instance protection**: each bot claims a pidfile on startup and exits if another instance is already running — two instances sharing one state file would corrupt it.

See `CLAUDE.md` for the full architecture writeup.

## Testing

There's no unit test suite. The convention here is to verify by running scripts against the real Polymarket endpoints they talk to.

`paper_trading/pair_cost_paper_trader.py --selftest`, `paper_trading/pair_cost_v2_paper_trader.py --selftest`, `paper_trading/copy_paper_trader.py --selftest` and `gabagool_strategy.py --selftest` are the exceptions — synthetic, no-network checks of each bot's rules.

## Linting

```bash
.venv/bin/ruff check .
.venv/bin/ruff format .
```

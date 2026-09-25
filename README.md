# PolyBot

A Python bot for trading Polymarket's recurring 5-minute "Bitcoin Up or Down" markets, built around an **oracle lag** edge: Binance spot prices move continuously and lead the Chainlink-based oracle these markets resolve against, so watching Binance gives an early read on which way a window will close before Polymarket's own order book has repriced.

Root-level scripts are each independently runnable and import from each other as plain modules — there's no package/build step, just a `.venv`.

## Status

Not affiliated with Polymarket or Binance. Educational/personal-use project. Paper trading only — no wallet, no order placement, no funds at risk.

## How it works

1. **`btc_price_feed.py`** streams real-time BTC price from Binance's public WebSocket (falls back to REST polling if the socket drops).
2. **`btc_5m_market_finder.py`** locates the live/currently-open Polymarket 5-minute BTC market by computing its slug directly from wall-clock time (`btc-updown-5m-<epoch>`), rather than trusting Gamma's "recently created" ordering — see [Key gotchas](#key-gotchas).
3. **`oracle_lag_strategy.py`** combines the two: a zero-drift Brownian-motion model estimates P(price closes above the window's anchor) from Binance's live price, sized with quarter-Kelly. Run with `--live` to see real signals printed to the console — **it never places orders itself.**
4. **`paper_trading/paper_trader.py`** simulates fills for those signals against a virtual bankroll (v1, or `--v2`/`--v3` for the stricter/compounding engines, or `--early` for `early_move_strategy.py`: early entries on a clear move only, fixed stake).

```
btc_price_feed.py ─┐
                    ├─► oracle_lag_strategy.py ─► paper_trading/paper_trader.py
btc_5m_market_finder.py ─┘
```

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
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Hourly strategy summaries and watchdog crash alerts, sent via `notify.py`. |

`.env` is gitignored; `.env.example` is the shareable template — never put real secrets in it.

## Running it

**Watch signals only, no funds at risk, no wallet needed:**
```bash
.venv/bin/python oracle_lag_strategy.py --live
```

**Paper trade** (simulated fills against a virtual bankroll):
```bash
.venv/bin/python paper_trading/paper_trader.py
.venv/bin/python paper_trading/web_dashboard.py   # separate process, http://127.0.0.1:5000
```
`paper_trader.py` and `web_dashboard.py` are deliberately independent processes — the dashboard only reads the bot's state file from disk, so restarting one never interrupts the other. `paper_trading/dashboard.py` is a terminal-only alternative to the web dashboard.

**Watchdog** (optional, restarts a crashed bot and sends a Telegram alert — does not resurrect a bot you stopped deliberately):
```bash
.venv/bin/python watchdog.py
```

## Running in the background

Run all commands from the project root. `nohup setsid` detaches the process so it keeps running after you close the terminal. `-u` writes the log live instead of in buffered chunks.

**Easiest: run the dashboard in the background, then start bots from its buttons.** Bots started from the dashboard inherit its detached session, so they keep running after the terminal closes too.
```bash
nohup setsid .venv/bin/python -u paper_trading/web_dashboard.py > paper_trading/dashboard.log 2>&1 < /dev/null &
echo $! > paper_trading/web_dashboard.pid        # the dashboard has no pidfile of its own — save it
```
Then open http://127.0.0.1:5000. The bots keep running if you stop the dashboard; restart it any time.

**Or run a bot directly** (Early-Move shown; these are the same files the dashboard uses, so its panel shows this bot too):
```bash
nohup setsid .venv/bin/python -u paper_trading/paper_trader.py --early --stake-usd 5 \
  --state-file paper_trading/paper_state_early.json --trades-log paper_trading/paper_trades_early.jsonl \
  --pid-file paper_trading/paper_trader_early.pid --autorestart-marker paper_trading/paper_trader_early.autorestart.json \
  --perf-log-dir paper_trading/perf_logs_early \
  >> paper_trading/bot_early.log 2>&1 < /dev/null &
```
Plain v1 needs none of the file flags: `nohup setsid .venv/bin/python -u paper_trading/paper_trader.py >> paper_trading/bot.log 2>&1 < /dev/null &`

**Check / watch / stop:**
```bash
cat paper_trading/paper_trader_early.pid                  # running? (file only exists while it is)
tail -f paper_trading/bot_early.log                       # live log — Ctrl+C stops watching, not the bot
kill $(cat paper_trading/paper_trader_early.pid)          # graceful stop (the Stop button does the same)
kill $(cat paper_trading/web_dashboard.pid)               # stop the dashboard
```
Stop by PID as above, not with `pkill -f web_dashboard`: `-f` also matches any shell whose command line contains that text, and can kill your own terminal.

**Logs for analysis** live in `paper_trading/perf_logs_early/` (`signals.csv` = every evaluation, `trades.csv` = every settled trade). The dashboard's Reset moves them to `paper_trading/archive/<bot>-<UTC time>/` and never deletes them.

**WSL note:** background processes only live as long as the WSL VM. Closing every WSL terminal lets Windows shut the VM down after a short idle, which kills the bots. Keep at least one WSL window open while the bots run.

## Key gotchas

- **Market discovery**: Polymarket pre-creates a full day of future 5-minute windows in advance, so sorting Gamma by "recently created" surfaces tomorrow's placeholder markets, not today's about-to-close one. `btc_5m_market_finder.py` computes the expected slug directly from wall-clock time instead. It also doesn't trust Gamma's `closed` flag for these fast-cycling markets, since it lags actual resolution.
- **Zero-drift probability model**: the Brownian-motion model in `oracle_lag_strategy.py` deliberately assumes zero drift — over a 5-minute horizon, real trend is swamped by noise, and the actual edge comes from seeing the price before the market does, not from predicting direction.
- **Quarter-Kelly by default**: sizing is deliberately conservative because the probability model is an approximation, not a known-true probability.
- **Pidfile-based single-instance protection**: `paper_trader.py` and `watchdog.py` each claim a pidfile on startup and exit if another instance is already running — two instances sharing one state file would corrupt it.

See `CLAUDE.md` for the full architecture writeup, including the watchdog's crash-vs-intentional-stop detection and the bot control plane in `web_dashboard.py`.

## Testing

There's no unit test suite. The convention here is to verify by running scripts against the real Binance/Polymarket endpoints they talk to.

`oracle_lag_strategy.py --selftest` is the exception — a synthetic, no-network sanity check of the probability/Kelly math.

## Linting

```bash
.venv/bin/ruff check .
.venv/bin/ruff format .
```

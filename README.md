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

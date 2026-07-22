# PolyBot

A Python bot for trading Polymarket's recurring 5-minute "Bitcoin Up or Down" markets, built around an **oracle lag** edge: Binance spot prices move continuously and lead the Chainlink-based oracle these markets resolve against, so watching Binance gives an early read on which way a window will close before Polymarket's own order book has repriced.

Root-level scripts are each independently runnable and import from each other as plain modules — there's no package/build step, just a `.venv`.

## Status

Not affiliated with Polymarket or Binance. Educational/personal-use project. Trading involves real financial risk — see [Safety model](#safety-model) before running anything with `--live`.

## How it works

1. **`btc_price_feed.py`** streams real-time BTC price from Binance's public WebSocket (falls back to REST polling if the socket drops).
2. **`btc_5m_market_finder.py`** locates the live/currently-open Polymarket 5-minute BTC market by computing its slug directly from wall-clock time (`btc-updown-5m-<epoch>`), rather than trusting Gamma's "recently created" ordering — see [Key gotchas](#key-gotchas).
3. **`oracle_lag_strategy.py`** combines the two: a zero-drift Brownian-motion model estimates P(price closes above the window's anchor) from Binance's live price, sized with quarter-Kelly. Run with `--live` to see real signals printed to the console — **it never places orders itself.**
4. **`order_executor.py`** / **`live_trading/live_trader.py`** take signals and actually place orders on the Polymarket CLOB, once you've validated the strategy in paper trading.

```
btc_price_feed.py ─┐
                    ├─► oracle_lag_strategy.py ─► order_executor.py / live_trading/live_trader.py
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

Copy `.env.example` to `.env` and fill in what you need. Nothing here is required just to watch signals (`oracle_lag_strategy.py --live` and the market/price feed scripts only read public data):

| Variable | Needed for |
|---|---|
| `POLYMARKET_PRIVATE_KEY` | Anything that places orders or checks wallet balances (`order_executor.py`, `preflight_check.py`, `approve_usdc.py`, `live_trading/live_trader.py`). Never pass a key on the CLI — only via `--private-key-env` (reads this var) or `--keystore`. |
| `POLYMARKET_FUNDER` | Only if trading through a Polymarket proxy wallet instead of a plain EOA. |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Hourly strategy summaries and watchdog crash alerts. |

`.env` is gitignored; `.env.example` is the shareable template — never put real secrets in it.

## Running it

**Watch signals only, no funds at risk, no wallet needed:**
```bash
.venv/bin/python oracle_lag_strategy.py --live
```

**Paper trade** (simulated fills against a virtual bankroll — the recommended step before risking real money):
```bash
.venv/bin/python paper_trading/paper_trader.py
.venv/bin/python paper_trading/web_dashboard.py   # separate process, http://127.0.0.1:5000
```
`paper_trader.py` and `web_dashboard.py` are deliberately independent processes — the dashboard only reads the bot's state file from disk, so restarting one never interrupts the other. `paper_trading/dashboard.py` is a terminal-only alternative to the web dashboard.

**Before trading with real funds**, run the read-only checks and, if needed, approve USDC:
```bash
.venv/bin/python preflight_check.py
.venv/bin/python approve_usdc.py --live   # only if preflight flags a missing allowance
```

**Live trading** (real orders, real funds) — run unattended via the dashboard's Start button (`mode=live`), which always re-runs `preflight_check.py` first and refuses to start if it isn't READY, or directly:
```bash
.venv/bin/python live_trading/live_trader.py --max-stake-usd 5 --max-trades-per-hour 6
```

**Watchdog** (optional, restarts a crashed bot and sends a Telegram alert — does not resurrect a bot you stopped deliberately):
```bash
.venv/bin/python watchdog.py
```

## Safety model

- Anything that can spend money or send a real transaction (`order_executor.py`, `approve_usdc.py`) defaults to a **dry run**, requires an explicit `--live` flag to submit anything, and still asks for a typed `yes` confirmation per action unless `--yes` is also passed.
- `live_trading/live_trader.py` is the one exception to per-trade confirmation — it's meant to run unattended, so confirmation is replaced with hard caps instead: `preflight_check.py` must return READY before it starts (not skippable), `--max-stake-usd` caps every trade, and `--max-trades-per-hour` / `--max-trades-per-day` / optional `--max-daily-loss-usd` bound it further. See `CLAUDE.md` for the full detail if you're extending this script.
- A private key is never accepted via CLI argument. Supply it via `--private-key-env VAR` (an environment variable name) or `--keystore PATH` (encrypted, password-prompted).
- `oracle_lag_strategy.py --live` means "run against live data feeds" and is still read-only — it never places orders. `order_executor.py --live` and `approve_usdc.py --live` mean "actually submit a real transaction." Same flag name, very different consequences.

## Key gotchas

- **Market discovery**: Polymarket pre-creates a full day of future 5-minute windows in advance, so sorting Gamma by "recently created" surfaces tomorrow's placeholder markets, not today's about-to-close one. `btc_5m_market_finder.py` computes the expected slug directly from wall-clock time instead. It also doesn't trust Gamma's `closed` flag for these fast-cycling markets, since it lags actual resolution.
- **Zero-drift probability model**: the Brownian-motion model in `oracle_lag_strategy.py` deliberately assumes zero drift — over a 5-minute horizon, real trend is swamped by noise, and the actual edge comes from seeing the price before the market does, not from predicting direction.
- **Quarter-Kelly by default**: sizing is deliberately conservative because the probability model is an approximation, not a known-true probability.
- **Pidfile-based single-instance protection**: `paper_trader.py`, `live_trader.py`, and `watchdog.py` each claim a pidfile on startup and exit if another instance is already running — this matters for `paper_trader.py` too, since two instances sharing one state file would corrupt it just as surely as two live bots sharing one wallet would double-trade it.
- **`live_trading/live_trader.py` does not redeem winning positions on-chain** — it tracks and reports P&L as an accounting estimate, but claiming resolved winnings is still a separate manual step.

See `CLAUDE.md` for the full architecture writeup, including the watchdog's crash-vs-intentional-stop detection and the bot control plane in `web_dashboard.py`.

## Testing

There's no unit test suite. The convention here is to verify by running scripts against the real Binance/Polymarket endpoints they talk to. For anything touching real funds, test with a disposable, zero-balance wallet (`createwallet/create_polygon_wallet.py`) to confirm the request/signing path reaches the real API and fails for the *expected* reason (insufficient balance, geoblock, etc.), not a bug.

`oracle_lag_strategy.py --selftest` is the exception — a synthetic, no-network sanity check of the probability/Kelly math.

## Linting

```bash
.venv/bin/ruff check .
.venv/bin/ruff format .
```

#!/usr/bin/env python3
"""
Paper trading bot for trend_strategy.py's TrendConfirmEngine (EMA cross +
Supertrend, trading only when both agree, optionally a third TradingView
rating vote via --tv-confirm) — a separate, independent bot from
paper_trading/paper_trader.py, built to A/B test whether this combination
adds anything over the oracle-lag strategy without touching that bot's
state, pidfile, or logs in any way. See trend_strategy.py's module docstring
for why this is worth doubting in the first place (a 7-day backtest found
no statistically significant edge for either indicator alone, or for
requiring both to agree) — this is the "test it live and see for yourself"
counterpart to that backtest. --tv-confirm is even less proven than the
two-indicator baseline (see trend_strategy.py's docstring) — it's here to
A/B against that baseline, not because it's known to help.

Does not reimplement the settlement/paper-portfolio machinery:
paper_trading/paper_trader.py's PaperTrader class, market_and_settlement_loop,
and stats_flush_loop are reused directly, unmodified, imported as a sibling
module. That works because trend_strategy.TrendConfirmEngine implements the
exact same on_price_tick(...)/set_market(...)/compute(...)/evaluate(...)
interface (and the same 5-tuple shape from compute()) that PaperTrader was
written against for oracle_lag_strategy.OracleLagEngine — see
trend_strategy.py's docstring.

Everything else about this bot is independent on purpose, the same way
paper_trader.py's v1/v2/v3 runs are independent of each other: its own
state file, trades log, perf_logs directory, and pidfile, all living in this
experiments/ directory. It can run at the same time as
paper_trading/paper_trader.py without any interference — that's the point,
so you can compare both strategies against the real market concurrently
instead of sequentially.

Monitor it with the existing tools, pointed at this bot's own files —
no new dashboard needed:
    python paper_trading/dashboard.py --state-file experiments/trend_paper_state.json
    python performance_tracker.py --report --log-dir experiments/perf_logs

Usage (run from anywhere — paths resolve relative to this script):
    python experiments/trend_paper_trader.py
    python experiments/trend_paper_trader.py --ema-fast 20 --ema-slow 50
    python experiments/trend_paper_trader.py --reset   # start over, ignore existing state
"""

import argparse
import asyncio
import signal as signal_module
import sys
from pathlib import Path

# This script lives in experiments/ but paper_trading/paper_trader.py,
# trend_strategy.py, performance_tracker.py, and pidfile.py all live one
# level up (or in paper_trading/, one level up then down) — put both on
# sys.path before importing them, regardless of cwd.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "paper_trading"))

import paper_trader as paper_bot  # reuse PaperTrader / market_and_settlement_loop / stats_flush_loop verbatim

import btc_price_feed as price_feed
import performance_tracker
import pidfile
import trend_strategy

DEFAULT_STATE_PATH = SCRIPT_DIR / "trend_paper_state.json"
DEFAULT_TRADES_LOG_PATH = SCRIPT_DIR / "trend_paper_trades.jsonl"
DEFAULT_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs"
DEFAULT_PID_PATH = SCRIPT_DIR / "trend_paper_trader.pid"
DEFAULT_AUTORESTART_MARKER = SCRIPT_DIR / "trend_paper_trader.autorestart.json"


async def run(args):
    engine = trend_strategy.TrendConfirmEngine(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        min_edge=args.min_edge,
        max_position_pct=args.max_position_pct,
        bar_seconds=args.bar_seconds,
        ema_fast=args.ema_fast,
        ema_slow=args.ema_slow,
        st_period=args.st_period,
        st_multiplier=args.st_multiplier,
        sensitivity=args.sensitivity,
        tv_confirm=args.tv_confirm,
        tv_symbol=args.tv_symbol,
        tv_exchange=args.tv_exchange,
        tv_screener=args.tv_screener,
        tv_interval=args.tv_interval,
        tv_max_age=args.tv_max_age,
    )
    tracker = None if args.no_perf_log else performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir)
    # engine is duck-typed here, not an OracleLagEngine — PaperTrader only
    # ever calls the on_price_tick/set_market/compute/evaluate interface
    # trend_strategy.TrendConfirmEngine implements identically. The
    # signals.csv "sigma_per_sqrt_sec" column (a fixed schema shared across
    # bots, see performance_tracker.py) holds this engine's agree_state
    # (+1.0/-1.0/0.0) instead — there's no volatility estimate to log here.
    trader = paper_bot.PaperTrader(
        args.bankroll,
        engine,
        Path(args.state_file),
        Path(args.trades_log),
        reset=args.reset,
        tracker=tracker,
        max_open_positions=args.max_open_positions,
    )

    def on_tick(tick):
        if tick.kind == "trade":
            trader.on_price_tick(tick.data["price"], tick.event_time_ms / 1000.0)
        elif tick.kind == "rest":
            trader.on_price_tick(tick.data["price"])

    price_task = asyncio.ensure_future(
        price_feed.stream_prices(
            "btcusdt", "trade", "1m", on_tick, rest_fallback=True, rest_fallback_after=2, rest_poll_interval=2.0
        )
    )
    market_task = asyncio.ensure_future(paper_bot.market_and_settlement_loop(trader, args.market_poll_interval))
    tv_task = asyncio.ensure_future(trend_strategy.tv_rating_poll_loop(engine, args.tv_poll_interval))
    stats_task = asyncio.ensure_future(paper_bot.stats_flush_loop(trader, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, tv_task, stats_task)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bankroll", type=float, default=1000.0)
    parser.add_argument("--kelly-multiplier", type=float, default=0.25)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--max-position-pct", type=float, default=0.05)
    parser.add_argument(
        "--max-open-positions",
        type=int,
        default=None,
        help="Cap on concurrently open positions (default: unlimited)",
    )
    parser.add_argument("--bar-seconds", type=float, default=60.0, help="Bar length in seconds (default: 60)")
    parser.add_argument("--ema-fast", type=int, default=9, help="Fast EMA period in bars (default: 9)")
    parser.add_argument("--ema-slow", type=int, default=21, help="Slow EMA period in bars (default: 21)")
    parser.add_argument("--st-period", type=int, default=10, help="Supertrend ATR period in bars (default: 10)")
    parser.add_argument("--st-multiplier", type=float, default=3.0, help="Supertrend ATR multiplier (default: 3.0)")
    parser.add_argument(
        "--sensitivity",
        type=float,
        default=0.5,
        help="How strongly agreement between both indicators tilts p_up away from 0.5, 0..1 (default: 0.5)",
    )
    parser.add_argument("--market-poll-interval", type=float, default=2.0)
    parser.add_argument(
        "--tv-confirm",
        action="store_true",
        help="Also require TradingView's own aggregate technical rating (via tradingview-ta) to "
        "agree with EMA cross + Supertrend before trading — off by default, unbacktested (see "
        "trend_strategy.py's module docstring)",
    )
    parser.add_argument("--tv-symbol", default="BTCUSDT", help="Symbol to query via tradingview-ta (default: BTCUSDT)")
    parser.add_argument(
        "--tv-exchange", default="BINANCE", help="Exchange to query via tradingview-ta (default: BINANCE)"
    )
    parser.add_argument("--tv-screener", default="crypto", help="tradingview-ta screener category (default: crypto)")
    parser.add_argument(
        "--tv-interval",
        default="5m",
        help="TradingView interval string for the rating, e.g. 1m/5m/15m/1h/4h/1d (default: 5m, matching the market window)",
    )
    parser.add_argument(
        "--tv-poll-interval",
        type=float,
        default=30.0,
        help="Seconds between TradingView rating refreshes — it's a network call, not per-tick (default: 30)",
    )
    parser.add_argument(
        "--tv-max-age",
        type=float,
        default=300.0,
        help="Treat the cached TradingView rating as unavailable if older than this many seconds (default: 300)",
    )
    parser.add_argument("--telegram-token", default=None)
    parser.add_argument("--telegram-chat-id", default=None)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument(
        "--reset", action="store_true", help="Ignore any existing --state-file and start over with a fresh bankroll"
    )
    parser.add_argument(
        "--perf-log-dir", default=str(DEFAULT_PERF_LOG_DIR), help="Directory for signals.csv/trades.csv/executions.csv"
    )
    parser.add_argument("--no-perf-log", action="store_true", help="Disable signals/trades/executions CSV logging")
    parser.add_argument(
        "--pid-file",
        default=str(DEFAULT_PID_PATH),
        help="Refuse to start if this pidfile names a still-running process",
    )
    parser.add_argument(
        "--autorestart-marker",
        default=str(DEFAULT_AUTORESTART_MARKER),
        help="Marker file a watchdog could use to tell a crash apart from an intentional stop "
        "(this bot isn't wired into watchdog.py's BOT_SCRIPTS by default; the marker is still "
        "written/cleared correctly if you add it there yourself later)",
    )
    args = parser.parse_args()

    pidfile.claim_or_exit(Path(args.pid_file))
    marker_path = Path(args.autorestart_marker)
    pidfile.write_autorestart_marker(marker_path, sys.argv[1:])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run(args))

    def shutdown(*_):
        task.cancel()

    for sig in (signal_module.SIGINT, signal_module.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pidfile.clear_autorestart_marker(marker_path)
    finally:
        loop.close()


if __name__ == "__main__":
    main()

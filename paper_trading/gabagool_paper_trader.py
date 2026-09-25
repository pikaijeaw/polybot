#!/usr/bin/env python3
"""
Paper trading loop for gabagool_strategy.py's GabagoolEngine — a YES/NO
order-book arbitrage strategy on Polymarket's BTC 15-minute Up/Down markets.
Lives alongside paper_trader.py (this directory) so both are managed by the
same web_dashboard.py control plane, but shares none of its logic or state:
own state file (gabagool_state.json), own trades log (gabagool_trades.jsonl),
own pidfile, own market finder (../btc_15m_market_finder.py, 15-minute
windows, not the 5-minute series paper_trader.py/trend_paper_trader.py
trade). See gabagool_strategy.py's module docstring for why this needed its
own engine rather than reusing oracle_lag_strategy.OracleLagEngine's
interface (the way trend_paper_trader.py does): arbitrage has no probability
model, no direction, no Kelly sizing, and no Binance price feed dependency
at all — it only ever looks at Polymarket's own order books. Consequently
this script does not reuse PaperTrader from paper_trader.py either — it has
its own bespoke GabagoolPaperTrader below, with a genuinely different
position/settlement shape (deterministic profit, not outcome-dependent).

PAPER TRADING ONLY. There is no --live flag and no order-signing path here
by design (see gabagool_strategy.py's "Leg risk" note) — real execution of a
two-leg arbitrage can't be made atomic with single-order submission, and a single-leg fill is directional exposure, which defeats the
entire point of this strategy. If live execution is wanted later, that's a
separate, deliberate piece of work — build and test the fill logic here
first.

Bucket sizing: each detected arbitrage opportunity deploys up to
--bucket-usd (default $50) total, split across both legs so the share counts
match (see gabagool_strategy.size_arbitrage) — never $50 per leg. $50 is
comfortably above the CLOB's $5 order minimum and keeps any one window's
capital small relative to a --bankroll-sized paper bankroll, which is the
point: this strategy's edge per trade is small (a few cents on a $50
bucket after fees, see gabagool_strategy.py's module docstring) and it
should be judged over many trades, not any single one. Most poll cycles will
find nothing tradeable — the book has to actually cross for there to be a
free-money opportunity — so an idle bot here is the expected steady state,
not a bug.

Settlement: the deterministic payout (shares * $1, since exactly one leg
always resolves to $1/share and the other to $0/share) doesn't actually
depend on which side wins, so unlike the directional bots in this repo this
one doesn't need Polymarket's resolution to know its P&L. It still polls
btc_15m_market_finder.resolve_market_outcome() after each window closes as a
paranoia check / for the trades log (see check_settlements), but falls back
to crediting the guaranteed payout after SETTLE_FALLBACK_AFTER even if that
lookup never resolves — waiting on it isn't actually load-bearing.

trades.jsonl is append-only, one line per CLOSED trade only (opened
positions live in gabagool_state.json's open_positions instead) — same
"blotter, not a lifecycle log" convention as the rest of this repo (see
../CLAUDE.md's Key Gotchas).

Monitor and control it via web_dashboard.py (this directory), which starts
it as a subprocess and reads gabagool_state.json/gabagool_trades.jsonl from
disk the same way it already does for paper_trader.py.

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/gabagool_paper_trader.py
    python paper_trading/gabagool_paper_trader.py --bucket-usd 50 --bankroll 1000
    python paper_trading/gabagool_paper_trader.py --reset   # start over, ignore existing state
"""

import argparse
import asyncio
import json
import signal as signal_module
import sys
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import btc_15m_market_finder as finder
import gabagool_strategy as strategy
import pidfile

DEFAULT_STATE_PATH = SCRIPT_DIR / "gabagool_state.json"
DEFAULT_TRADES_LOG_PATH = SCRIPT_DIR / "gabagool_trades.jsonl"
DEFAULT_PID_PATH = SCRIPT_DIR / "gabagool_paper_trader.pid"
DEFAULT_AUTORESTART_MARKER = SCRIPT_DIR / "gabagool_paper_trader.autorestart.json"

# How long to wait past a window's close before trusting/giving up on Gamma's
# own resolution — see module docstring's Settlement section. Not the same
# constant name/value as the rest of the repo's SETTLE_FALLBACK_AFTER (120s)
# on purpose: that one gates a *price-based guess* fallback for a directional
# bet; this one gates a lookup that's cosmetic (paranoia-check) rather than
# load-bearing, so it can wait a little longer without holding up real P&L.
SETTLE_MIN_WAIT_SECONDS = 15.0
SETTLE_FALLBACK_AFTER_SECONDS = 180.0


def _new_state(starting_bankroll: float) -> dict:
    return {
        "starting_bankroll": starting_bankroll,
        "bankroll": starting_bankroll,
        "open_positions": {},
        "recent_closed": [],
        "stats": {
            "opportunities_checked": 0,
            "trades_opened": 0,
            "trades_closed": 0,
            "total_locked_profit": 0.0,
            "total_fees_paid": 0.0,
        },
        "updated_at": time.time(),
    }


class GabagoolPaperTrader:
    def __init__(
        self,
        engine: strategy.GabagoolEngine,
        state_path: Path,
        trades_log_path: Path,
        starting_bankroll: float,
        reset: bool = False,
    ):
        self.engine = engine
        self.state_path = state_path
        self.trades_log_path = trades_log_path

        if reset or not state_path.exists():
            self.state = _new_state(starting_bankroll)
        else:
            try:
                self.state = json.loads(state_path.read_text())
            except (OSError, json.JSONDecodeError):
                self.state = _new_state(starting_bankroll)

    def maybe_open_position(self, market, session: requests.Session) -> None:
        if market.slug in self.state["open_positions"]:
            return  # only one arb position per window
        if not market.accepting_orders:
            return

        try:
            up_book = finder.fetch_order_book(session, market.token_up)
            down_book = finder.fetch_order_book(session, market.token_down)
        except requests.RequestException as e:
            print(f"book fetch failed for {market.slug}: {e}", file=sys.stderr)
            return

        self.state["stats"]["opportunities_checked"] += 1
        opp = self.engine.evaluate(market.slug, market.epoch, up_book, down_book)
        if opp is None:
            return

        if opp.total_cost > self.state["bankroll"]:
            print(
                f"skipping {market.slug}: opportunity costs ${opp.total_cost:.2f}, "
                f"only ${self.state['bankroll']:.2f} available",
                file=sys.stderr,
            )
            return

        self.state["bankroll"] -= opp.total_cost
        self.state["open_positions"][market.slug] = {
            "market_slug": opp.market_slug,
            "end_epoch": opp.end_epoch,
            "shares": opp.shares,
            "avg_price_up": opp.avg_price_up,
            "avg_price_down": opp.avg_price_down,
            "cost_up": opp.cost_up,
            "cost_down": opp.cost_down,
            "fee_up": opp.fee_up,
            "fee_down": opp.fee_down,
            "total_cost": opp.total_cost,
            "net_profit": opp.net_profit,
            "net_margin_per_share": opp.net_margin_per_share,
            "opened_at": opp.ts,
        }
        self.state["stats"]["trades_opened"] += 1
        print(strategy.format_opportunity(opp) + "  -> OPENED")

    def check_settlements(self, session: requests.Session) -> None:
        now = time.time()
        for slug in list(self.state["open_positions"].keys()):
            pos = self.state["open_positions"][slug]
            if now < pos["end_epoch"] + SETTLE_MIN_WAIT_SECONDS:
                continue

            resolution = None
            source = "unresolved-timeout"
            try:
                resolution = finder.resolve_market_outcome(session, slug)
            except requests.RequestException as e:
                print(f"resolution lookup failed for {slug}: {e}", file=sys.stderr)
            if resolution is not None:
                source = "gamma"
            elif now < pos["end_epoch"] + SETTLE_FALLBACK_AFTER_SECONDS:
                continue  # give Gamma a bit longer before crediting on trust

            # Guaranteed regardless of which side actually won — that's the
            # whole point of matching share counts on both legs. See module
            # docstring's Settlement section.
            payout = pos["shares"] * 1.0
            self.state["bankroll"] += payout

            closed = {
                **pos,
                "closed_at": now,
                "resolution": resolution or "UNKNOWN",
                "resolution_source": source,
                "payout": payout,
            }
            with open(self.trades_log_path, "a") as f:
                f.write(json.dumps(closed) + "\n")

            self.state["stats"]["trades_closed"] += 1
            self.state["stats"]["total_locked_profit"] += closed["net_profit"]
            self.state["stats"]["total_fees_paid"] += closed["fee_up"] + closed["fee_down"]
            self.state["recent_closed"].insert(0, closed)
            self.state["recent_closed"] = self.state["recent_closed"][:25]
            del self.state["open_positions"][slug]
            print(f"CLOSED {slug} resolution={closed['resolution']} ({source}) net_profit=${closed['net_profit']:.3f}")

    def save_state(self) -> None:
        open_cost = sum(p["total_cost"] for p in self.state["open_positions"].values())
        self.state["equity"] = self.state["bankroll"] + open_cost
        self.state["updated_at"] = time.time()
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2))
        tmp.replace(self.state_path)


async def market_and_settlement_loop(trader: GabagoolPaperTrader, poll_interval: float):
    session = requests.Session()
    session.headers.update({"User-Agent": "gabagool-paper-trader/1.0"})
    while True:
        try:
            markets = await asyncio.to_thread(finder.fetch_btc_15m_markets, session)
            for m in markets:
                if m.status in ("LIVE", "UPCOMING"):
                    trader.maybe_open_position(m, session)
            trader.check_settlements(session)
            trader.save_state()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market/settlement loop error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(trader: GabagoolPaperTrader, telegram_token, telegram_chat_id, interval: float = 3600.0):
    """Posts a plain hourly bankroll/trade-count summary to Telegram, if
    configured — much simpler than the other bots' HourlyStatsTracker since
    there's no probability/edge distribution to summarize, just counts and a
    running total."""
    import notify

    while True:
        await asyncio.sleep(interval)
        s = trader.state["stats"]
        summary = (
            f"Gabagool (BTC 15m arb) hourly summary\n"
            f"Bankroll: ${trader.state['bankroll']:.2f} (started ${trader.state['starting_bankroll']:.2f})\n"
            f"Opportunities checked: {s['opportunities_checked']}\n"
            f"Trades opened/closed: {s['trades_opened']}/{s['trades_closed']}\n"
            f"Total locked profit: ${s['total_locked_profit']:.3f}\n"
            f"Total fees paid: ${s['total_fees_paid']:.3f}\n"
            f"Open positions: {len(trader.state['open_positions'])}"
        )
        notify.send_telegram_message(summary, telegram_token, telegram_chat_id)


async def run(args):
    engine = strategy.GabagoolEngine(
        bucket_usd=args.bucket_usd,
        fee_rate=args.fee_rate,
        min_profit_margin=args.min_profit_margin,
        min_seconds_remaining=args.min_seconds_remaining,
    )
    trader = GabagoolPaperTrader(
        engine, Path(args.state_file), Path(args.trades_log), starting_bankroll=args.bankroll, reset=args.reset
    )
    market_task = asyncio.ensure_future(market_and_settlement_loop(trader, args.market_poll_interval))
    stats_task = asyncio.ensure_future(
        stats_flush_loop(trader, args.telegram_token, args.telegram_chat_id, args.telegram_interval)
    )
    await asyncio.gather(market_task, stats_task)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Starting paper bankroll in USD (default: 1000)")
    parser.add_argument(
        "--bucket-usd",
        type=float,
        default=50.0,
        help="Max total $ deployed per arbitrage opportunity, split across both legs (default: 50)",
    )
    parser.add_argument(
        "--fee-rate",
        type=float,
        default=strategy.FEE_RATE_CRYPTO,
        help=f"Taker fee rate used in the fee-aware profit check (default: {strategy.FEE_RATE_CRYPTO}, "
        "Polymarket's crypto-category rate — see gabagool_strategy.py docstring)",
    )
    parser.add_argument(
        "--min-profit-margin",
        type=float,
        default=0.005,
        help="Minimum fee-aware net profit required per share, in dollars, before taking a trade (default: 0.005)",
    )
    parser.add_argument(
        "--min-seconds-remaining",
        type=float,
        default=30.0,
        help="Don't open a new position inside this many seconds of a window's close (default: 30)",
    )
    parser.add_argument("--market-poll-interval", type=float, default=3.0)
    parser.add_argument("--telegram-token", default=None)
    parser.add_argument("--telegram-chat-id", default=None)
    parser.add_argument("--telegram-interval", type=float, default=3600.0)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument(
        "--reset", action="store_true", help="Ignore any existing --state-file and start over with a fresh bankroll"
    )
    parser.add_argument(
        "--pid-file",
        default=str(DEFAULT_PID_PATH),
        help="Refuse to start if this pidfile names a still-running process",
    )
    parser.add_argument("--autorestart-marker", default=str(DEFAULT_AUTORESTART_MARKER))
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

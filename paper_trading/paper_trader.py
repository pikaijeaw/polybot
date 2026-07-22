#!/usr/bin/env python3
"""
Paper trading bot — no dashboard in this process, by design.

Runs the exact same live pipeline as oracle_lag_strategy.py --live (real
Binance prices, real Polymarket btc-updown-5m quotes, real Brownian
probability + Kelly sizing) but instead of routing signals to
order_executor.py, it simulates fills against a virtual bankroll — no wallet,
no CLOB auth, no funds at risk. It's the thing to run for days before you
ever point order_executor.py at real money.

This process only trades — it doesn't serve a web UI. Run
web_dashboard.py separately (a different process) to monitor it; that
script reads the state file this one writes. That isolation is
deliberate: restarting the dashboard (to change ports, pick up a template
edit, whatever) never interrupts a live trading session, and a dashboard
crash/hang can't affect the bot either. See web_dashboard.py's docstring.

Each qualifying signal opens at most one paper position per market window
(so repeated signals for the same 5-minute window don't pyramid). When a
window's close time passes, the position is settled by polling Polymarket's
own Gamma API for the resolved outcome (outcomePrices) — the authoritative
source, since it reflects the actual Chainlink-based resolution rather than
our own approximation. If Gamma hasn't resolved it within
SETTLE_FALLBACK_AFTER (120s; resolution is sometimes not instant), we fall
back to comparing the last known price to the window's anchor, clearly
flagged as a fallback in the log.

State is written to --state-file (a JSON snapshot, overwritten each update —
read by web_dashboard.py / dashboard.py) and --trades-log (a JSON-lines
append-only audit trail of every open/close event). Both persist across
restarts: relaunching this script resumes from --state-file if it exists,
rather than starting over. Both default to living alongside this script in
paper_trading/.

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/paper_trader.py
    python paper_trading/paper_trader.py --bankroll 5000 --kelly-multiplier 0.25
    python paper_trading/paper_trader.py --reset                # start over, ignore existing state
"""

import argparse
import asyncio
import json
import os
import signal as signal_module
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests

# This script lives in paper_trading/ but btc_5m_market_finder.py,
# btc_price_feed.py, and oracle_lag_strategy.py live one level up — put the
# project root on sys.path before importing them, regardless of cwd.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import btc_5m_market_finder as finder
import btc_price_feed as price_feed
import oracle_lag_strategy as strategy
import performance_tracker  # signals.csv / trades.csv / executions.csv
import pidfile

DEFAULT_STATE_PATH = SCRIPT_DIR / "paper_state.json"
DEFAULT_TRADES_LOG_PATH = SCRIPT_DIR / "paper_trades.jsonl"
DEFAULT_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs"
DEFAULT_PID_PATH = SCRIPT_DIR / "paper_trader.pid"
DEFAULT_AUTORESTART_MARKER = SCRIPT_DIR / "paper_trader.autorestart.json"
SETTLE_FALLBACK_AFTER = 120.0  # seconds past window close before trusting our own price-based guess


# ---------------------------------------------------------------------------
# Portfolio model
# ---------------------------------------------------------------------------


@dataclass
class PaperPosition:
    market_slug: str
    side: str
    entry_price: float
    shares: float
    stake_usd: float
    opened_at: float
    end_epoch: int
    anchor_price: float
    edge_at_entry: float
    signal_probability: float = 0.0  # engine's P(side) at signal time — for trades.csv
    kelly_fraction: float = 0.0  # applied Kelly fraction at signal time — for trades.csv
    bankroll_before: float = 0.0  # paper cash immediately before this stake was deducted
    status: str = "OPEN"  # OPEN | WON | LOST
    settled_at: float | None = None
    pnl_usd: float | None = None
    resolution_source: str | None = None  # "gamma" | "price-fallback"


class PaperTrader:
    def __init__(
        self,
        bankroll: float,
        engine: strategy.OracleLagEngine,
        state_path: Path,
        trades_log_path: Path,
        reset: bool = False,
        tracker: performance_tracker.PerformanceTracker | None = None,
        max_open_positions: int | None = None,
    ):
        self.starting_bankroll = bankroll
        self.cash = bankroll
        self.engine = engine
        self.state_path = state_path
        self.trades_log_path = trades_log_path
        self.tracker = tracker
        self.max_open_positions = max_open_positions
        self.open_positions: dict = {}
        self.closed_positions: list = []
        self.traded_slugs: set = set()
        self._last_engine_snapshot: dict | None = None

        if not reset and state_path.exists():
            self._load_state()

    # -- persistence ---------------------------------------------------

    def _load_state(self):
        try:
            data = json.loads(self.state_path.read_text())
        except (json.JSONDecodeError, OSError):
            return
        self.starting_bankroll = data.get("starting_bankroll", self.starting_bankroll)
        self.cash = data.get("cash", self.cash)
        self.open_positions = {p["market_slug"]: PaperPosition(**p) for p in data.get("open_positions", [])}
        self.traded_slugs = set(self.open_positions)
        for p in data.get("recent_closed", []):
            self.closed_positions.append(PaperPosition(**p))
            self.traded_slugs.add(p["market_slug"])
        stats = data.get("stats", {})
        # closed_positions here only holds what was in "recent_closed" (capped);
        # full history lives in the trades log. Stats we can't fully
        # reconstruct from a truncated list are re-derived from that cap,
        # which is fine — win_rate/pnl below recompute from what we have.
        print(
            f"resumed paper state: cash=${self.cash:.2f}, {len(self.open_positions)} open, "
            f"{stats.get('trades', len(self.closed_positions))} historical trades",
            file=sys.stderr,
        )

    def _append_trade_log(self, event: dict):
        with open(self.trades_log_path, "a") as f:
            f.write(json.dumps(event) + "\n")

    def _write_state(self):
        snapshot = {
            "updated_at": time.time(),
            "starting_bankroll": self.starting_bankroll,
            "cash": self.cash,
            "equity": self.equity(),
            "engine": self._last_engine_snapshot,
            "open_positions": [asdict(p) for p in self.open_positions.values()],
            "stats": self.stats(),
            "recent_closed": [asdict(p) for p in self.closed_positions[-25:]],
        }
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2))
        os.replace(tmp, self.state_path)

    # -- portfolio math --------------------------------------------------

    def equity(self) -> float:
        # open positions marked at cost (stake) until settled — simplest
        # honest accounting without needing a live mark-to-model price.
        return self.cash + sum(p.stake_usd for p in self.open_positions.values())

    def stats(self) -> dict:
        n = len(self.closed_positions)
        wins = sum(1 for p in self.closed_positions if p.status == "WON")
        realized_pnl = sum(p.pnl_usd or 0.0 for p in self.closed_positions)
        total_staked = sum(p.stake_usd for p in self.closed_positions)
        return {
            "trades": n,
            "wins": wins,
            "losses": n - wins,
            "win_rate": wins / n if n else None,
            "realized_pnl": realized_pnl,
            "total_staked": total_staked,
            "return_pct": (self.equity() - self.starting_bankroll) / self.starting_bankroll
            if self.starting_bankroll
            else None,
        }

    # -- trading ----------------------------------------------------------

    def on_price_tick(self, price: float, ts: float | None = None):
        self.engine.on_price_tick(price, ts)

    def process_market_update(self, slug: str, end_epoch: int, up_ask: float, down_ask: float) -> PaperPosition | None:
        """Single entry point per poll cycle for a live/upcoming market:
        registers the window, calls engine.compute() exactly once, logs that
        evaluation to signals.csv (traded or not — this is what makes "what
        are we missing" answerable later), opens a paper position if it
        qualifies, and refreshes the dashboard snapshot from the same
        compute() result. Nothing here calls compute() a second time."""
        window = self.engine.set_market(slug, end_epoch)
        computed = self.engine.compute(up_ask, down_ask)

        pos, skip_reason = self._maybe_open(slug, end_epoch, up_ask, down_ask, computed)

        if self.tracker is not None and computed is not None:
            p_up, seconds_remaining, sigma, up_r, down_r = computed
            best_side, best_result = ("UP", up_r) if up_r.edge >= down_r.edge else ("DOWN", down_r)
            self.tracker.log_signal(
                market_slug=slug,
                seconds_remaining=seconds_remaining,
                last_price=self.engine.last_price,
                anchor_price=window.anchor_price,
                anchor_is_approximate=window.anchor_is_approximate,
                sigma_per_sqrt_sec=sigma,
                p_up=p_up,
                up_ask=up_ask,
                down_ask=down_ask,
                up_edge=up_r.edge,
                down_edge=down_r.edge,
                best_side=best_side,
                best_edge=best_result.edge,
                kelly_fraction_applied=best_result.applied_kelly,
                stake_usd=best_result.stake_usd,
                min_edge_threshold=self.engine.sizer.min_edge,
                traded=pos is not None,
                skip_reason=skip_reason,
            )

        self._update_engine_snapshot(computed, up_ask, down_ask)
        return pos

    def clear_engine_snapshot(self):
        """Called when no tradable market is found this cycle, so the
        dashboard doesn't keep showing a stale window."""
        self._last_engine_snapshot = None
        self._write_state()

    def _maybe_open(self, slug: str, end_epoch: int, up_ask: float, down_ask: float, computed) -> tuple:
        """Returns (position_or_None, skip_reason). skip_reason is "" when a
        position was opened."""
        if slug in self.traded_slugs:
            return None, "already_traded_this_window"
        if computed is None:
            return None, "no_data"

        sig = self.engine.evaluate(up_ask, down_ask, _precomputed=computed)
        if sig is None:
            return None, "below_min_edge"
        if self.max_open_positions is not None and len(self.open_positions) >= self.max_open_positions:
            print(
                f"skip {slug} {sig.side}: {len(self.open_positions)} position(s) already open "
                f"(--max-open-positions {self.max_open_positions})",
                file=sys.stderr,
            )
            self.traded_slugs.add(slug)
            return None, "max_open_positions_reached"
        if sig.stake_usd > self.cash:
            print(
                f"skip {slug} {sig.side}: stake ${sig.stake_usd:.2f} exceeds paper cash ${self.cash:.2f}",
                file=sys.stderr,
            )
            self.traded_slugs.add(slug)
            return None, "insufficient_cash"

        window = self.engine.market
        bankroll_before = self.cash
        self.cash -= sig.stake_usd
        pos = PaperPosition(
            market_slug=slug,
            side=sig.side,
            entry_price=sig.market_price,
            shares=sig.shares,
            stake_usd=sig.stake_usd,
            opened_at=sig.ts,
            end_epoch=end_epoch,
            anchor_price=window.anchor_price,
            edge_at_entry=sig.edge,
            signal_probability=sig.probability,
            kelly_fraction=sig.kelly_fraction,
            bankroll_before=bankroll_before,
        )
        self.open_positions[slug] = pos
        self.traded_slugs.add(slug)
        self._append_trade_log({"event": "OPEN", **asdict(pos)})
        self._write_state()
        return pos, ""

    def try_settle(self, session: requests.Session) -> list:
        settled = []
        now = time.time()
        for slug, pos in list(self.open_positions.items()):
            if now < pos.end_epoch:
                continue
            try:
                if self.tracker is not None:
                    with self.tracker.time_execution(api="gamma_resolve", endpoint="/markets", context=slug) as ex:
                        outcome = finder.resolve_market_outcome(session, slug)
                        ex.http_status = 200
                else:
                    outcome = finder.resolve_market_outcome(session, slug)
                source = "gamma"
            except requests.RequestException as e:
                print(f"settlement lookup failed for {slug}: {e}", file=sys.stderr)
                outcome, source = None, None

            if outcome is None:
                if now - pos.end_epoch < SETTLE_FALLBACK_AFTER:
                    continue
                last_price = self.engine.last_price
                if last_price is None:
                    continue
                outcome = "UP" if last_price >= pos.anchor_price else "DOWN"
                source = "price-fallback"

            won = outcome == pos.side
            payout = pos.shares if won else 0.0
            pos.status = "WON" if won else "LOST"
            pos.settled_at = now
            pos.pnl_usd = payout - pos.stake_usd
            pos.resolution_source = source
            self.cash += payout

            del self.open_positions[slug]
            self.closed_positions.append(pos)
            settled.append(pos)
            self._append_trade_log({"event": "CLOSE", **asdict(pos)})
            self._write_state()

            if self.tracker is not None:
                self.tracker.log_trade(
                    market_slug=pos.market_slug,
                    side=pos.side,
                    signal_ts=pos.opened_at,
                    signal_probability=pos.signal_probability,
                    signal_edge=pos.edge_at_entry,
                    kelly_fraction=pos.kelly_fraction,
                    requested_price=pos.entry_price,
                    filled_price=pos.entry_price,  # paper fills assume no slippage
                    stake_usd=pos.stake_usd,
                    shares=pos.shares,
                    bankroll_before=pos.bankroll_before,
                    bankroll_after=self.cash,
                    entry_ts=pos.opened_at,
                    resolve_ts=pos.settled_at,
                    resolve_outcome=outcome,
                    resolve_source=source,
                    status=pos.status,
                    pnl_usd=pos.pnl_usd,
                )
        return settled

    def _update_engine_snapshot(self, computed, up_ask: float, down_ask: float):
        window = self.engine.market
        snap = None
        if window is not None and computed is not None:
            p_up, seconds_remaining, sigma, up_r, down_r = computed
            snap = {
                "market_slug": window.slug,
                "last_price": self.engine.last_price,
                "anchor_price": window.anchor_price,
                "anchor_is_approximate": window.anchor_is_approximate,
                "seconds_remaining": seconds_remaining,
                "p_up": p_up,
                "up_ask": up_ask,
                "up_edge": up_r.edge,
                "down_ask": down_ask,
                "down_edge": down_r.edge,
                "sigma_per_sqrt_sec": sigma,
            }
        self._last_engine_snapshot = snap
        self._write_state()


# ---------------------------------------------------------------------------
# Live wiring
# ---------------------------------------------------------------------------


async def market_and_settlement_loop(trader: PaperTrader, poll_interval: float):
    session = requests.Session()
    session.headers.update({"User-Agent": "oracle-lag-paper-trader/1.0"})
    while True:
        try:
            settled = trader.try_settle(session)
            for pos in settled:
                print(
                    f"[{time.strftime('%H:%M:%S')}] SETTLED {pos.market_slug} {pos.side} "
                    f"-> {pos.status} pnl=${pos.pnl_usd:+.2f} (source={pos.resolution_source})"
                )

            if trader.tracker is not None:
                with trader.tracker.time_execution(
                    api="gamma_markets", endpoint="/markets", context="btc-updown-5m discovery"
                ) as ex:
                    markets = await asyncio.to_thread(finder.fetch_btc_5m_markets, session)
                    ex.http_status = 200
            else:
                markets = await asyncio.to_thread(finder.fetch_btc_5m_markets, session)

            live_market = next((m for m in markets if m.status == "LIVE"), None) or next(
                (m for m in markets if m.status == "UPCOMING"), None
            )

            if live_market is not None and live_market.best_bid is not None and live_market.best_ask is not None:
                up_ask = live_market.best_ask
                down_ask = 1.0 - live_market.best_bid
                pos = trader.process_market_update(live_market.slug, live_market.epoch, up_ask, down_ask)
                if pos is not None:
                    print(
                        f"[{time.strftime('%H:%M:%S')}] OPENED {pos.market_slug} {pos.side} "
                        f"price={pos.entry_price:.3f} stake=${pos.stake_usd:.2f} edge={pos.edge_at_entry:+.1%}"
                    )
            else:
                trader.clear_engine_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market/settlement loop error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(trader: PaperTrader, telegram_token, telegram_chat_id, check_interval: float = 30.0):
    while True:
        try:
            summary = trader.engine.stats.maybe_flush()
            if summary is not None:
                s = trader.stats()
                portfolio_line = (
                    f"\nPaper equity: ${trader.equity():,.2f} ({(s['return_pct'] or 0) * 100:+.2f}%)  "
                    f"trades={s['trades']} win_rate={(s['win_rate'] or 0) * 100:.1f}%  "
                    f"realized_pnl=${s['realized_pnl']:+.2f}"
                )
                strategy.send_telegram_message(summary + portfolio_line, telegram_token, telegram_chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # A failed hourly summary is never worth taking the trading loop
            # down over — log it and keep going, same as market_and_settlement_loop.
            print(f"stats flush loop error: {e}", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run(args):
    engine = strategy.OracleLagEngine(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        min_edge=args.min_edge,
        max_position_pct=args.max_position_pct,
        vol_window_seconds=args.vol_window,
        fallback_sigma_annual=args.fallback_sigma_annual,
    )
    tracker = None if args.no_perf_log else performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir)
    trader = PaperTrader(
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
            "btcusdt",
            "trade",
            "1m",
            on_tick,
            rest_fallback=True,
            rest_fallback_after=2,
            rest_poll_interval=2.0,
        )
    )
    market_task = asyncio.ensure_future(market_and_settlement_loop(trader, args.market_poll_interval))
    stats_task = asyncio.ensure_future(stats_flush_loop(trader, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, stats_task)


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
        help="Cap on concurrently open positions (default: unlimited). Positions from consecutive windows can "
        "overlap if settlement is still pending when the next window's signal qualifies — this bounds that.",
    )
    parser.add_argument("--vol-window", type=float, default=180.0)
    parser.add_argument("--fallback-sigma-annual", type=float, default=0.6)
    parser.add_argument("--market-poll-interval", type=float, default=2.0)
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
        help="Marker file watchdog.py uses to tell a crash apart from an intentional stop",
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
        # A deliberate stop (SIGINT/SIGTERM, e.g. the dashboard's Stop
        # button or Ctrl+C) — clear the marker so watchdog.py knows not to
        # restart this. Any OTHER exception here is a real crash and
        # deliberately left uncleared, so the marker stays for watchdog.py
        # to notice and act on.
        pidfile.clear_autorestart_marker(marker_path)
    finally:
        loop.close()


if __name__ == "__main__":
    main()

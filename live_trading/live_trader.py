#!/usr/bin/env python3
"""
Live trading bot — runs the same oracle-lag pipeline as paper_trader.py but
places REAL orders on the Polymarket CLOB with REAL funds, fully automated
(no per-trade confirmation prompt). Only run this once you trust the
strategy from days of paper_trader.py results.

Safety model (deliberately different from order_executor.py's dry-run /
--live / --yes / typed-"yes" pattern — this bot is meant to run unattended,
so a per-trade prompt would just hang forever on a closed stdin):

  1. preflight_check.py's checks MUST all pass before this process will even
     start trading (wallet funded, CLOB auth working, USDC allowance
     approved). Not skippable via any flag. See run_preflight_gate().
  2. --max-stake-usd is a hard per-trade cap in USD, enforced via
     order_executor.check_market_order (the exact same risk check
     order_executor.py itself uses) — never overridden by Kelly sizing.
  3. --max-trades-per-hour / --max-trades-per-day is a circuit breaker
     (RateLimiter) independent of signal quality.
  4. --max-daily-loss-usd is an OPTIONAL cumulative stop-loss (disabled by
     default). The three caps above bound risk per trade and per hour but
     not total daily drawdown — a string of losses within the rate limit
     could still exceed what you meant to risk in a day. Worth setting
     explicitly (e.g. a fraction of --bankroll) if you care about that.

Order submission reuses order_executor's create_market_order/post_order
call directly (the exact signing path already exercised by order_executor.py
itself) rather than reimplementing it — see submit_live_market_order().

Scope limits, read before trusting the numbers this bot reports:
  - Positions are tracked and reported (OPEN/WON/LOST, estimated P&L) by
    polling Gamma's resolution the same way paper_trader.py does. This bot
    does NOT redeem/claim winning conditional-token positions on-chain —
    that still requires the normal Polymarket redemption flow (via
    polymarket.com or a separate script). pnl_usd here is an accounting
    estimate assuming the order filled at the observed ask with no
    slippage — actual fills on a FOK market order can differ.

State is written to --state-file/--trades-log, mirroring paper_trader.py's
shape (with order_id/order_result added), so the same dashboard/report
tooling can read either. Both live in live_trading/ by default, kept
completely separate from paper_trading/'s files.

Usage:
    python live_trading/live_trader.py --max-stake-usd 2 --max-trades-per-hour 4
    python live_trading/live_trader.py --dry-run   # logs intended orders, never calls create_order/post_order
"""

import argparse
import asyncio
import json
import os
import signal as signal_module
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, MarketOrderArgs, OrderType

# This script lives in live_trading/ but the root-level modules it depends on
# do not — put the project root on sys.path before importing them, regardless
# of cwd. Same pattern as paper_trading/paper_trader.py.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

import btc_5m_market_finder as finder
import btc_price_feed as price_feed
import oracle_lag_strategy as strategy
import order_executor
import performance_tracker  # signals.csv / trades.csv / executions.csv
import pidfile
import preflight_check as preflight

DEFAULT_STATE_PATH = SCRIPT_DIR / "live_state.json"
DEFAULT_TRADES_LOG_PATH = SCRIPT_DIR / "live_trades.jsonl"
DEFAULT_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs"
DEFAULT_PID_PATH = SCRIPT_DIR / "live_trader.pid"
DEFAULT_AUTORESTART_MARKER = SCRIPT_DIR / "live_trader.autorestart.json"
SETTLE_FALLBACK_AFTER = 120.0  # seconds past window close before trusting our own price-based guess


# ---------------------------------------------------------------------------
# Circuit breakers
# ---------------------------------------------------------------------------


class RateLimiter:
    """Sliding-window trade-count limiter. Refuses a NEW trade once too many
    have fired in the last hour or day, regardless of signal quality — an
    independent backstop on top of the min-edge/Kelly filtering."""

    def __init__(self, max_per_hour: int, max_per_day: int):
        self.max_per_hour = max_per_hour
        self.max_per_day = max_per_day
        self._timestamps: deque = deque()

    def _evict(self, now: float) -> None:
        cutoff = now - 86400
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def can_trade(self, now: float | None = None) -> tuple:
        now = now if now is not None else time.time()
        self._evict(now)
        last_hour = sum(1 for t in self._timestamps if t >= now - 3600)
        if last_hour >= self.max_per_hour:
            return False, f"max_trades_per_hour ({self.max_per_hour}) reached"
        if len(self._timestamps) >= self.max_per_day:
            return False, f"max_trades_per_day ({self.max_per_day}) reached"
        return True, ""

    def record(self, ts: float | None = None) -> None:
        self._timestamps.append(ts if ts is not None else time.time())


class DailyLossGuard:
    """Optional cumulative stop-loss: halts new trades once today's
    (estimated) realized P&L drops below -max_daily_loss_usd. Disabled by
    default (max_daily_loss_usd=None) since it wasn't one of the originally
    requested caps — it's here because per-trade + per-hour caps alone don't
    bound total daily drawdown."""

    def __init__(self, max_daily_loss_usd: float | None):
        self.max_daily_loss_usd = max_daily_loss_usd
        self._day = self._day_start()
        self._realized_pnl_today = 0.0

    @staticmethod
    def _day_start(ts: float | None = None) -> int:
        ts = ts if ts is not None else time.time()
        return int(ts // 86400) * 86400

    def _roll(self, now: float) -> None:
        day = self._day_start(now)
        if day != self._day:
            self._day = day
            self._realized_pnl_today = 0.0

    def record_pnl(self, pnl_usd: float, now: float | None = None) -> None:
        now = now if now is not None else time.time()
        self._roll(now)
        self._realized_pnl_today += pnl_usd

    def tripped(self, now: float | None = None) -> bool:
        if self.max_daily_loss_usd is None:
            return False
        now = now if now is not None else time.time()
        self._roll(now)
        return self._realized_pnl_today <= -abs(self.max_daily_loss_usd)


# ---------------------------------------------------------------------------
# Preflight gate — reuses preflight_check.py's own check functions
# ---------------------------------------------------------------------------


def run_preflight_gate(client, address: str, rpc_url: str, chain_id: int) -> bool:
    """Runs the exact same checks as `python preflight_check.py` (calling its
    functions directly, not reimplementing them) and returns True only if
    every section passed. This is the mandatory gate — there is no flag to
    bypass it."""
    from py_clob_client.config import get_contract_config
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        print(f"preflight gate: could not connect to RPC {rpc_url}", file=sys.stderr)
        return False

    collateral_address = get_contract_config(chain_id).collateral
    results = preflight.run_all_checks(address, w3, collateral_address, client)
    return preflight.print_report(results, address)


# ---------------------------------------------------------------------------
# Order submission — reuses order_executor's signing/post path directly
# ---------------------------------------------------------------------------


def submit_live_market_order(
    client, limits: order_executor.RiskLimits, token_id: str, side: str, amount_usd: float, dry_run: bool
):
    """Places a real FOK market BUY order sized at amount_usd. Deliberately
    calls client.create_market_order/post_order directly instead of going
    through order_executor.place_market_order — that function's
    confirm_or_abort() calls input(), which would hang or raise EOFError with
    no stdin attached (this bot runs unattended by design; caps substitute
    for the confirmation prompt, see check_market_order below)."""
    reference_price = None
    try:
        reference_price = float(client.get_price(token_id, side)["price"])
    except Exception:
        pass
    order_executor.check_market_order(limits, side, amount_usd, reference_price)  # raises RiskCheckFailed over cap

    if dry_run:
        print(f"[DRY RUN] would place MARKET {side} token_id={token_id} amount=${amount_usd:.2f}", file=sys.stderr)
        return None

    order_args = MarketOrderArgs(token_id=token_id, amount=amount_usd, side=side, order_type=OrderType.FOK)
    signed_order = client.create_market_order(order_args)
    return client.post_order(signed_order, orderType=OrderType.FOK)


# ---------------------------------------------------------------------------
# Portfolio model
# ---------------------------------------------------------------------------


@dataclass
class LivePosition:
    market_slug: str
    side: str
    entry_price: float  # ask observed pre-trade
    shares: float  # stake / entry_price — estimate, assumes no slippage
    stake_usd: float  # requested stake for this order, post-cap
    opened_at: float
    end_epoch: int
    anchor_price: float
    edge_at_entry: float
    signal_probability: float = 0.0
    kelly_fraction: float = 0.0
    order_id: str | None = None
    order_result: dict | None = None
    status: str = "OPEN"  # OPEN | WON | LOST
    settled_at: float | None = None
    pnl_usd: float | None = None  # estimated — see module docstring's scope note
    resolution_source: str | None = None  # "gamma" | "price-fallback"


class LiveTrader:
    def __init__(
        self,
        client,
        limits: order_executor.RiskLimits,
        engine: strategy.OracleLagEngine,
        state_path: Path,
        trades_log_path: Path,
        rate_limiter: RateLimiter,
        loss_guard: DailyLossGuard,
        dry_run: bool = False,
        reset: bool = False,
        tracker: performance_tracker.PerformanceTracker | None = None,
        max_open_positions: int | None = 1,
    ):
        self.client = client
        self.limits = limits
        self.engine = engine
        self.state_path = state_path
        self.trades_log_path = trades_log_path
        self.rate_limiter = rate_limiter
        self.loss_guard = loss_guard
        self.dry_run = dry_run
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
        self.open_positions = {p["market_slug"]: LivePosition(**p) for p in data.get("open_positions", [])}
        self.traded_slugs = set(self.open_positions)
        for p in data.get("recent_closed", []):
            self.closed_positions.append(LivePosition(**p))
            self.traded_slugs.add(p["market_slug"])
        print(
            f"resumed live state: {len(self.open_positions)} open, "
            f"{len(self.closed_positions)} historical (capped) trades",
            file=sys.stderr,
        )

    def _append_trade_log(self, event: dict):
        with open(self.trades_log_path, "a") as f:
            f.write(json.dumps(event) + "\n")

    def _write_state(self):
        snapshot = {
            "updated_at": time.time(),
            "mode": "live",
            "dry_run": self.dry_run,
            "engine": self._last_engine_snapshot,
            "open_positions": [asdict(p) for p in self.open_positions.values()],
            "stats": self.stats(),
            "caps": {
                "max_stake_usd": self.limits.max_order_usd,
                "max_trades_per_hour": self.rate_limiter.max_per_hour,
                "max_trades_per_day": self.rate_limiter.max_per_day,
                "max_daily_loss_usd": self.loss_guard.max_daily_loss_usd,
                "daily_loss_guard_tripped": self.loss_guard.tripped(),
            },
            "recent_closed": [asdict(p) for p in self.closed_positions[-25:]],
        }
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2))
        os.replace(tmp, self.state_path)

    # -- portfolio math --------------------------------------------------

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
        }

    def _fetch_usdc_balance(self) -> float:
        resp = self.client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        raw = resp.get("balance")
        return int(raw or 0) / 10**6

    # -- trading ----------------------------------------------------------

    def on_price_tick(self, price: float, ts: float | None = None):
        self.engine.on_price_tick(price, ts)

    def process_market_update(self, market: finder.Market, up_ask: float, down_ask: float) -> LivePosition | None:
        window = self.engine.set_market(market.slug, market.epoch)
        computed = self.engine.compute(up_ask, down_ask)

        pos, skip_reason = self._maybe_open(market, up_ask, down_ask, computed)

        if self.tracker is not None and computed is not None:
            p_up, seconds_remaining, sigma, up_r, down_r = computed
            best_side, best_result = ("UP", up_r) if up_r.edge >= down_r.edge else ("DOWN", down_r)
            self.tracker.log_signal(
                market_slug=market.slug,
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
        self._last_engine_snapshot = None
        self._write_state()

    def _maybe_open(self, market: finder.Market, up_ask: float, down_ask: float, computed) -> tuple:
        slug = market.slug
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

        if self.loss_guard.tripped():
            print(
                f"skip {slug} {sig.side}: daily loss guard tripped (limit ${self.loss_guard.max_daily_loss_usd:.2f})",
                file=sys.stderr,
            )
            self.traded_slugs.add(slug)
            return None, "daily_loss_guard_tripped"

        can_trade, reason = self.rate_limiter.can_trade()
        if not can_trade:
            print(f"skip {slug} {sig.side}: {reason}", file=sys.stderr)
            self.traded_slugs.add(slug)
            return None, "rate_limited"

        stake_usd = min(sig.stake_usd, self.limits.max_order_usd)
        token_id = market.token_up if sig.side == "UP" else market.token_down

        if not self.dry_run:
            try:
                real_balance = self._fetch_usdc_balance()
            except Exception as e:
                print(f"skip {slug} {sig.side}: balance check failed: {e}", file=sys.stderr)
                self.traded_slugs.add(slug)
                return None, "balance_check_failed"
            if stake_usd > real_balance:
                print(
                    f"skip {slug} {sig.side}: stake ${stake_usd:.2f} exceeds real USDC balance ${real_balance:.2f}",
                    file=sys.stderr,
                )
                self.traded_slugs.add(slug)
                return None, "insufficient_real_balance"

        try:
            result = submit_live_market_order(
                self.client, self.limits, token_id, order_executor.BUY, stake_usd, self.dry_run
            )
        except order_executor.RiskCheckFailed as e:
            print(f"skip {slug} {sig.side}: risk check failed: {e}", file=sys.stderr)
            self.traded_slugs.add(slug)
            return None, "risk_check_failed"
        except Exception as e:
            print(f"ORDER FAILED {slug} {sig.side} stake=${stake_usd:.2f}: {e}", file=sys.stderr)
            self.traded_slugs.add(slug)
            return None, "order_submit_failed"

        window = self.engine.market
        shares = stake_usd / sig.market_price if sig.market_price > 0 else 0.0
        pos = LivePosition(
            market_slug=slug,
            side=sig.side,
            entry_price=sig.market_price,
            shares=shares,
            stake_usd=stake_usd,
            opened_at=sig.ts,
            end_epoch=market.epoch,
            anchor_price=window.anchor_price,
            edge_at_entry=sig.edge,
            signal_probability=sig.probability,
            kelly_fraction=sig.kelly_fraction,
            order_id=result.get("orderID") if isinstance(result, dict) else None,
            order_result=result if isinstance(result, dict) else ({"dry_run": True} if self.dry_run else None),
        )
        self.open_positions[slug] = pos
        self.traded_slugs.add(slug)
        self.rate_limiter.record(pos.opened_at)
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
            self.loss_guard.record_pnl(pos.pnl_usd, now)

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
                    filled_price=pos.entry_price,  # FOK market order — no separate fill-price data available here
                    stake_usd=pos.stake_usd,
                    shares=pos.shares,
                    bankroll_before=None,
                    bankroll_after=None,
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


async def market_and_settlement_loop(trader: LiveTrader, poll_interval: float):
    session = requests.Session()
    session.headers.update({"User-Agent": "oracle-lag-live-trader/1.0"})
    while True:
        try:
            settled = trader.try_settle(session)
            for pos in settled:
                print(
                    f"[{time.strftime('%H:%M:%S')}] SETTLED {pos.market_slug} {pos.side} "
                    f"-> {pos.status} est_pnl=${pos.pnl_usd:+.2f} (source={pos.resolution_source})"
                )

            markets = await asyncio.to_thread(finder.fetch_btc_5m_markets, session)
            live_market = next((m for m in markets if m.status == "LIVE"), None) or next(
                (m for m in markets if m.status == "UPCOMING"), None
            )

            if live_market is not None and live_market.best_bid is not None and live_market.best_ask is not None:
                up_ask = live_market.best_ask
                down_ask = 1.0 - live_market.best_bid
                pos = trader.process_market_update(live_market, up_ask, down_ask)
                if pos is not None:
                    print(
                        f"[{time.strftime('%H:%M:%S')}] OPENED LIVE {pos.market_slug} {pos.side} "
                        f"price={pos.entry_price:.3f} stake=${pos.stake_usd:.2f} edge={pos.edge_at_entry:+.1%} "
                        f"order_id={pos.order_id}"
                    )
            else:
                trader.clear_engine_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market/settlement loop error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(trader: LiveTrader, telegram_token, telegram_chat_id, check_interval: float = 30.0):
    while True:
        try:
            summary = trader.engine.stats.maybe_flush()
            if summary is not None:
                s = trader.stats()
                portfolio_line = (
                    f"\nLive trading — trades={s['trades']} win_rate={(s['win_rate'] or 0) * 100:.1f}%  "
                    f"est_realized_pnl=${s['realized_pnl']:+.2f}"
                )
                strategy.send_telegram_message(summary + portfolio_line, telegram_token, telegram_chat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"stats flush loop error: {e}", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run(args) -> int:
    """Returns a process exit code rather than calling sys.exit() directly —
    raising SystemExit from inside a coroutine driven by
    loop.run_until_complete() doesn't cleanly stop the process, it just
    leaves an unhandled exception dangling on the task. main() turns this
    return value into the actual process exit."""
    limits = order_executor.RiskLimits(max_order_usd=args.max_stake_usd)
    client = order_executor.build_client(args)
    address = client.get_address()

    ready = run_preflight_gate(client, address, args.rpc_url, args.chain_id)
    if not ready:
        print(
            "\nPreflight NOT READY — refusing to start live trading. "
            "Run `python preflight_check.py` for the full report and fix the failing checks first.",
            file=sys.stderr,
        )
        return 1

    engine = strategy.OracleLagEngine(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        min_edge=args.min_edge,
        max_position_pct=args.max_position_pct,
        vol_window_seconds=args.vol_window,
        fallback_sigma_annual=args.fallback_sigma_annual,
    )
    tracker = None if args.no_perf_log else performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir)
    rate_limiter = RateLimiter(args.max_trades_per_hour, args.max_trades_per_day)
    loss_guard = DailyLossGuard(args.max_daily_loss_usd)
    trader = LiveTrader(
        client,
        limits,
        engine,
        Path(args.state_file),
        Path(args.trades_log),
        rate_limiter,
        loss_guard,
        dry_run=args.dry_run,
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
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        "--bankroll", type=float, default=50.0, help="Reference bankroll for Kelly sizing (default: 50.0)"
    )
    parser.add_argument("--kelly-multiplier", type=float, default=0.25)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--max-position-pct", type=float, default=0.05)
    parser.add_argument("--vol-window", type=float, default=180.0)
    parser.add_argument("--fallback-sigma-annual", type=float, default=0.6)
    parser.add_argument("--market-poll-interval", type=float, default=2.0)

    parser.add_argument(
        "--max-stake-usd", type=float, default=2.0, help="Hard cap on stake per trade in USD (default: 2.0)"
    )
    parser.add_argument("--max-trades-per-hour", type=int, default=4)
    parser.add_argument("--max-trades-per-day", type=int, default=20)
    parser.add_argument(
        "--max-open-positions",
        type=int,
        default=1,
        help="Cap on concurrently open real positions (default: 1, the safest setting). A position from a "
        "just-closed window can still be awaiting settlement when the next window's signal qualifies — this "
        "bounds how many can stack up unsettled at once. Raise it to allow deliberate overlap.",
    )
    parser.add_argument(
        "--max-daily-loss-usd",
        type=float,
        default=None,
        help="Optional cumulative stop-loss: halt new trades once today's estimated P&L drops below -this (default: disabled)",
    )

    parser.add_argument("--host", default=order_executor.DEFAULT_HOST)
    parser.add_argument("--chain-id", type=int, default=order_executor.DEFAULT_CHAIN_ID)
    parser.add_argument("--rpc-url", default=preflight.DEFAULT_RPC_URL)
    parser.add_argument("--private-key-env", default="POLYMARKET_PRIVATE_KEY")
    parser.add_argument("--keystore", default=None)
    parser.add_argument("--signature-type", type=int, default=0, choices=[0, 1, 2])
    parser.add_argument("--funder", default=os.environ.get("POLYMARKET_FUNDER") or None)

    parser.add_argument("--telegram-token", default=None)
    parser.add_argument("--telegram-chat-id", default=None)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument("--reset", action="store_true", help="Ignore any existing --state-file and start over")
    parser.add_argument("--perf-log-dir", default=str(DEFAULT_PERF_LOG_DIR))
    parser.add_argument("--no-perf-log", action="store_true")
    parser.add_argument("--pid-file", default=str(DEFAULT_PID_PATH))
    parser.add_argument(
        "--autorestart-marker",
        default=str(DEFAULT_AUTORESTART_MARKER),
        help="Marker file watchdog.py uses to tell a crash apart from an intentional stop",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log intended orders but never call create_market_order/post_order — preflight gate still applies",
    )
    args = parser.parse_args()

    if args.signature_type in (1, 2) and not args.funder:
        parser.error("--funder is required when --signature-type is 1 or 2")

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

    exit_code = 0
    try:
        exit_code = loop.run_until_complete(task)
    except asyncio.CancelledError:
        # Deliberate stop — clear the marker so watchdog.py won't resurrect a
        # live-money bot the user intentionally stopped. Any other exception
        # here is a real crash and left uncleared on purpose (see pidfile.py).
        pidfile.clear_autorestart_marker(marker_path)
    finally:
        loop.close()

    sys.exit(exit_code or 0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Oracle-lag strategy engine, v2: the same Brownian-motion probability model
and Kelly sizing as oracle_lag_strategy.py, with three additional entry
filters ported from a reference implementation
(github.com/JLowo/gengar_polymarket_bot) that hardens entry discipline on
the same edge. Reuses v1's RollingVolatilityEstimator, brownian_probability_up,
KellySizer, MarketWindow, and HourlyStatsTracker
directly by import rather than duplicating them — the probability/Kelly math
is identical between v1 and v2; only what qualifies as a tradeable signal
changes. Kept as a separate, parallel engine rather than an in-place change
to oracle_lag_strategy.py so v1 stays runnable unchanged for comparison —
run both against the same live data via paper_trader.py (--v2 to opt into
this engine) and judge which trades better over time, same as
trend_strategy.py's relationship to oracle_lag_strategy.py.

The three new filters, all ANDed together with v1's existing --min-edge
check (a genuine signal has to clear all four to fire):

    1. --min-prob: an absolute floor on the model's probability for the
       side being considered (default 0.80) — separate from min-edge.
       min-edge alone (edge = p - price >= threshold) says nothing about
       how confident the model actually is; a p=0.55 vs price=0.52 clears
       a small min-edge but is close to a coin flip. This filter refuses
       to size a trade at all below the probability floor, regardless of
       edge.

    2. --safety-factor: a MULTIPLICATIVE margin-of-safety filter,
       market_price <= p_true * safety_factor (default 0.85) — distinct
       from the ADDITIVE min-edge filter (p_true - price >= min_edge).
       These diverge sharply away from p=0.5: at p=0.90, min_edge=0.02
       allows paying up to 0.88, but safety_factor=0.85 only allows paying
       up to 0.765. The idea (value-investing framed, per the reference
       repo): if the model is off by up to (1 - safety_factor) in relative
       terms, the trade still breaks even. In practice, at the recommended
       defaults this filter dominates min-edge almost everywhere above the
       probability floor — see run_selftest() for a worked comparison.

    3. --entry-window-start / --entry-window-end: only evaluate for entry
       when seconds_remaining is between --entry-window-end and
       --entry-window-start (defaults 10 and 240, for a 300s/5-minute
       window) — i.e. skip the first 60s after a window opens (anchor
       price / volatility estimate are least reliable right at open) and
       the last 10s before close (not enough runway for an order to fill
       before resolution). v1 has no lower bound at all and evaluates
       continuously for the whole window.

Usage:
    python oracle_lag_strategy_v2.py --selftest   # synthetic, no-network sanity check
    python oracle_lag_strategy_v2.py --live       # real Binance + Polymarket feeds, read-only (prints signals)
"""

import argparse
import asyncio
import dataclasses
import signal as signal_module
import sys
import time
from datetime import UTC, datetime

import requests

import btc_5m_market_finder as finder
import btc_price_feed as price_feed
import notify
import oracle_lag_strategy as v1

WINDOW_SECONDS = v1.WINDOW_SECONDS

DEFAULT_MIN_PROB = 0.80
DEFAULT_SAFETY_FACTOR = 0.85
DEFAULT_ENTRY_WINDOW_START = 240.0
DEFAULT_ENTRY_WINDOW_END = 10.0


def passes_v2_filters(p_true: float, market_price: float, min_prob: float, safety_factor: float) -> bool:
    """The two new per-side filters (1 and 2 from the module docstring),
    factored out so they can be unit-tested directly without needing a
    full engine/market-window setup — see run_selftest()."""
    if p_true < min_prob:
        return False
    return market_price <= p_true * safety_factor


class OracleLagEngineV2:
    """Same on_price_tick(...)/set_market(...)/compute(...)/evaluate(...)
    interface as v1.OracleLagEngine (and the same 5-tuple shape from
    compute()) — a drop-in for anything written against that interface,
    e.g. paper_trading/paper_trader.py's PaperTrader (--v2-selectable)."""

    def __init__(
        self,
        bankroll_usd: float = 1000.0,
        kelly_multiplier: float = 0.25,
        min_edge: float = 0.05,
        max_position_pct: float = 0.05,
        vol_window_seconds: float = 180.0,
        fallback_sigma_annual: float = 0.6,
        min_prob: float = DEFAULT_MIN_PROB,
        safety_factor: float = DEFAULT_SAFETY_FACTOR,
        entry_window_start: float = DEFAULT_ENTRY_WINDOW_START,
        entry_window_end: float = DEFAULT_ENTRY_WINDOW_END,
        stats_label: str = "Oracle-lag scalper v2",
    ):
        self.vol = v1.RollingVolatilityEstimator(
            window_seconds=vol_window_seconds, fallback_sigma_annual=fallback_sigma_annual
        )
        self.sizer = v1.KellySizer(bankroll_usd, kelly_multiplier, min_edge, max_position_pct)
        self.stats = v1.HourlyStatsTracker(version_label=stats_label)
        self.min_prob = min_prob
        self.safety_factor = safety_factor
        self.entry_window_start = entry_window_start
        self.entry_window_end = entry_window_end
        self.last_price: float | None = None
        self.market: v1.MarketWindow | None = None

    def on_price_tick(self, price: float, ts: float | None = None) -> None:
        self.vol.update(price, ts)
        self.last_price = price

    def set_market(self, slug: str, end_epoch: int) -> v1.MarketWindow:
        """Identical logic to v1.OracleLagEngine.set_market — see its
        docstring."""
        if self.market is None or self.market.slug != slug:
            self.market = v1.MarketWindow(slug=slug, end_epoch=end_epoch, anchor_price=None)

        if self.market.anchor_price is None:
            anchor_epoch = end_epoch - WINDOW_SECONDS
            anchor_price = self.vol.price_near(anchor_epoch)
            if anchor_price is not None:
                self.market.anchor_price = anchor_price
                self.market.anchor_is_approximate = False
            elif self.last_price is not None:
                self.market.anchor_price = self.last_price
                self.market.anchor_is_approximate = True

        return self.market

    def compute(self, up_ask: float, down_ask: float):
        """Same probability + both-sides Kelly sizing as v1.compute(), with
        the min-prob/safety-factor filters (1, 2) applied to each side's
        KellyResult — a side that fails either filter comes back with
        stake_usd/shares forced to 0, same as failing min-edge. Does NOT
        apply the entry-window filter (3) — that's evaluate()'s job, same
        split as v1 (compute() is also used for live status display, which
        should keep showing probabilities/edges outside the entry window,
        just not act on them)."""
        if self.market is None or self.last_price is None or self.market.anchor_price is None:
            return None
        seconds_remaining = self.market.end_epoch - time.time()
        if seconds_remaining <= 0:
            return None

        sigma = self.vol.sigma_per_sqrt_sec
        p_up = v1.brownian_probability_up(self.market.anchor_price, self.last_price, seconds_remaining, sigma)

        up_result = self.sizer.size(p_up, up_ask)
        if not passes_v2_filters(p_up, up_ask, self.min_prob, self.safety_factor):
            up_result = dataclasses.replace(up_result, stake_usd=0.0, shares=0.0)

        p_down = 1.0 - p_up
        down_result = self.sizer.size(p_down, down_ask)
        if not passes_v2_filters(p_down, down_ask, self.min_prob, self.safety_factor):
            down_result = dataclasses.replace(down_result, stake_usd=0.0, shares=0.0)

        return p_up, seconds_remaining, sigma, up_result, down_result

    def evaluate(self, up_ask: float, down_ask: float, _precomputed=None) -> v1.Signal | None:
        """Full evaluation: applies the entry-window filter (3) on top of
        compute()'s already-filtered results, then the same
        best-of-both-sides selection as v1.evaluate()."""
        computed = _precomputed if _precomputed is not None else self.compute(up_ask, down_ask)
        if computed is None:
            return None
        p_up, seconds_remaining, sigma, up_result, down_result = computed

        if not (self.entry_window_end <= seconds_remaining <= self.entry_window_start):
            return None

        if up_result.stake_usd <= 0 and down_result.stake_usd <= 0:
            return None

        if up_result.stake_usd >= down_result.stake_usd:
            side, price, prob, result = "UP", up_ask, p_up, up_result
        else:
            side, price, prob, result = "DOWN", down_ask, 1.0 - p_up, down_result

        sig = v1.Signal(
            ts=time.time(),
            market_slug=self.market.slug,
            side=side,
            probability=prob,
            market_price=price,
            edge=result.edge,
            kelly_fraction=result.applied_kelly,
            stake_usd=result.stake_usd,
            shares=result.shares,
            seconds_remaining=seconds_remaining,
            sigma_per_sqrt_sec=sigma,
        )
        self.stats.record(sig)
        return sig


# ---------------------------------------------------------------------------
# Live wiring: Binance trade stream + Polymarket btc-updown-5m market
# ---------------------------------------------------------------------------


def status_line(engine: OracleLagEngineV2, window: v1.MarketWindow, up_ask: float, down_ask: float) -> str:
    computed = engine.compute(up_ask, down_ask)
    ts = datetime.now(UTC).strftime("%H:%M:%S")
    if computed is None:
        return f"[{ts}] {window.slug}: waiting for price/window data..."
    p_up, seconds_remaining, sigma, up_result, down_result = computed
    approx = "~" if window.anchor_is_approximate else ""
    in_window = engine.entry_window_end <= seconds_remaining <= engine.entry_window_start
    return (
        f"[{ts}] {window.slug} t-{seconds_remaining:.0f}s {'IN-WINDOW' if in_window else 'outside-entry-window'} "
        f"anchor={approx}{window.anchor_price:.2f} last={engine.last_price:.2f} "
        f"p_up={p_up:.1%} up_ask={up_ask:.3f}(edge{up_result.edge:+.1%}) "
        f"down_ask={down_ask:.3f}(edge{down_result.edge:+.1%}) sigma={sigma:.6f}"
    )


async def market_poll_loop(engine: OracleLagEngineV2, poll_interval: float = 2.0):
    session = requests.Session()
    session.headers.update({"User-Agent": "oracle-lag-scalper-v2/1.0"})
    while True:
        try:
            markets = await asyncio.to_thread(finder.fetch_btc_5m_markets, session)
            live_market = next((m for m in markets if m.status == "LIVE"), None) or next(
                (m for m in markets if m.status == "UPCOMING"), None
            )

            if live_market is None or live_market.best_bid is None or live_market.best_ask is None:
                print("no tradable btc-updown-5m market found right now", file=sys.stderr)
            else:
                window = engine.set_market(live_market.slug, live_market.epoch)
                up_ask = live_market.best_ask
                down_ask = 1.0 - live_market.best_bid  # complementary pricing (Up + Down ~= $1)

                print(status_line(engine, window, up_ask, down_ask))
                sig = engine.evaluate(up_ask, down_ask)
                if sig is not None:
                    print("  >> " + v1.format_signal(sig, window))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market poll error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(
    engine: OracleLagEngineV2, telegram_token: str | None, telegram_chat_id: str | None, check_interval: float = 30.0
):
    while True:
        summary = engine.stats.maybe_flush()
        if summary is not None:
            sent = notify.send_telegram_message(summary, telegram_token, telegram_chat_id)
            if sent:
                print("hourly summary sent to Telegram", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run_live(args):
    engine = OracleLagEngineV2(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        min_edge=args.min_edge,
        max_position_pct=args.max_position_pct,
        vol_window_seconds=args.vol_window,
        fallback_sigma_annual=args.fallback_sigma_annual,
        min_prob=args.min_prob,
        safety_factor=args.safety_factor,
        entry_window_start=args.entry_window_start,
        entry_window_end=args.entry_window_end,
    )

    def on_tick(tick):
        if tick.kind == "trade":
            engine.on_price_tick(tick.data["price"], tick.event_time_ms / 1000.0)
        elif tick.kind == "rest":
            engine.on_price_tick(tick.data["price"])

    price_task = asyncio.ensure_future(
        price_feed.stream_prices(
            "btcusdt", "trade", "1m", on_tick, rest_fallback=True, rest_fallback_after=2, rest_poll_interval=2.0
        )
    )
    market_task = asyncio.ensure_future(market_poll_loop(engine, args.market_poll_interval))
    stats_task = asyncio.ensure_future(stats_flush_loop(engine, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, stats_task)


def run_selftest():
    """Synthetic, no-network sanity check. Demonstrates each of the three
    new filters rejecting a case that v1 would take, AND one case that
    clears all four filters (min-edge + min-prob + safety-factor +
    entry-window) together — proving the composition doesn't reject
    everything, just the point of the exercise."""
    print("=== oracle_lag_strategy_v2 self-test (synthetic data, no network) ===\n")

    min_prob, safety_factor = DEFAULT_MIN_PROB, DEFAULT_SAFETY_FACTOR
    print(f"Filters under test: min_prob={min_prob}  safety_factor={safety_factor}\n")

    print("Case 1: p=0.75, price=0.60 (edge=0.15, clears min-edge; price <= p*safety_factor=0.6375 too)")
    ok = passes_v2_filters(0.75, 0.60, min_prob, safety_factor)
    print(f"  passes_v2_filters -> {ok}")
    assert not ok, "expected REJECTION: p=0.75 is below the min_prob floor of 0.80"
    print("  correctly REJECTED — below the min-prob floor, regardless of edge/safety-factor\n")

    print("Case 2: p=0.85, price=0.73 (edge=0.12, clears min-edge; p clears min-prob)")
    ok = passes_v2_filters(0.85, 0.73, min_prob, safety_factor)
    print(f"  passes_v2_filters -> {ok}  (p*safety_factor = {0.85 * safety_factor:.4f}, price=0.73 exceeds it)")
    assert not ok, "expected REJECTION: price 0.73 > p*safety_factor 0.7225"
    print("  correctly REJECTED — fails the multiplicative safety-factor filter despite clearing min-edge\n")

    print("Case 3: p=0.85, price=0.70 (should pass every filter)")
    ok = passes_v2_filters(0.85, 0.70, min_prob, safety_factor)
    print(f"  passes_v2_filters -> {ok}  (p*safety_factor = {0.85 * safety_factor:.4f}, price=0.70 is under it)")
    assert ok, "expected ACCEPTANCE: this case should clear both new filters"
    print("  correctly ACCEPTED\n")

    print("Case 4: entry-window gate — full engine, strong signal, but too early in the window")
    engine = OracleLagEngineV2(bankroll_usd=1000.0, min_edge=0.0, max_position_pct=1.0, vol_window_seconds=1.0)
    now = time.time()
    anchor = 65000.0
    price = anchor
    for i in range(30):
        price *= 1.002  # sharp synthetic uptrend so p_up ends up well above min_prob
        engine.on_price_tick(price, now - 30 + i)
    # 290s remaining on a 300s window -> more than entry_window_start (240s), i.e. "too early"
    window = v1.MarketWindow(slug="v2-selftest-early", end_epoch=int(now) + 290, anchor_price=anchor)
    engine.market = window
    engine.on_price_tick(price, now)
    computed = engine.compute(up_ask=0.55, down_ask=0.47)
    assert computed is not None
    p_up = computed[0]
    print(f"  p_up={p_up:.1%} (comfortably above min_prob) but seconds_remaining=290 > entry_window_start=240")
    sig = engine.evaluate(up_ask=0.55, down_ask=0.47, _precomputed=computed)
    assert sig is None, "expected no signal: outside the entry window (too early)"
    print("  correctly produced NO signal — outside the entry window, even though the model is confident\n")

    print("Case 5: same setup, now within the entry window -> should fire a real signal")
    window2 = v1.MarketWindow(slug="v2-selftest-inwindow", end_epoch=int(now) + 120, anchor_price=anchor)
    engine.market = window2
    engine.on_price_tick(price, now)
    sig = engine.evaluate(up_ask=0.55, down_ask=0.47)
    assert sig is not None, "expected a real signal: strong p_up, cheap price, inside the entry window"
    print(f"  {v1.format_signal(sig, window2)}")
    print("  correctly FIRED — proves the composed filters don't reject everything\n")

    print("All self-test assertions passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest", action="store_true", help="Run a synthetic, no-network check of the filter math and exit"
    )
    parser.add_argument("--bankroll", type=float, default=1000.0)
    parser.add_argument("--kelly-multiplier", type=float, default=0.25)
    parser.add_argument(
        "--min-edge",
        type=float,
        default=0.05,
        help="Minimum model-vs-market edge (default: 0.05, matching the reference repo this filter set was "
        "ported from — v1's default is 0.02). ANDed with --min-prob and --safety-factor, not a replacement.",
    )
    parser.add_argument("--max-position-pct", type=float, default=0.05)
    parser.add_argument("--vol-window", type=float, default=180.0)
    parser.add_argument("--fallback-sigma-annual", type=float, default=0.6)
    parser.add_argument(
        "--min-prob",
        type=float,
        default=DEFAULT_MIN_PROB,
        help=f"Absolute floor on model probability before considering a side at all (default: {DEFAULT_MIN_PROB})",
    )
    parser.add_argument(
        "--safety-factor",
        type=float,
        default=DEFAULT_SAFETY_FACTOR,
        help="Multiplicative margin of safety: only buy if price <= probability * this "
        f"(default: {DEFAULT_SAFETY_FACTOR})",
    )
    parser.add_argument(
        "--entry-window-start",
        type=float,
        default=DEFAULT_ENTRY_WINDOW_START,
        help=f"Start considering entries once this many seconds remain (default: {DEFAULT_ENTRY_WINDOW_START})",
    )
    parser.add_argument(
        "--entry-window-end",
        type=float,
        default=DEFAULT_ENTRY_WINDOW_END,
        help=f"Stop considering entries once fewer than this many seconds remain (default: {DEFAULT_ENTRY_WINDOW_END})",
    )
    parser.add_argument("--market-poll-interval", type=float, default=2.0)
    parser.add_argument("--telegram-token", default=None)
    parser.add_argument("--telegram-chat-id", default=None)
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run_live(args))

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
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()

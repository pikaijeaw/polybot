#!/usr/bin/env python3
"""
Experimental strategy engine for Polymarket's 5-minute BTC Up/Down markets,
combining two classic trend-following indicators — EMA cross and Supertrend
— as a confirmation filter: it only produces a signal when both agree on
direction. (An earlier version of this experiment tried RSI alone; it was
dropped in favor of this pair. See git history if you want that code back.)

Built for the same reason to doubt it as the RSI version had: a 7-day
backtest against real Binance 1-minute BTC data (2,012 five-minute windows)
found neither EMA(9/21), EMA(20/50), nor Supertrend(10, 3.0)/(7, 2.0) had
predictive power for the next window's Up/Down outcome distinguishable from
the ~50% base rate — every bucket landed within 1-3 points of it. Requiring
both to agree is a real, different hypothesis from either alone (maybe
agreement between two independently-flat trend signals filters noise
better than either does solo) but hasn't itself been backtested — that's
what trend_paper_trading/ is for. --sensitivity defaults low (0.5) given
neither indicator showed strong individual edge.

Optional third confirmation leg (--tv-confirm, off by default): TradingView's
own aggregate technical rating for the symbol (via the `tradingview-ta`
package — a plain HTTP client for the same summary TradingView.com's own
Technical Analysis tab shows, combining ~23 oscillators/moving averages into
one BUY/SELL/NEUTRAL call), polled on its own slow interval rather than
per-tick since it's a network call per refresh. When enabled, all three
signals (EMA cross, Supertrend, TradingView rating) must agree for a
non-neutral p_up — same "agreement filters noise" hypothesis as above,
extended with a third, independently-computed source instead of trusting
this repo's own indicator math alone. Also unbacktested; that's what
--tv-confirm is for A/B testing against the two-indicator baseline.

Reuses oracle_lag_strategy.py's KellySizer, MarketWindow, HourlyStatsTracker,
RollingVolatilityEstimator (only for its price_near() anchor lookup),
WINDOW_SECONDS, and send_telegram_message rather than duplicating them —
TrendConfirmEngine only replaces the probability model. It implements the
exact same on_price_tick(...)/set_market(...)/compute(...)/evaluate(...)
interface (and the same 5-tuple shape from compute()) as OracleLagEngine,
so it's a drop-in swap for anything written against that interface — see
experiments/trend_paper_trader.py, which reuses paper_trading/paper_trader.py's
PaperTrader unmodified this way.

Usage:
    python trend_strategy.py --selftest   # synthetic, no-network sanity check
    python trend_strategy.py              # real Binance + Polymarket feeds, read-only (prints signals, never trades)
"""

import argparse
import asyncio
import signal as signal_module
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from tradingview_ta import TA_Handler

import btc_5m_market_finder as finder
import btc_price_feed as price_feed
import oracle_lag_strategy as strategy


def fetch_tv_rating(symbol: str, exchange: str, screener: str, interval: str) -> str | None:
    """Blocking HTTP call (tradingview-ta has no async client) — call this
    via asyncio.to_thread, never directly from an event loop. Returns
    TradingView's own aggregate RECOMMENDATION for the symbol/interval —
    one of STRONG_BUY/BUY/NEUTRAL/SELL/STRONG_SELL — or None if the
    request fails (rate-limited, symbol not found, network error)."""
    handler = TA_Handler(symbol=symbol, exchange=exchange, screener=screener, interval=interval)
    return handler.get_analysis().summary.get("RECOMMENDATION")


@dataclass
class Bar:
    open: float
    high: float
    low: float
    close: float


class RollingOhlcAggregator:
    """Aggregates raw trade ticks into fixed-length OHLC bars (default 60s).
    A bar only finalizes once a tick from the *next* bucket arrives, so
    there's no look-ahead — the in-progress bar is never included in .bars."""

    def __init__(self, bar_seconds: float = 60.0, max_bars: int = 200):
        self.bar_seconds = bar_seconds
        self._bars: deque = deque(maxlen=max_bars)
        self._current_bucket: int | None = None
        self._current: Bar | None = None

    def update(self, price: float, ts: float | None = None) -> None:
        ts = ts if ts is not None else time.time()
        bucket = int(ts // self.bar_seconds)
        if self._current_bucket is None:
            self._current_bucket = bucket
            self._current = Bar(open=price, high=price, low=price, close=price)
            return
        if bucket != self._current_bucket:
            self._bars.append(self._current)
            self._current_bucket = bucket
            self._current = Bar(open=price, high=price, low=price, close=price)
            return
        self._current.high = max(self._current.high, price)
        self._current.low = min(self._current.low, price)
        self._current.close = price

    @property
    def bars(self) -> list:
        return list(self._bars)


def ema_series(values: list, period: int) -> list:
    """Standard exponential moving average, seeded with the first value."""
    k = 2.0 / (period + 1)
    out = []
    ema = None
    for v in values:
        ema = v if ema is None else v * k + ema * (1 - k)
        out.append(ema)
    return out


def atr_series(bars: list, period: int) -> list:
    """Wilder-smoothed average true range."""
    if not bars:
        return []
    trs = [bars[0].high - bars[0].low]
    for i in range(1, len(bars)):
        tr = max(
            bars[i].high - bars[i].low, abs(bars[i].high - bars[i - 1].close), abs(bars[i].low - bars[i - 1].close)
        )
        trs.append(tr)
    atr = []
    for i, tr in enumerate(trs):
        atr.append(sum(trs[: i + 1]) / (i + 1) if i < period else (atr[i - 1] * (period - 1) + tr) / period)
    return atr


def supertrend_series(bars: list, period: int = 10, multiplier: float = 3.0) -> list:
    """Standard Supertrend: returns a per-bar trend state, 1 (up) or -1
    (down). Path-dependent (each bar's bands depend on the prior bar's), so
    this always recomputes over the full retained bar history rather than
    incrementally — cheap at the bar counts kept here (see max_bars)."""
    if len(bars) < 2:
        return [1] * len(bars)
    atr = atr_series(bars, period)
    n = len(bars)
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    trend = [1] * n
    for i in range(n):
        hl2 = (bars[i].high + bars[i].low) / 2
        basic_upper = hl2 + multiplier * atr[i]
        basic_lower = hl2 - multiplier * atr[i]
        if i == 0:
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            continue
        final_upper[i] = (
            basic_upper
            if (basic_upper < final_upper[i - 1] or bars[i - 1].close > final_upper[i - 1])
            else final_upper[i - 1]
        )
        final_lower[i] = (
            basic_lower
            if (basic_lower > final_lower[i - 1] or bars[i - 1].close < final_lower[i - 1])
            else final_lower[i - 1]
        )
        if trend[i - 1] == 1:
            trend[i] = -1 if bars[i].close < final_lower[i] else 1
        else:
            trend[i] = 1 if bars[i].close > final_upper[i] else -1
    return trend


@dataclass
class Signal:
    ts: float
    market_slug: str
    side: str  # "UP" or "DOWN"
    probability: float
    market_price: float
    edge: float
    kelly_fraction: float
    stake_usd: float
    shares: float
    seconds_remaining: float
    agree_state: float  # +1.0 both agree UP, -1.0 both agree DOWN, 0.0 disagree/insufficient data


class TrendConfirmEngine:
    def __init__(
        self,
        bankroll_usd: float = 1000.0,
        kelly_multiplier: float = 0.25,
        min_edge: float = 0.02,
        max_position_pct: float = 0.05,
        bar_seconds: float = 60.0,
        ema_fast: int = 9,
        ema_slow: int = 21,
        st_period: int = 10,
        st_multiplier: float = 3.0,
        sensitivity: float = 0.5,
        tv_confirm: bool = False,
        tv_symbol: str = "BTCUSDT",
        tv_exchange: str = "BINANCE",
        tv_screener: str = "crypto",
        tv_interval: str = "5m",
        tv_max_age: float = 300.0,
    ):
        self.ohlc = RollingOhlcAggregator(bar_seconds=bar_seconds, max_bars=max(200, ema_slow * 4, st_period * 4))
        # Reused only for its price_near() anchor lookup, not its (unused,
        # lazily-computed, so effectively free) volatility calculation.
        self._anchor_lookup = strategy.RollingVolatilityEstimator(window_seconds=strategy.WINDOW_SECONDS + 60)
        self.sizer = strategy.KellySizer(bankroll_usd, kelly_multiplier, min_edge, max_position_pct)
        self.stats = strategy.HourlyStatsTracker()
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.sensitivity = sensitivity
        self.tv_confirm = tv_confirm
        self.tv_symbol = tv_symbol
        self.tv_exchange = tv_exchange
        self.tv_screener = tv_screener
        self.tv_interval = tv_interval
        self.tv_max_age = tv_max_age
        self._tv_rating: str | None = None
        self._tv_rating_ts: float = 0.0
        self.last_price: float | None = None
        self.market: strategy.MarketWindow | None = None

    def update_tv_rating(self, rating: str | None, ts: float | None = None) -> None:
        """Called by tv_rating_poll_loop with the latest TradingView summary
        RECOMMENDATION. Kept separate from _indicator_state's per-tick local
        computation since this is refreshed on its own (slow, network-bound)
        cadence, not once per price tick."""
        self._tv_rating = rating
        self._tv_rating_ts = ts if ts is not None else time.time()

    def _tv_bullish(self) -> bool | None:
        """None if unset, stale (older than tv_max_age — e.g. the poll loop
        has been failing), or itself NEUTRAL — any of which means "no
        opinion", not "bearish"."""
        if self._tv_rating is None or (time.time() - self._tv_rating_ts) > self.tv_max_age:
            return None
        if self._tv_rating in ("STRONG_BUY", "BUY"):
            return True
        if self._tv_rating in ("STRONG_SELL", "SELL"):
            return False
        return None

    def on_price_tick(self, price: float, ts: float | None = None) -> None:
        self.ohlc.update(price, ts)
        self._anchor_lookup.update(price, ts)
        self.last_price = price

    def set_market(self, slug: str, end_epoch: int) -> strategy.MarketWindow:
        """Identical logic to OracleLagEngine.set_market — see its docstring."""
        if self.market is None or self.market.slug != slug:
            self.market = strategy.MarketWindow(slug=slug, end_epoch=end_epoch, anchor_price=None)

        if self.market.anchor_price is None:
            anchor_epoch = end_epoch - strategy.WINDOW_SECONDS
            anchor_price = self._anchor_lookup.price_near(anchor_epoch)
            if anchor_price is not None:
                self.market.anchor_price = anchor_price
                self.market.anchor_is_approximate = False
            elif self.last_price is not None:
                self.market.anchor_price = self.last_price
                self.market.anchor_is_approximate = True

        return self.market

    def _indicator_state(self):
        """Returns (ema_bullish, st_uptrend), each True/False/None (None =
        not enough finalized bars yet for that indicator)."""
        bars = self.ohlc.bars
        ema_bullish = None
        if len(bars) >= self.ema_slow:
            closes = [b.close for b in bars]
            fast = ema_series(closes, self.ema_fast)
            slow = ema_series(closes, self.ema_slow)
            ema_bullish = fast[-1] > slow[-1]
        st_uptrend = None
        if len(bars) >= self.st_period + 1:
            trend = supertrend_series(bars, self.st_period, self.st_multiplier)
            st_uptrend = trend[-1] == 1
        return ema_bullish, st_uptrend

    def compute(self, up_ask: float, down_ask: float):
        """Returns (p_up, seconds_remaining, agree_state, up_result,
        down_result), or None if there isn't enough state yet — same
        5-tuple shape as OracleLagEngine.compute() (the 3rd slot holds
        agree_state here instead of sigma: +1.0/-1.0/0.0, see Signal).
        p_up stays 0.5 (no edge) until both EMA cross and Supertrend have
        enough bars AND agree on direction — with 60s bars and the
        defaults that's up to ~21 minutes of warmup after (re)starting.
        With --tv-confirm, TradingView's polled rating is folded in as a
        third required vote (see _tv_bullish) — all three must agree."""
        if self.market is None or self.last_price is None or self.market.anchor_price is None:
            return None
        seconds_remaining = self.market.end_epoch - time.time()
        if seconds_remaining <= 0:
            return None

        ema_bullish, st_uptrend = self._indicator_state()
        if ema_bullish is None or st_uptrend is None or ema_bullish != st_uptrend:
            bullish = None
        else:
            bullish = ema_bullish

        if self.tv_confirm and bullish is not None:
            tv_bullish = self._tv_bullish()
            if tv_bullish is None or tv_bullish != bullish:
                bullish = None

        if bullish is None:
            p_up = 0.5
            agree_state = 0.0
        elif bullish:
            p_up = 0.5 + self.sensitivity / 2
            agree_state = 1.0
        else:
            p_up = 0.5 - self.sensitivity / 2
            agree_state = -1.0

        up_result = self.sizer.size(p_up, up_ask)
        down_result = self.sizer.size(1.0 - p_up, down_ask)
        return p_up, seconds_remaining, agree_state, up_result, down_result

    def evaluate(self, up_ask: float, down_ask: float, _precomputed=None) -> Signal | None:
        computed = _precomputed if _precomputed is not None else self.compute(up_ask, down_ask)
        if computed is None:
            return None
        p_up, seconds_remaining, agree_state, up_result, down_result = computed

        if up_result.stake_usd <= 0 and down_result.stake_usd <= 0:
            return None

        if up_result.stake_usd >= down_result.stake_usd:
            side, price, prob, result = "UP", up_ask, p_up, up_result
        else:
            side, price, prob, result = "DOWN", down_ask, 1.0 - p_up, down_result

        sig = Signal(
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
            agree_state=agree_state,
        )
        self.stats.record(sig)
        return sig


# ---------------------------------------------------------------------------
# Live wiring: Binance trade stream + Polymarket btc-updown-5m market
# ---------------------------------------------------------------------------


def format_signal(sig: Signal, window: strategy.MarketWindow, tv_confirm: bool = False) -> str:
    approx = "~" if window.anchor_is_approximate else ""
    ts = datetime.fromtimestamp(sig.ts, tz=UTC).strftime("%H:%M:%S")
    up_label, down_label = ("ALL-UP", "ALL-DOWN") if tv_confirm else ("BOTH-UP", "BOTH-DOWN")
    agree_str = {1.0: up_label, -1.0: down_label, 0.0: "disagree/warming-up"}[sig.agree_state]
    return (
        f"[{ts}] {sig.market_slug} anchor={approx}{window.anchor_price:.2f} "
        f"SIGNAL {sig.side} indicators={agree_str} p={sig.probability:.1%} price={sig.market_price:.3f} "
        f"edge={sig.edge:+.1%} kelly={sig.kelly_fraction:.1%} "
        f"stake=${sig.stake_usd:,.2f} ({sig.shares:.1f} shares) "
        f"t-{sig.seconds_remaining:.0f}s"
    )


def status_line(engine: TrendConfirmEngine, window: strategy.MarketWindow, up_ask: float, down_ask: float) -> str:
    computed = engine.compute(up_ask, down_ask)
    ts = datetime.now(UTC).strftime("%H:%M:%S")
    if computed is None:
        return f"[{ts}] {window.slug}: waiting for price/window data..."
    p_up, seconds_remaining, agree_state, up_result, down_result = computed
    approx = "~" if window.anchor_is_approximate else ""
    up_label, down_label = ("ALL-UP", "ALL-DOWN") if engine.tv_confirm else ("BOTH-UP", "BOTH-DOWN")
    agree_str = {1.0: up_label, -1.0: down_label, 0.0: "disagree/warming-up"}[agree_state]
    return (
        f"[{ts}] {window.slug} t-{seconds_remaining:.0f}s "
        f"anchor={approx}{window.anchor_price:.2f} last={engine.last_price:.2f} indicators={agree_str} "
        f"p_up={p_up:.1%} up_ask={up_ask:.3f}(edge{up_result.edge:+.1%}) "
        f"down_ask={down_ask:.3f}(edge{down_result.edge:+.1%})"
    )


async def market_poll_loop(engine: TrendConfirmEngine, poll_interval: float = 2.0):
    session = requests.Session()
    session.headers.update({"User-Agent": "trend-confirm-scalper/1.0"})
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
                down_ask = 1.0 - live_market.best_bid

                print(status_line(engine, window, up_ask, down_ask))
                sig = engine.evaluate(up_ask, down_ask)
                if sig is not None:
                    print("  >> " + format_signal(sig, window, engine.tv_confirm))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market poll error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def tv_rating_poll_loop(engine: TrendConfirmEngine, poll_interval: float = 30.0):
    """Refreshes engine's cached TradingView rating on its own cadence,
    independent of price ticks and market polling — this is an HTTP call
    per refresh, not something to fire every tick. No-ops if --tv-confirm
    wasn't enabled (nothing reads the cached value in that case, but no
    sense spending the network call)."""
    if not engine.tv_confirm:
        return
    while True:
        try:
            rating = await asyncio.to_thread(
                fetch_tv_rating, engine.tv_symbol, engine.tv_exchange, engine.tv_screener, engine.tv_interval
            )
            engine.update_tv_rating(rating)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"tradingview rating poll error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(engine: TrendConfirmEngine, telegram_token, telegram_chat_id, check_interval: float = 30.0):
    while True:
        summary = engine.stats.maybe_flush()
        if summary is not None:
            sent = strategy.send_telegram_message(summary, telegram_token, telegram_chat_id)
            if sent:
                print("hourly summary sent to Telegram", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run_live(args):
    engine = TrendConfirmEngine(
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
    tv_task = asyncio.ensure_future(tv_rating_poll_loop(engine, args.tv_poll_interval))
    stats_task = asyncio.ensure_future(stats_flush_loop(engine, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, tv_task, stats_task)


def run_selftest():
    """Synthetic, no-network sanity check of the indicator/probability/Kelly math."""
    print("=== trend_strategy self-test (synthetic data, no network) ===\n")

    engine = TrendConfirmEngine(
        bankroll_usd=1000.0, kelly_multiplier=0.25, min_edge=0.0, max_position_pct=1.0, bar_seconds=1.0, ema_slow=21
    )

    now = time.time()
    anchor = 65000.0

    # Feed a synthetic uptrend, one bar (1s here, matching bar_seconds
    # above) apart, so both EMA cross and Supertrend should read bullish.
    price = anchor
    for i in range(30):
        price *= 1.0006
        engine.on_price_tick(price, now - 30 + i)

    window = strategy.MarketWindow(slug="btc-updown-5m-selftest", end_epoch=int(now) + 60, anchor_price=anchor)
    engine.market = window
    engine.on_price_tick(price, now)

    ema_bullish, st_uptrend = engine._indicator_state()
    print(
        f"anchor={anchor:.2f}  current={price:.2f}  ema_bullish={ema_bullish}  st_uptrend={st_uptrend}  bars={len(engine.ohlc.bars)}\n"
    )

    print(status_line(engine, window, up_ask=0.55, down_ask=0.47))
    sig = engine.evaluate(up_ask=0.55, down_ask=0.47)
    if sig is not None:
        print(format_signal(sig, window, engine.tv_confirm))
    else:
        print("No qualifying signal at these quotes (edge below min_edge on both sides)")

    print("\nHourly summary preview:")
    print(strategy.HourlyStatsTracker.build_summary(engine.stats._bucket_start, engine.stats._signals))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest", action="store_true", help="Run a synthetic, no-network check of the math and exit"
    )
    parser.add_argument("--bankroll", type=float, default=1000.0)
    parser.add_argument("--kelly-multiplier", type=float, default=0.25)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--max-position-pct", type=float, default=0.05)
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
        "agree with EMA cross + Supertrend before signaling — off by default, unbacktested (see "
        "module docstring)",
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

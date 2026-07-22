#!/usr/bin/env python3
"""
Strategy engine for the oracle-lag scalper on Polymarket's 5-minute BTC
Up/Down markets.

The premise: each market resolves Up/Down by comparing the Chainlink
BTC/USD price at the end of a 5-minute window to the price at the start of
that window (see btc_5m_market_finder.py). Binance trade prices move
continuously and lead Chainlink's oracle updates by a beat, so watching
Binance (see btc_price_feed.py) gives an early read on which way a window
is likely to resolve before Polymarket's own order book has repriced.

Three parts, usable standalone or together:

    RollingVolatilityEstimator / brownian_probability_up
        Zero-drift GBM estimate of P(price_at_close >= anchor_price) from a
        rolling realized-volatility estimate of recent trade prices.

    KellySizer
        Binary-market Kelly criterion position sizing (quarter-Kelly by
        default).

    HourlyStatsTracker / send_telegram_message
        Rolls signals up into an hourly summary and can push it to a
        Telegram chat via the Bot API.

`OracleLagEngine` wires the three together into a feed(...)/evaluate() loop.
Run this file directly to drive it against live data:

    python oracle_lag_strategy.py --selftest   # synthetic, no network
    python oracle_lag_strategy.py --live       # real Binance + Polymarket feeds

No trading keys required — this only reads public data and prints signals;
it does not place orders.
"""

import argparse
import asyncio
import math
import os
import signal as signal_module
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist

import requests
from dotenv import load_dotenv

import btc_5m_market_finder as finder
import btc_price_feed as price_feed

# Real env vars always win over .env — load_dotenv() defaults to not overriding.
load_dotenv(Path(__file__).resolve().parent / ".env")

WINDOW_SECONDS = 300  # these are 5-minute markets
_NORMAL = NormalDist(0.0, 1.0)


# ---------------------------------------------------------------------------
# 1. Brownian motion probability estimation
# ---------------------------------------------------------------------------


@dataclass
class PriceSample:
    ts: float
    price: float


class RollingVolatilityEstimator:
    """
    Realized-volatility estimator over a rolling window of trade prices,
    expressed as sigma per sqrt(second) so that sigma * sqrt(tau) plugs
    straight into the GBM formula for a horizon of tau seconds.

    Falls back to a fixed annualized-vol assumption until enough samples
    have accumulated (min_samples) — a handful of ticks produces a wildly
    noisy realized-vol estimate. The window is also used to look up the
    price near a market's anchor timestamp, so it's kept at least
    WINDOW_SECONDS + a buffer regardless of what's requested for the vol
    calculation itself.
    """

    def __init__(
        self,
        window_seconds: float = 180.0,
        min_samples: int = 20,
        fallback_sigma_annual: float = 0.6,
        max_samples: int = 10000,
    ):
        self.window_seconds = max(window_seconds, WINDOW_SECONDS + 60)
        self.min_samples = min_samples
        self._fallback_sigma_per_sqrt_sec = fallback_sigma_annual / math.sqrt(365.0 * 24 * 3600)
        self._samples: deque = deque(maxlen=max_samples)

    def update(self, price: float, ts: float | None = None) -> None:
        ts = ts if ts is not None else time.time()
        self._samples.append(PriceSample(ts, price))
        self._evict(ts)

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._samples and self._samples[0].ts < cutoff:
            self._samples.popleft()

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    @property
    def sigma_per_sqrt_sec(self) -> float:
        """Realized volatility over the retained window, per sqrt(second)."""
        samples = list(self._samples)
        if len(samples) < self.min_samples:
            return self._fallback_sigma_per_sqrt_sec

        sum_sq_returns = 0.0
        total_dt = 0.0
        for prev, cur in zip(samples, samples[1:], strict=False):  # deliberately unequal lengths (pairwise)
            dt = cur.ts - prev.ts
            if dt <= 0 or prev.price <= 0 or cur.price <= 0:
                continue
            r = math.log(cur.price / prev.price)
            sum_sq_returns += r * r
            total_dt += dt

        if total_dt <= 0:
            return self._fallback_sigma_per_sqrt_sec

        sigma = math.sqrt(sum_sq_returns / total_dt)
        return sigma if sigma > 0 else self._fallback_sigma_per_sqrt_sec

    def price_near(self, target_ts: float, tolerance: float = 5.0) -> float | None:
        """Closest recorded price to target_ts, if one exists within tolerance seconds."""
        best = None
        best_dt = tolerance
        for s in self._samples:
            dt = abs(s.ts - target_ts)
            if dt <= best_dt:
                best_dt = dt
                best = s.price
        return best


def brownian_probability_up(
    anchor_price: float, current_price: float, seconds_remaining: float, sigma_per_sqrt_sec: float
) -> float:
    """
    P(price at window close >= anchor_price), under zero-drift geometric
    Brownian motion:

        d2 = [ln(S/K) - 0.5*sigma^2*tau] / (sigma*sqrt(tau))
        P(up) = Phi(d2)

    S = current_price, K = anchor_price, tau = seconds_remaining, sigma is
    per sqrt(second) so sigma*sqrt(tau) is total volatility over the
    remaining window. Zero drift is deliberate: over a 5-minute horizon any
    real trend is swamped by noise, and it keeps the estimate a pure
    martingale with no built-in directional bias — the only edge comes from
    already knowing the current price before the market/oracle does.
    """
    if seconds_remaining <= 0:
        return 1.0 if current_price >= anchor_price else 0.0
    if anchor_price <= 0 or current_price <= 0:
        raise ValueError("prices must be positive")

    total_sigma = sigma_per_sqrt_sec * math.sqrt(seconds_remaining)
    if total_sigma <= 1e-12:
        return 1.0 if current_price >= anchor_price else 0.0

    d2 = (math.log(current_price / anchor_price) - 0.5 * total_sigma**2) / total_sigma
    return _NORMAL.cdf(d2)


# ---------------------------------------------------------------------------
# 2. Kelly criterion position sizing
# ---------------------------------------------------------------------------


@dataclass
class KellyResult:
    edge: float
    full_kelly: float
    applied_kelly: float
    stake_usd: float
    shares: float


class KellySizer:
    """
    Binary-market Kelly sizing. For a share priced at market_price that
    pays $1 if you're right and $0 otherwise, the Kelly fraction of
    bankroll to stake is:

        f* = (p - market_price) / (1 - market_price)

    (the standard f* = p - (1-p)/b with net odds b = (1-price)/price on a
    winning $1 stake). kelly_multiplier scales that down — the default 0.25
    (quarter-Kelly) trades some growth rate for a much shallower drawdown
    curve, which matters because the probability model is an approximation,
    not a known-true probability. max_position_pct is a hard cap on top of
    that, independent of what Kelly suggests.
    """

    def __init__(
        self,
        bankroll_usd: float,
        kelly_multiplier: float = 0.25,
        min_edge: float = 0.02,
        max_position_pct: float = 0.05,
    ):
        self.bankroll_usd = bankroll_usd
        self.kelly_multiplier = kelly_multiplier
        self.min_edge = min_edge
        self.max_position_pct = max_position_pct

    def size(self, p_true: float, market_price: float) -> KellyResult:
        edge = p_true - market_price
        if market_price <= 0.0 or market_price >= 1.0:
            full_kelly = 0.0
        else:
            full_kelly = max(0.0, edge / (1.0 - market_price))

        applied = min(full_kelly * self.kelly_multiplier, self.max_position_pct)
        stake = self.bankroll_usd * applied if edge >= self.min_edge else 0.0
        shares = stake / market_price if market_price > 0 and stake > 0 else 0.0
        return KellyResult(edge=edge, full_kelly=full_kelly, applied_kelly=applied, stake_usd=stake, shares=shares)


# ---------------------------------------------------------------------------
# Engine: ties probability estimation + sizing to a live market window
# ---------------------------------------------------------------------------


@dataclass
class MarketWindow:
    slug: str
    end_epoch: int
    anchor_price: float | None = None
    anchor_is_approximate: bool = False


@dataclass
class Signal:
    ts: float
    market_slug: str
    side: str  # "UP" or "DOWN"
    probability: float  # model probability of the chosen side
    market_price: float  # ask price paid for that side
    edge: float
    kelly_fraction: float
    stake_usd: float
    shares: float
    seconds_remaining: float
    sigma_per_sqrt_sec: float


class OracleLagEngine:
    def __init__(
        self,
        bankroll_usd: float = 1000.0,
        kelly_multiplier: float = 0.25,
        min_edge: float = 0.02,
        max_position_pct: float = 0.05,
        vol_window_seconds: float = 180.0,
        fallback_sigma_annual: float = 0.6,
    ):
        self.vol = RollingVolatilityEstimator(
            window_seconds=vol_window_seconds, fallback_sigma_annual=fallback_sigma_annual
        )
        self.sizer = KellySizer(bankroll_usd, kelly_multiplier, min_edge, max_position_pct)
        self.stats = HourlyStatsTracker()
        self.last_price: float | None = None
        self.market: MarketWindow | None = None

    def on_price_tick(self, price: float, ts: float | None = None) -> None:
        self.vol.update(price, ts)
        self.last_price = price

    def set_market(self, slug: str, end_epoch: int) -> MarketWindow:
        """Register the market window currently being traded. Anchor price is
        the price at window open (end_epoch - WINDOW_SECONDS), pulled from
        tick history if we have it; otherwise approximated with the latest
        known price and flagged via anchor_is_approximate.

        If neither is available yet (e.g. we registered the market before
        the first Binance tick arrived), anchor_price is left unresolved and
        retried on every call for this slug until a price becomes available
        — it must never get permanently stuck at None for the window."""
        if self.market is None or self.market.slug != slug:
            self.market = MarketWindow(slug=slug, end_epoch=end_epoch, anchor_price=None)

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
        """Probability + both-sides Kelly sizing without applying the
        min-edge filter or recording a signal. Used for live status display
        as well as by evaluate(). Returns None if there isn't enough state
        yet (no market registered, no price ticks, or window has closed)."""
        if self.market is None or self.last_price is None or self.market.anchor_price is None:
            return None
        seconds_remaining = self.market.end_epoch - time.time()
        if seconds_remaining <= 0:
            return None

        sigma = self.vol.sigma_per_sqrt_sec
        p_up = brownian_probability_up(self.market.anchor_price, self.last_price, seconds_remaining, sigma)
        up_result = self.sizer.size(p_up, up_ask)
        down_result = self.sizer.size(1.0 - p_up, down_ask)
        return p_up, seconds_remaining, sigma, up_result, down_result

    def evaluate(self, up_ask: float, down_ask: float, _precomputed=None) -> Signal | None:
        """Full evaluation: applies the min-edge filter and, if either side
        qualifies, returns a Signal for the better one and records it into
        the hourly stats tracker.

        _precomputed lets a caller that already called compute() this cycle
        (e.g. to log every evaluation, not just qualifying ones) pass that
        result through instead of triggering a second, redundant compute()."""
        computed = _precomputed if _precomputed is not None else self.compute(up_ask, down_ask)
        if computed is None:
            return None
        p_up, seconds_remaining, sigma, up_result, down_result = computed

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
            sigma_per_sqrt_sec=sigma,
        )
        self.stats.record(sig)
        return sig


# ---------------------------------------------------------------------------
# 3. Hourly stats tracking for Telegram summaries
# ---------------------------------------------------------------------------


class HourlyStatsTracker:
    def __init__(self):
        self._bucket_start = self._hour_start()
        self._signals: list = []

    @staticmethod
    def _hour_start(ts: float | None = None) -> int:
        ts = ts if ts is not None else time.time()
        return int(ts // 3600) * 3600

    def record(self, signal: Signal) -> None:
        self._signals.append(signal)

    def maybe_flush(self, now: float | None = None) -> str | None:
        """Returns a formatted summary and resets the bucket once the wall
        clock has rolled into a new hour; otherwise returns None."""
        now = now if now is not None else time.time()
        current_hour = self._hour_start(now)
        if current_hour == self._bucket_start:
            return None
        summary = self.build_summary(self._bucket_start, self._signals)
        self._bucket_start = current_hour
        self._signals = []
        return summary

    @staticmethod
    def build_summary(hour_start: int, signals: list) -> str:
        hour_label = datetime.fromtimestamp(hour_start, tz=UTC).strftime("%Y-%m-%d %H:00 UTC")
        if not signals:
            return f"*Oracle-lag scalper — {hour_label}*\nNo qualifying signals this hour."

        n = len(signals)
        up = sum(1 for s in signals if s.side == "UP")
        down = n - up
        avg_edge = sum(s.edge for s in signals) / n
        avg_prob = sum(s.probability for s in signals) / n
        total_stake = sum(s.stake_usd for s in signals)
        best = max(signals, key=lambda s: s.edge)

        return (
            f"*Oracle-lag scalper — {hour_label}*\n"
            f"Signals: {n}  (UP {up} / DOWN {down})\n"
            f"Avg edge: {avg_edge:+.1%}   Avg model prob: {avg_prob:.1%}\n"
            f"Total suggested stake: ${total_stake:,.2f}\n"
            f"Best signal: {best.side} {best.market_slug}  edge={best.edge:+.1%}  "
            f"stake=${best.stake_usd:,.2f}"
        )


def send_telegram_message(text: str, bot_token: str | None = None, chat_id: str | None = None) -> bool:
    """Posts text to a Telegram chat via the Bot API. Reads credentials from
    the arguments or, if omitted, TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID env
    vars. Returns False (and prints the text to stderr instead) if no
    credentials are configured, OR if the API call itself fails (bad
    token/chat id, network error, rate limit, etc.) — this is a
    best-effort notification, never worth crashing the caller's trading
    loop over."""
    bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print(
            "Telegram not configured (set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID); printing summary instead:\n" + text,
            file=sys.stderr,
        )
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"Telegram send failed ({e}); printing summary instead:\n{text}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# Live wiring: Binance trade stream + Polymarket btc-updown-5m market
# ---------------------------------------------------------------------------


def format_signal(sig: Signal, window: MarketWindow) -> str:
    approx = "~" if window.anchor_is_approximate else ""
    ts = datetime.fromtimestamp(sig.ts, tz=UTC).strftime("%H:%M:%S")
    return (
        f"[{ts}] {sig.market_slug} anchor={approx}{window.anchor_price:.2f} "
        f"SIGNAL {sig.side} p={sig.probability:.1%} price={sig.market_price:.3f} "
        f"edge={sig.edge:+.1%} kelly={sig.kelly_fraction:.1%} "
        f"stake=${sig.stake_usd:,.2f} ({sig.shares:.1f} shares) "
        f"t-{sig.seconds_remaining:.0f}s"
    )


def status_line(engine: OracleLagEngine, window: MarketWindow, up_ask: float, down_ask: float) -> str:
    computed = engine.compute(up_ask, down_ask)
    ts = datetime.now(UTC).strftime("%H:%M:%S")
    if computed is None:
        return f"[{ts}] {window.slug}: waiting for price/window data..."
    p_up, seconds_remaining, sigma, up_result, down_result = computed
    approx = "~" if window.anchor_is_approximate else ""
    return (
        f"[{ts}] {window.slug} t-{seconds_remaining:.0f}s "
        f"anchor={approx}{window.anchor_price:.2f} last={engine.last_price:.2f} "
        f"p_up={p_up:.1%} up_ask={up_ask:.3f}(edge{up_result.edge:+.1%}) "
        f"down_ask={down_ask:.3f}(edge{down_result.edge:+.1%}) sigma={sigma:.6f}"
    )


async def market_poll_loop(engine: OracleLagEngine, poll_interval: float = 2.0):
    session = requests.Session()
    session.headers.update({"User-Agent": "oracle-lag-scalper/1.0"})
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
                    print("  >> " + format_signal(sig, window))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"market poll error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


async def stats_flush_loop(
    engine: OracleLagEngine, telegram_token: str | None, telegram_chat_id: str | None, check_interval: float = 30.0
):
    while True:
        summary = engine.stats.maybe_flush()
        if summary is not None:
            sent = send_telegram_message(summary, telegram_token, telegram_chat_id)
            if sent:
                print("hourly summary sent to Telegram", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run_live(args):
    engine = OracleLagEngine(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        min_edge=args.min_edge,
        max_position_pct=args.max_position_pct,
        vol_window_seconds=args.vol_window,
        fallback_sigma_annual=args.fallback_sigma_annual,
    )

    def on_tick(tick):
        if tick.kind == "trade":
            engine.on_price_tick(tick.data["price"], tick.event_time_ms / 1000.0)
        elif tick.kind == "rest":
            engine.on_price_tick(tick.data["price"])

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
    market_task = asyncio.ensure_future(market_poll_loop(engine, args.market_poll_interval))
    stats_task = asyncio.ensure_future(stats_flush_loop(engine, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, stats_task)


def run_selftest():
    """Synthetic, no-network sanity check of the probability/Kelly/stats math."""
    print("=== oracle_lag_strategy self-test (synthetic data, no network) ===\n")

    engine = OracleLagEngine(bankroll_usd=1000.0, kelly_multiplier=0.25, min_edge=0.0, max_position_pct=1.0)

    now = time.time()
    anchor = 65000.0

    # Feed some synthetic tick history so the realized-vol estimate has samples.
    price = anchor
    for i in range(30):
        price *= 1 + 0.00005 * ((-1) ** i)
        engine.on_price_tick(price, now - 30 + i)

    window = MarketWindow(slug="btc-updown-5m-selftest", end_epoch=int(now) + 60, anchor_price=anchor)
    engine.market = window

    current_price = anchor * 1.0015  # drifted ~0.15% above the anchor with 60s left
    engine.on_price_tick(current_price, now)

    print(
        f"anchor={anchor:.2f}  current={current_price:.2f}  "
        f"sigma/sqrt(s)={engine.vol.sigma_per_sqrt_sec:.6f}  samples={engine.vol.sample_count}\n"
    )

    print(status_line(engine, window, up_ask=0.55, down_ask=0.47))
    sig = engine.evaluate(up_ask=0.55, down_ask=0.47)
    if sig is not None:
        print(format_signal(sig, window))
    else:
        print("No qualifying signal at these quotes (edge below min_edge on both sides)")

    print("\nHourly summary preview:")
    print(HourlyStatsTracker.build_summary(engine.stats._bucket_start, engine.stats._signals))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest", action="store_true", help="Run a synthetic, no-network check of the math and exit"
    )
    parser.add_argument(
        "--bankroll", type=float, default=1000.0, help="Bankroll in USD used for Kelly sizing (default: 1000)"
    )
    parser.add_argument(
        "--kelly-multiplier",
        type=float,
        default=0.25,
        help="Fraction of full Kelly to apply (default: 0.25, quarter-Kelly)",
    )
    parser.add_argument(
        "--min-edge",
        type=float,
        default=0.02,
        help="Minimum model-vs-market edge required to size a trade (default: 0.02)",
    )
    parser.add_argument(
        "--max-position-pct",
        type=float,
        default=0.05,
        help="Cap on stake as a fraction of bankroll, regardless of Kelly (default: 0.05)",
    )
    parser.add_argument(
        "--vol-window",
        type=float,
        default=180.0,
        help="Seconds of trade history used for the realized-vol estimate (default: 180)",
    )
    parser.add_argument(
        "--fallback-sigma-annual",
        type=float,
        default=0.6,
        help="Annualized vol assumption used until enough live samples accumulate (default: 0.6)",
    )
    parser.add_argument(
        "--market-poll-interval",
        type=float,
        default=2.0,
        help="Seconds between Polymarket market/quote polls in --live mode (default: 2.0)",
    )
    parser.add_argument("--telegram-token", default=None, help="Telegram bot token (or set TELEGRAM_BOT_TOKEN)")
    parser.add_argument("--telegram-chat-id", default=None, help="Telegram chat id (or set TELEGRAM_CHAT_ID)")
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

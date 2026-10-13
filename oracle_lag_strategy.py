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

    HourlyStatsTracker / notify.send_telegram_message
        Rolls signals up into an hourly summary and can push it to a
        Telegram chat via the Bot API.

`OracleLagEngine` (v1) wires the three together into a feed(...)/evaluate() loop.
Run this file directly to drive it against live data:

    python oracle_lag_strategy.py --selftest        # synthetic, no network (v1, v2 and v3 checks)
    python oracle_lag_strategy.py --live            # real Binance + Polymarket feeds, v1
    python oracle_lag_strategy.py --live --v2       # same, v2 engine
    python oracle_lag_strategy.py --live --v3       # same, v3 engine (fixed bankroll here, see below)

All three versions live in this one file (v2 and v3 used to be
oracle_lag_strategy_v2.py / _v3.py). They are separate classes, not one
engine with switches, so v1 stays runnable unchanged for comparison:

    OracleLagEngine      v1: probability model + Kelly + --min-edge.
    OracleLagEngineV2    v1 + three extra entry filters (below).
    OracleLagEngineV3    v2 + Kelly sizing off the live bankroll (below).

V2 — ENTRY FILTERS (ported from github.com/JLowo/gengar_polymarket_bot)

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

V3 — COMPOUNDING BANKROLL

Identical filters to v2, with one change: Kelly sizing is computed off the
*current* bankroll instead of the fixed --bankroll value v1/v2 use for the
whole run.

How the current bankroll is supplied: OracleLagEngineV3 does NOT read any
wallet/ledger itself. The caller (paper_trader.py) wires a
zero-arg `bankroll_provider` callback via set_bankroll_provider() *after*
constructing both the engine and the trader, since the trader's constructor
needs the already-built engine (a construction-order chicken-and-egg that
the late-bound setter avoids). The callback must return the trader's
current ABSOLUTE total bankroll (e.g. PaperTrader.equity(), which is cash +
value of open positions), not a delta/pnl figure — a delta approach would
need to be added to a stored "initial bankroll" every call, and stays
correct only if the caller's realized-pnl bookkeeping is itself unbounded.
It isn't, in this repo: PaperTrader.stats()['realized_pnl'] is
summed from an in-memory closed-positions list that, on process restart, is
rebuilt only from the capped `recent_closed` tail (last 25) in the state
file — so a delta fed by that figure would silently understate the true
bankroll after any restart following more than 25 closed trades. Reading
an absolute, restart-safe running balance sidesteps that trap entirely.
Standalone `--live` mode (no trader wrapping the engine) never calls
set_bankroll_provider(), so it behaves exactly like v2 with a fixed
bankroll — compounding only ever activates when a caller opts in.

Bankroll is floored at 0 before being handed to KellySizer, so a
bankroll_provider that (temporarily) returns a negative number never
produces a negative stake.

No trading keys required — this only reads public data and prints signals;
it does not place orders.
"""

import argparse
import asyncio
import dataclasses
import math
import signal as signal_module
import sys
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist

import requests
from dotenv import load_dotenv

import btc_5m_market_finder as finder
import btc_price_feed as price_feed
import notify

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

        # Sample the last price in each whole second, then take returns
        # between those. Raw tick-to-tick returns badly underestimate vol here:
        # Binance prints most trades in same-millisecond bursts (dt == 0), and
        # skipping those pairs threw away the price changes inside them —
        # measured live at ~3%/yr for BTC, making a 15bp move look like
        # certainty. Same-second sampling is the standard fix.
        last_per_sec: dict = {}
        for s in samples:
            if s.price > 0:
                last_per_sec[int(s.ts)] = s.price
        secs = sorted(last_per_sec)
        if len(secs) < 2:
            return self._fallback_sigma_per_sqrt_sec

        sum_sq_returns = sum(
            math.log(last_per_sec[b] / last_per_sec[a]) ** 2
            for a, b in zip(secs, secs[1:], strict=False)  # deliberately unequal lengths (pairwise)
        )
        sigma = math.sqrt(sum_sq_returns / (secs[-1] - secs[0]))
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
    def __init__(self, version_label: str = "Oracle-lag scalper"):
        self._bucket_start = self._hour_start()
        self._signals: list = []
        self._version_label = version_label

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
        summary = self.build_summary(self._bucket_start, self._signals, self._version_label)
        self._bucket_start = current_hour
        self._signals = []
        return summary

    @staticmethod
    def build_summary(hour_start: int, signals: list, version_label: str = "Oracle-lag scalper") -> str:
        hour_label = datetime.fromtimestamp(hour_start, tz=UTC).strftime("%Y-%m-%d %H:00 UTC")
        if not signals:
            return f"*{version_label} — {hour_label}*\nNo qualifying signals this hour."

        n = len(signals)
        up = sum(1 for s in signals if s.side == "UP")
        down = n - up
        avg_edge = sum(s.edge for s in signals) / n
        avg_prob = sum(s.probability for s in signals) / n
        total_stake = sum(s.stake_usd for s in signals)
        best = max(signals, key=lambda s: s.edge)

        return (
            f"*{version_label} — {hour_label}*\n"
            f"Signals: {n}  (UP {up} / DOWN {down})\n"
            f"Avg edge: {avg_edge:+.1%}   Avg model prob: {avg_prob:.1%}\n"
            f"Total suggested stake: ${total_stake:,.2f}\n"
            f"Best signal: {best.side} {best.market_slug}  edge={best.edge:+.1%}  "
            f"stake=${best.stake_usd:,.2f}"
        )


# ---------------------------------------------------------------------------
# v2: extra entry filters. v3: v2 + compounding bankroll. See module docstring.
# ---------------------------------------------------------------------------

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


class OracleLagEngineV2(OracleLagEngine):
    """v1 plus the min-prob / safety-factor / entry-window filters. Same
    interface and the same 5-tuple from compute(), so it is a drop-in for
    anything written against OracleLagEngine (paper_trader.py --v2).
    on_price_tick/set_market are inherited from v1 unchanged."""

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
        super().__init__(
            bankroll_usd, kelly_multiplier, min_edge, max_position_pct, vol_window_seconds, fallback_sigma_annual
        )
        self.stats = HourlyStatsTracker(version_label=stats_label)
        self.min_prob = min_prob
        self.safety_factor = safety_factor
        self.entry_window_start = entry_window_start
        self.entry_window_end = entry_window_end

    def compute(self, up_ask: float, down_ask: float):
        """Same probability + both-sides Kelly sizing as compute(), with
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
        p_up = brownian_probability_up(self.market.anchor_price, self.last_price, seconds_remaining, sigma)

        up_result = self.sizer.size(p_up, up_ask)
        if not passes_v2_filters(p_up, up_ask, self.min_prob, self.safety_factor):
            up_result = dataclasses.replace(up_result, stake_usd=0.0, shares=0.0)

        p_down = 1.0 - p_up
        down_result = self.sizer.size(p_down, down_ask)
        if not passes_v2_filters(p_down, down_ask, self.min_prob, self.safety_factor):
            down_result = dataclasses.replace(down_result, stake_usd=0.0, shares=0.0)

        return p_up, seconds_remaining, sigma, up_result, down_result

    def evaluate(self, up_ask: float, down_ask: float, _precomputed=None) -> Signal | None:
        """Full evaluation: applies the entry-window filter (3) on top of
        compute()'s already-filtered results, then the same
        best-of-both-sides selection as evaluate()."""
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


class OracleLagEngineV3(OracleLagEngineV2):
    """Drop-in for the same on_price_tick(...)/set_market(...)/compute(...)
    /evaluate(...) interface as v1/v2 — only compute() is overridden, to
    refresh the sizer's bankroll from an optional bankroll_provider before
    delegating to v2's (inherited, unmodified) filter logic."""

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
        stats_label: str = "Oracle-lag scalper v3",
    ):
        super().__init__(
            bankroll_usd=bankroll_usd,
            kelly_multiplier=kelly_multiplier,
            min_edge=min_edge,
            max_position_pct=max_position_pct,
            vol_window_seconds=vol_window_seconds,
            fallback_sigma_annual=fallback_sigma_annual,
            min_prob=min_prob,
            safety_factor=safety_factor,
            entry_window_start=entry_window_start,
            entry_window_end=entry_window_end,
            stats_label=stats_label,
        )
        self._initial_bankroll = bankroll_usd
        self._bankroll_provider: Callable[[], float] | None = None

    def set_bankroll_provider(self, provider: Callable[[], float] | None) -> None:
        """Registers a zero-arg callback returning the caller's current
        ABSOLUTE total bankroll (see module docstring for why absolute, not
        delta). Pass None to fall back to the fixed --bankroll value, same
        as v1/v2."""
        self._bankroll_provider = provider

    def current_bankroll(self) -> float:
        if self._bankroll_provider is None:
            return self._initial_bankroll
        return max(0.0, self._bankroll_provider())

    def compute(self, up_ask: float, down_ask: float):
        """Same as compute(), except the sizer's bankroll is refreshed
        from current_bankroll() first, so every sizing decision this tick
        uses the live bankroll rather than the value the engine was
        constructed with."""
        self.sizer.bankroll_usd = self.current_bankroll()
        return super().compute(up_ask, down_ask)


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
    tag = ""
    if isinstance(engine, OracleLagEngineV2):
        in_window = engine.entry_window_end <= seconds_remaining <= engine.entry_window_start
        tag = "IN-WINDOW " if in_window else "outside-entry-window "
    return (
        f"[{ts}] {window.slug} t-{seconds_remaining:.0f}s {tag}"
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
            sent = notify.send_telegram_message(summary, telegram_token, telegram_chat_id)
            if sent:
                print("hourly summary sent to Telegram", file=sys.stderr)
        await asyncio.sleep(check_interval)


async def run_live(args):
    common = dict(
        bankroll_usd=args.bankroll,
        kelly_multiplier=args.kelly_multiplier,
        max_position_pct=args.max_position_pct,
        vol_window_seconds=args.vol_window,
        fallback_sigma_annual=args.fallback_sigma_annual,
    )
    if args.v2 or args.v3:
        # No bankroll provider is wired here, so --v3 sizes off the fixed --bankroll like v2.
        engine = (OracleLagEngineV3 if args.v3 else OracleLagEngineV2)(
            min_edge=0.05 if args.min_edge is None else args.min_edge,
            min_prob=args.min_prob,
            safety_factor=args.safety_factor,
            entry_window_start=args.entry_window_start,
            entry_window_end=args.entry_window_end,
            **common,
        )
    else:
        engine = OracleLagEngine(min_edge=0.02 if args.min_edge is None else args.min_edge, **common)

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


def run_selftest_v2():
    """Synthetic, no-network sanity check. Demonstrates each of the three
    new filters rejecting a case that v1 would take, AND one case that
    clears all four filters (min-edge + min-prob + safety-factor +
    entry-window) together — proving the composition doesn't reject
    everything, just the point of the exercise."""
    print("=== oracle_lag_strategy v2 self-test (synthetic data, no network) ===\n")

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
    window = MarketWindow(slug="v2-selftest-early", end_epoch=int(now) + 290, anchor_price=anchor)
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
    window2 = MarketWindow(slug="v2-selftest-inwindow", end_epoch=int(now) + 120, anchor_price=anchor)
    engine.market = window2
    engine.on_price_tick(price, now)
    sig = engine.evaluate(up_ask=0.55, down_ask=0.47)
    assert sig is not None, "expected a real signal: strong p_up, cheap price, inside the entry window"
    print(f"  {format_signal(sig, window2)}")
    print("  correctly FIRED — proves the composed filters don't reject everything\n")

    print("All self-test assertions passed.")


def run_selftest_v3():
    """Synthetic, no-network sanity check. Proves the one behavioral delta
    from v2: stake size scales with a wired bankroll_provider — up after a
    simulated win, down after a simulated loss, floored at 0 for a negative
    bankroll — while leaving v2's filter behavior (proven by
    run_selftest_v2) untouched."""
    print("=== oracle_lag_strategy v3 self-test (synthetic data, no network) ===\n")

    def make_engine(bankroll_provider=None):
        engine = OracleLagEngineV3(
            bankroll_usd=1000.0, kelly_multiplier=0.25, min_edge=0.0, max_position_pct=1.0, vol_window_seconds=1.0
        )
        if bankroll_provider is not None:
            engine.set_bankroll_provider(bankroll_provider)
        now = time.time()
        anchor = 65000.0
        price = anchor
        for i in range(30):
            price *= 1.002  # sharp synthetic uptrend so p_up clears v2's min_prob/safety-factor floors
            engine.on_price_tick(price, now - 30 + i)
        window = MarketWindow(slug="v3-selftest", end_epoch=int(now) + 120, anchor_price=anchor)
        engine.market = window
        engine.on_price_tick(price, now)
        return engine

    print("Case 1: no bankroll_provider wired -> behaves exactly like a fixed --bankroll of 1000 (v1/v2 parity)")
    engine = make_engine()
    computed = engine.compute(up_ask=0.55, down_ask=0.47)
    assert computed is not None
    baseline_stake = computed[3].stake_usd  # up_result
    print(f"  stake_usd={baseline_stake:.2f} bankroll={engine.sizer.bankroll_usd:.2f}")
    assert baseline_stake > 0, "expected a real stake at the fixed 1000 bankroll"
    print("  correct — matches static v1/v2 sizing\n")

    print("Case 2: bankroll_provider reports a WIN (bankroll grew to 1500) -> stake should scale up")
    engine = make_engine(bankroll_provider=lambda: 1500.0)
    computed = engine.compute(up_ask=0.55, down_ask=0.47)
    win_stake = computed[3].stake_usd
    print(f"  stake_usd={win_stake:.2f} bankroll={engine.sizer.bankroll_usd:.2f}")
    assert engine.sizer.bankroll_usd == 1500.0
    assert win_stake > baseline_stake, "expected a larger stake off a larger live bankroll"
    print(f"  correct — {win_stake:.2f} > {baseline_stake:.2f}, scaled up with the bankroll\n")

    print("Case 3: bankroll_provider reports a LOSS (bankroll shrank to 400) -> stake should scale down")
    engine = make_engine(bankroll_provider=lambda: 400.0)
    computed = engine.compute(up_ask=0.55, down_ask=0.47)
    loss_stake = computed[3].stake_usd
    print(f"  stake_usd={loss_stake:.2f} bankroll={engine.sizer.bankroll_usd:.2f}")
    assert engine.sizer.bankroll_usd == 400.0
    assert loss_stake < baseline_stake, "expected a smaller stake off a smaller live bankroll"
    print(f"  correct — {loss_stake:.2f} < {baseline_stake:.2f}, scaled down with the bankroll\n")

    print("Case 4: bankroll_provider reports a negative number -> floored at 0, never a negative stake")
    engine = make_engine(bankroll_provider=lambda: -50.0)
    computed = engine.compute(up_ask=0.55, down_ask=0.47)
    zero_stake = computed[3].stake_usd
    print(f"  stake_usd={zero_stake:.2f} bankroll={engine.sizer.bankroll_usd:.2f}")
    assert engine.sizer.bankroll_usd == 0.0, "expected the negative provider value to be floored at 0"
    assert zero_stake == 0.0
    print("  correct — floored at 0, no negative stake\n")

    print("Case 5: stats_label defaults to a v3-tagged Telegram summary title")
    summary = HourlyStatsTracker.build_summary(engine.stats._bucket_start, [], engine.stats._version_label)
    print(f"  {summary.splitlines()[0]}")
    assert "v3" in summary
    print("  correct — hourly summaries are distinguishable from v1/v2\n")

    print("All self-test assertions passed.")


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
        default=None,
        help="Minimum model-vs-market edge required to size a trade (default: 0.02 for v1, 0.05 for --v2/--v3)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run against live feeds — already the default; accepted so the documented command works",
    )
    version = parser.add_mutually_exclusive_group()
    version.add_argument("--v2", action="store_true", help="Use OracleLagEngineV2 (v1 + three extra entry filters)")
    version.add_argument("--v3", action="store_true", help="Use OracleLagEngineV3 (v2 + compounding bankroll)")
    parser.add_argument(
        "--min-prob",
        type=float,
        default=DEFAULT_MIN_PROB,
        help=f"(--v2/--v3) Absolute floor on model probability before considering a side (default: {DEFAULT_MIN_PROB})",
    )
    parser.add_argument(
        "--safety-factor",
        type=float,
        default=DEFAULT_SAFETY_FACTOR,
        help=f"(--v2/--v3) Only buy if price <= probability * this (default: {DEFAULT_SAFETY_FACTOR})",
    )
    parser.add_argument(
        "--entry-window-start",
        type=float,
        default=DEFAULT_ENTRY_WINDOW_START,
        help=f"(--v2/--v3) Start considering entries once this many seconds remain (default: {DEFAULT_ENTRY_WINDOW_START})",
    )
    parser.add_argument(
        "--entry-window-end",
        type=float,
        default=DEFAULT_ENTRY_WINDOW_END,
        help=f"(--v2/--v3) Stop considering entries below this many seconds remaining (default: {DEFAULT_ENTRY_WINDOW_END})",
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
        print()
        run_selftest_v2()
        print()
        run_selftest_v3()
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

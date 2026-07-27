#!/usr/bin/env python3
"""
Oracle-lag strategy engine, v3: identical filters to oracle_lag_strategy_v2.py
(min-edge + min-prob + safety-factor + entry-window) via subclassing
OracleLagEngineV2 directly, with one behavioral change — Kelly sizing is
computed off the *current* bankroll instead of the fixed --bankroll value
v1/v2 use for the whole run. As paper_trader.py/live_trader.py's tracked
equity grows or shrinks, position sizes compound with it instead of staying
pinned to the number the process was started with.

Kept as a separate engine (not an in-place v2 change) for the same reason
v2 was kept separate from v1: v1 and v2 stay runnable unchanged for
comparison, and v3 can be A/B'd against both via paper_trader.py's
--v2/--v3 flags.

How the current bankroll is supplied: OracleLagEngineV3 does NOT read any
wallet/ledger itself. The caller (paper_trader.py, live_trader.py) wires a
zero-arg `bankroll_provider` callback via set_bankroll_provider() *after*
constructing both the engine and the trader, since the trader's constructor
needs the already-built engine (a construction-order chicken-and-egg that
the late-bound setter avoids). The callback must return the trader's
current ABSOLUTE total bankroll (e.g. PaperTrader.equity(), which is cash +
value of open positions), not a delta/pnl figure — a delta approach would
need to be added to a stored "initial bankroll" every call, and stays
correct only if the caller's realized-pnl bookkeeping is itself unbounded.
It isn't, in this repo: PaperTrader/LiveTrader.stats()['realized_pnl'] is
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

Usage:
    python oracle_lag_strategy_v3.py --selftest   # synthetic, no-network sanity check
    python oracle_lag_strategy_v3.py --live       # real Binance + Polymarket feeds, read-only, fixed bankroll
                                                   # (compounding only happens when a trader wires a provider)
"""

import argparse
import asyncio
import signal as signal_module
import time
from collections.abc import Callable

import btc_price_feed as price_feed
import oracle_lag_strategy as v1
import oracle_lag_strategy_v2 as v2

DEFAULT_MIN_PROB = v2.DEFAULT_MIN_PROB
DEFAULT_SAFETY_FACTOR = v2.DEFAULT_SAFETY_FACTOR
DEFAULT_ENTRY_WINDOW_START = v2.DEFAULT_ENTRY_WINDOW_START
DEFAULT_ENTRY_WINDOW_END = v2.DEFAULT_ENTRY_WINDOW_END


class OracleLagEngineV3(v2.OracleLagEngineV2):
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
        """Same as v2.compute(), except the sizer's bankroll is refreshed
        from current_bankroll() first, so every sizing decision this tick
        uses the live bankroll rather than the value the engine was
        constructed with."""
        self.sizer.bankroll_usd = self.current_bankroll()
        return super().compute(up_ask, down_ask)


# ---------------------------------------------------------------------------
# Live wiring: Binance trade stream + Polymarket btc-updown-5m market.
# Reuses v2's market_poll_loop/stats_flush_loop as-is (they only call
# methods OracleLagEngineV3 fully inherits/overrides compatibly) rather than
# duplicating them.
# ---------------------------------------------------------------------------


async def run_live(args):
    engine = OracleLagEngineV3(
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
    market_task = asyncio.ensure_future(v2.market_poll_loop(engine, args.market_poll_interval))
    stats_task = asyncio.ensure_future(v2.stats_flush_loop(engine, args.telegram_token, args.telegram_chat_id))

    await asyncio.gather(price_task, market_task, stats_task)


def run_selftest():
    """Synthetic, no-network sanity check. Proves the one behavioral delta
    from v2: stake size scales with a wired bankroll_provider — up after a
    simulated win, down after a simulated loss, floored at 0 for a negative
    bankroll — while leaving v2's filter behavior (proven by
    oracle_lag_strategy_v2.py --selftest) untouched."""
    print("=== oracle_lag_strategy_v3 self-test (synthetic data, no network) ===\n")

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
        window = v1.MarketWindow(slug="v3-selftest", end_epoch=int(now) + 120, anchor_price=anchor)
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
    summary = v1.HourlyStatsTracker.build_summary(engine.stats._bucket_start, [], engine.stats._version_label)
    print(f"  {summary.splitlines()[0]}")
    assert "v3" in summary
    print("  correct — hourly summaries are distinguishable from v1/v2\n")

    print("All self-test assertions passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest", action="store_true", help="Run a synthetic, no-network check of compounding sizing and exit"
    )
    parser.add_argument("--bankroll", type=float, default=1000.0)
    parser.add_argument("--kelly-multiplier", type=float, default=0.25)
    parser.add_argument("--min-edge", type=float, default=0.05)
    parser.add_argument("--max-position-pct", type=float, default=0.05)
    parser.add_argument("--vol-window", type=float, default=180.0)
    parser.add_argument("--fallback-sigma-annual", type=float, default=0.6)
    parser.add_argument("--min-prob", type=float, default=DEFAULT_MIN_PROB)
    parser.add_argument("--safety-factor", type=float, default=DEFAULT_SAFETY_FACTOR)
    parser.add_argument("--entry-window-start", type=float, default=DEFAULT_ENTRY_WINDOW_START)
    parser.add_argument("--entry-window-end", type=float, default=DEFAULT_ENTRY_WINDOW_END)
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

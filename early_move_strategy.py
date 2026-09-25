#!/usr/bin/env python3
"""
Early-move strategy: enter EARLY in a 5-minute window, and only on a real
move — never a coin flip.

At window open BTC sits at the anchor, so any model reads ~50/50 and
picking a side there is random. This engine waits until Binance has moved
clearly away from the anchor within the first ~90s, then buys that side
while Polymarket's book is still near 50c (the oracle-lag edge, taken early
instead of late):

    z = ln(last / anchor) / (sigma * sqrt(elapsed))

"Clearly" means |z| >= --min-z: the move so far is at least that many
standard deviations of what noise alone produces over the elapsed time.
sign(z) picks the side. No clear move inside the entry window = skip the
window entirely.

Entry filters, all must pass:
  1. min_elapsed <= seconds since open <= max_elapsed (default 15..90s) —
     the first seconds are skipped since microstructure noise inflates z at
     tiny horizons.
  2. Anchor is a real recorded price at window open, not approximated (a
     bot started mid-window can't measure the move, so it sits out).
  3. |z| >= min_z.
  4. ask <= max_price — once the book has already repriced, the edge is gone.
  5. model probability - ask >= min_edge (same Brownian model as v1).

Sizing is a FIXED stake per trade (--stake-usd) — no Kelly, no compounding,
never scales up with bankroll.

Same on_price_tick/set_market/compute/evaluate interface as
oracle_lag_strategy.OracleLagEngine (subclassed, so tick history, anchor
lookup, volatility and hourly stats are reused as-is). Driven by
paper_trading/paper_trader.py --early.

Usage:
    python early_move_strategy.py --selftest   # synthetic, no network
    python paper_trading/paper_trader.py --early --stake-usd 5 \
        --state-file paper_state_early.json --trades-log paper_trades_early.jsonl \
        --pid-file paper_trader_early.pid --autorestart-marker paper_trader_early.autorestart.json \
        --perf-log-dir perf_logs_early
"""

import argparse
import math
import time

import oracle_lag_strategy as v1

WINDOW_SECONDS = v1.WINDOW_SECONDS

DEFAULT_STAKE_USD = 5.0
DEFAULT_MIN_Z = 1.5
DEFAULT_MAX_PRICE = 0.65
DEFAULT_MIN_ELAPSED = 15.0
DEFAULT_MAX_ELAPSED = 90.0
DEFAULT_MIN_EDGE = 0.03


def move_z(anchor: float, last: float, elapsed: float, sigma_per_sqrt_sec: float) -> float:
    """Move since the anchor, in standard deviations of noise over `elapsed` seconds."""
    if elapsed <= 0 or sigma_per_sqrt_sec <= 0:
        return 0.0
    return math.log(last / anchor) / (sigma_per_sqrt_sec * math.sqrt(elapsed))


class EarlyMoveEngine(v1.OracleLagEngine):
    def __init__(
        self,
        stake_usd: float = DEFAULT_STAKE_USD,
        min_z: float = DEFAULT_MIN_Z,
        max_price: float = DEFAULT_MAX_PRICE,
        min_elapsed: float = DEFAULT_MIN_ELAPSED,
        max_elapsed: float = DEFAULT_MAX_ELAPSED,
        min_edge: float = DEFAULT_MIN_EDGE,
        vol_window_seconds: float = 180.0,
        fallback_sigma_annual: float = 0.6,
    ):
        # sizer is only kept for its min_edge (paper_trader logs it); stakes are fixed, see _result()
        super().__init__(
            min_edge=min_edge, vol_window_seconds=vol_window_seconds, fallback_sigma_annual=fallback_sigma_annual
        )
        self.stats = v1.HourlyStatsTracker(version_label="Early-move")
        self.stake_usd = stake_usd
        self.min_z = min_z
        self.max_price = max_price
        self.min_elapsed = min_elapsed
        self.max_elapsed = max_elapsed
        self.last_z: float | None = None

    def _result(self, p: float, ask: float, qualifies: bool) -> v1.KellyResult:
        edge = p - ask
        ok = qualifies and 0.0 < ask <= self.max_price and edge >= self.sizer.min_edge
        stake = self.stake_usd if ok else 0.0
        return v1.KellyResult(
            edge=edge, full_kelly=0.0, applied_kelly=0.0, stake_usd=stake, shares=stake / ask if ok else 0.0
        )

    def compute(self, up_ask: float, down_ask: float):
        """Same 5-tuple as v1.compute(). A side gets a non-zero stake only if
        every entry filter passes for it (see module docstring)."""
        m = self.market
        if m is None or self.last_price is None or m.anchor_price is None:
            return None
        seconds_remaining = m.end_epoch - time.time()
        if seconds_remaining <= 0:
            return None

        sigma = self.vol.sigma_per_sqrt_sec
        elapsed = WINDOW_SECONDS - seconds_remaining
        p_up = v1.brownian_probability_up(m.anchor_price, self.last_price, seconds_remaining, sigma)
        z = move_z(m.anchor_price, self.last_price, elapsed, sigma)
        self.last_z = z

        tradeable = not m.anchor_is_approximate and self.min_elapsed <= elapsed <= self.max_elapsed
        up_result = self._result(p_up, up_ask, tradeable and z >= self.min_z)
        down_result = self._result(1.0 - p_up, down_ask, tradeable and z <= -self.min_z)
        return p_up, seconds_remaining, sigma, up_result, down_result

    # evaluate() is inherited: v1 picks whichever side has a stake, builds the Signal, records stats.


def run_selftest():
    print("=== early_move_strategy self-test (synthetic data, no network) ===\n")
    sigma = 0.0001  # per sqrt(second)

    def make_engine(elapsed: float, move_sigmas: float, approximate: bool = False) -> EarlyMoveEngine:
        e = EarlyMoveEngine(stake_usd=5.0, min_z=1.5, fallback_sigma_annual=sigma * math.sqrt(365 * 24 * 3600))
        now = time.time()
        anchor = 65000.0
        e.market = v1.MarketWindow(
            slug="test", end_epoch=int(now + WINDOW_SECONDS - elapsed), anchor_price=anchor,
            anchor_is_approximate=approximate,
        )  # fmt: skip
        e.last_price = anchor * math.exp(move_sigmas * sigma * math.sqrt(elapsed))
        return e

    cases = [
        ("clear up-move at 45s -> UP", make_engine(45, 2.0), 0.52, 0.50, "UP"),
        ("clear down-move at 45s -> DOWN", make_engine(45, -2.0), 0.50, 0.52, "DOWN"),
        ("small move (noise) -> skip", make_engine(45, 0.5), 0.50, 0.50, None),
        ("too early (5s) -> skip", make_engine(5, 3.0), 0.50, 0.50, None),
        ("too late (150s) -> skip", make_engine(150, 3.0), 0.50, 0.50, None),
        ("book already repriced (ask 0.80) -> skip", make_engine(45, 2.0), 0.80, 0.22, None),
        ("approximate anchor -> skip", make_engine(45, 2.0, approximate=True), 0.52, 0.50, None),
    ]
    for name, engine, up_ask, down_ask, want in cases:
        sig = engine.evaluate(up_ask, down_ask)
        got = sig.side if sig else None
        print(f"{name}: got {got} (z={engine.last_z:+.2f})")
        assert got == want, f"{name}: expected {want}, got {got}"
        if sig:
            assert sig.stake_usd == 5.0, "stake must be fixed, never scaled"

    print("\nall early-move self-test cases passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--selftest", action="store_true", help="Run synthetic no-network checks")
    args = parser.parse_args()
    if args.selftest:
        run_selftest()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

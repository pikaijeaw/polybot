#!/usr/bin/env python3
"""
Arbitrage (Pair Cost) v2: the same 5-minute BTC Up/Down bot as
pair_cost_paper_trader.py (v1), plus one rule in front of it:

  0. RISK-FREE PAIR FIRST. Whenever the position is balanced and buying Up
     and Down together, right now, costs less than $1 per pair AFTER FEES
     (by at least --min-margin per pair), buy both sides at once. One pair
     always pays exactly $1, so that profit is locked the moment both legs
     fill — whoever wins.

If there is no such pair, v2 does exactly what v1 does (open a cheap leg,
hedge it later — see v1's docstring), unless --pair-only is set, in which
case it never opens a leg on its own and only ever trades rule 0.

Why rule 0 alone is not enough: a crossed book (Up ask + Down ask under $1
after fees) is rare. Watching the live market for 2 minutes on 2026-10-14
found the two asks summing to $1.01 most of the time, under $1 in 8 of 124
checks, and profitable after fees in 1. Fees are largest at 50/50 — a pair
bought for 97c at 50/50 prices loses about half a cent.

"Risk-free" assumes BOTH legs fill. They are simulated as two
Fill-And-Kill limit orders sent together (v1's fill_from_book): after
--latency-ms each leg is matched against a fresh copy of its book, at or
under the price the pair was priced at. If one leg fills less than the
other, the difference is an ordinary unhedged leg — exactly what a real
account would be left with — and the normal hedge rule (v1 rule 2) tries to
close it on later polls, also in --pair-only mode.

The pair is sized with gabagool_strategy.size_arbitrage (walks both ask
ladders, stops where the fee-aware profit per pair drops under the margin),
reused by import rather than copied. Separate state file, pidfile and log
folder from v1, so both can run side by side for comparison.

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/pair_cost_v2_paper_trader.py --selftest
    python paper_trading/pair_cost_v2_paper_trader.py
    python paper_trading/pair_cost_v2_paper_trader.py --pair-only     # risk-free pairs only
"""

import argparse
import asyncio
import signal as signal_module
import sys
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
sys.path.insert(0, str(SCRIPT_DIR))

import pair_cost_paper_trader as v1

import btc_5m_market_finder as finder
import performance_tracker
import pidfile
from gabagool_strategy import FEE_RATE_CRYPTO, _sorted_asks, size_arbitrage

DEFAULT_STATE_PATH = SCRIPT_DIR / "pair_cost_v2_state.json"
DEFAULT_LOG_DIR = performance_tracker.LOG_ROOT / "arbitrage_pair_cost_v2"
DEFAULT_TRADES_LOG_PATH = DEFAULT_LOG_DIR / "events.jsonl"
DEFAULT_PID_PATH = SCRIPT_DIR / "pair_cost_v2_paper_trader.pid"


def price_pair(up_book: dict, down_book: dict, budget: float, min_margin: float, fee_rate: float):
    """Returns (shares, limit_up, limit_down, fill) for the risk-free pair the
    two books offer right now, or None. Limits are the deepest ask level each
    leg needs, so the orders never pay more than the pair was priced at."""
    up, down = _sorted_asks(up_book), _sorted_asks(down_book)
    fill = size_arbitrage(up, down, budget, fee_rate, min_margin)
    if fill is None:
        return None
    shares = int(fill.shares * 100) / 100  # the CLOB takes sizes to 0.01 shares
    if shares < v1.MIN_ORDER_SHARES:
        return None

    def limit(ladder):
        filled = 0.0
        for price, size in ladder:
            filled += size
            if filled >= shares - 1e-9:
                return price
        return ladder[-1][0]

    return shares, limit(up), limit(down), fill


class PairCostV2PaperTrader(v1.PairCostPaperTrader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pairs = 0  # risk-free pairs fully filled, this run only

    def trade_market(self, market, session: requests.Session) -> None:
        pos = self.positions.get(market.slug) or v1.new_position(market.slug, market.epoch)
        balanced = abs(pos["shares_up"] - pos["shares_down"]) < v1.MIN_ORDER_SHARES
        if balanced and market.accepting_orders and market.seconds_to_close >= self.knobs["min_seconds_remaining"]:
            self._try_pair(market, session, pos)
        super().trade_market(market, session)  # v1 rules: hedge any gap, and (unless --pair-only) open legs
        if self.engine_snapshot is not None:
            self.engine_snapshot["rows"].append(["Risk-free pairs (this run)", str(self.pairs)])

    def _try_pair(self, market, session: requests.Session, pos: dict) -> None:
        spent = pos["cost_up"] + pos["cost_down"]
        budget = min(self.cash, self.knobs["clip_usd"], self.knobs["max_window_usd"] - spent)
        books = {
            s: finder.fetch_order_book(session, t) for s, t in (("UP", market.token_up), ("DOWN", market.token_down))
        }
        priced = price_pair(books["UP"], books["DOWN"], budget, self.knobs["min_margin"], self.fee_rate)
        if priced is None:
            return
        shares, limit_up, limit_down, planned = priced

        # Both orders go out together and reach the exchange a moment later.
        time.sleep(self.latency_ms / 1000.0)
        legs = {}
        for side, token, lim in (("UP", market.token_up, limit_up), ("DOWN", market.token_down, limit_down)):
            legs[side] = v1.fill_from_book(finder.fetch_order_book(session, token), shares, lim, self.fee_rate)

        for side, leg in legs.items():
            if isinstance(leg, str):
                self.missed += 1
                continue
            k = side.lower()
            pos[f"shares_{k}"] += leg[0]
            pos[f"cost_{k}"] += leg[1]
            self.cash -= leg[1]
            self.filled += 1
        if any(not isinstance(leg, str) for leg in legs.values()):
            self.positions[market.slug] = pos
        both = all(not isinstance(leg, str) and abs(leg[0] - shares) < 1e-9 for leg in legs.values())
        self.pairs += both

        def leg_record(leg, lim):
            if isinstance(leg, str):
                return {"limit_price": lim, "filled_shares": 0.0, "reason": leg}
            return {"limit_price": lim, "filled_shares": leg[0], "price": leg[2], "cost_usd": leg[1]}

        performance_tracker.append_jsonl(
            self.trades_log_path,
            {
                "event": "PAIR",
                "market_slug": market.slug,
                "want_shares": shares,
                "planned_profit_usd": planned.net_profit * shares / planned.shares,
                "risk_free": both,  # False = one leg came up short; the gap is left to the hedge rule
                "up": leg_record(legs["UP"], limit_up),
                "down": leg_record(legs["DOWN"], limit_down),
                "seconds_remaining": market.seconds_to_close,
                "pair_cost": v1.pair_cost(pos),
            },
        )
        got = " / ".join("miss" if isinstance(leg, str) else f"{leg[0]:.2f}" for leg in legs.values())
        print(
            f"PAIR {market.slug} want {shares:.2f} @ <= {limit_up:.2f}+{limit_down:.2f} -> filled Up/Down {got}"
            + ("" if both else "  (NOT risk-free: legs differ, hedge rule takes over)")
        )


async def run(trader: PairCostV2PaperTrader, poll_interval: float):
    session = requests.Session()
    session.headers.update({"User-Agent": "pair-cost-v2-paper-trader/1.0"})
    while True:
        try:
            await asyncio.to_thread(trader.tick, session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"tick error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


def run_selftest():
    """Synthetic, no-network check of the risk-free pair rule and its fills."""
    import tempfile

    def book(*asks):
        return {"min_order_size": "5", "asks": [{"price": str(p), "size": str(s)} for p, s in asks]}

    # 0.47 + 0.48 = 0.95 before fees, ~0.985 after: profitable at a 0.01 margin.
    priced = price_pair(book((0.47, 50)), book((0.48, 50)), 10.0, 0.01, FEE_RATE_CRYPTO)
    assert priced is not None and priced[0] >= v1.MIN_ORDER_SHARES and priced[1:3] == (0.47, 0.48)
    # 0.485 + 0.485 = 0.97 looks fine but loses money after fees -> no pair.
    assert price_pair(book((0.485, 50)), book((0.485, 50)), 10.0, 0.01, FEE_RATE_CRYPTO) is None
    # Normal book (sum above $1) -> no pair.
    assert price_pair(book((0.51, 50)), book((0.50, 50)), 10.0, 0.01, FEE_RATE_CRYPTO) is None
    # Too little depth for the exchange's 5-share minimum -> no pair.
    assert price_pair(book((0.47, 3)), book((0.48, 50)), 10.0, 0.01, FEE_RATE_CRYPTO) is None

    class FakeMarket:
        slug, epoch, token_up, token_down, accepting_orders = "selftest", int(time.time()) + 200, "U", "D", True
        seconds_to_close = 200.0

    def trader_with(books_now, books_later, pair_only):
        d = Path(tempfile.mkdtemp())
        t = PairCostV2PaperTrader(
            d / "s.json", d / "e.jsonl", 50.0, latency_ms=0.0,
            clip_usd=10.0, entry_price=-1.0 if pair_only else 0.45, min_margin=0.01,
            max_window_usd=20.0, min_seconds_remaining=60.0, fee_rate=FEE_RATE_CRYPTO,
        )  # fmt: skip
        reads = iter([books_now["UP"], books_now["DOWN"], books_later["UP"], books_later["DOWN"]])
        # v1's own read after the pair sees the later books.
        finder.fetch_order_book = lambda s, token: next(reads, books_later["UP" if token == "U" else "DOWN"])
        return t

    real_fetch = finder.fetch_order_book
    try:
        cheap = {"UP": book((0.47, 50)), "DOWN": book((0.48, 50))}
        # Both legs fill -> equal shares, cost under $1 per pair, counted as risk-free.
        t = trader_with(cheap, cheap, pair_only=True)
        t.trade_market(FakeMarket, None)
        p = t.positions["selftest"]
        assert p["shares_up"] == p["shares_down"] >= v1.MIN_ORDER_SHARES and t.pairs == 1
        assert p["cost_up"] + p["cost_down"] < p["shares_up"]  # locked profit, whoever wins
        # Down ask is gone by the time the orders arrive -> only Up fills; NOT risk-free.
        t = trader_with(cheap, {"UP": book((0.47, 50)), "DOWN": book((0.60, 50))}, pair_only=True)
        t.trade_market(FakeMarket, None)
        p = t.positions["selftest"]
        assert p["shares_up"] > 0 and p["shares_down"] == 0 and t.pairs == 0 and t.missed == 1
        # --pair-only on a normal book -> no trade at all (v1 never opens a leg).
        normal = {"UP": book((0.40, 50)), "DOWN": book((0.62, 50))}
        t = trader_with(normal, normal, pair_only=True)
        t.trade_market(FakeMarket, None)
        assert "selftest" not in t.positions
        # Without --pair-only the same book falls back to v1: a 0.40 leg gets opened.
        t = trader_with(normal, normal, pair_only=False)
        t.trade_market(FakeMarket, None)
        assert t.positions["selftest"]["shares_up"] == 10.0
    finally:
        finder.fetch_order_book = real_fetch
    print("All self-test assertions passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--selftest", action="store_true", help="Synthetic, no-network check of the rules, then exit")
    parser.add_argument("--pair-only", action="store_true", help="Only buy risk-free pairs; never open a single leg")
    parser.add_argument("--bankroll", type=float, default=50.0, help="Starting paper bankroll in USD (default: 50)")
    parser.add_argument("--clip-usd", type=float, default=v1.DEFAULT_CLIP_USD, help="Budget for one full pair")
    parser.add_argument(
        "--entry-price", type=float, default=v1.DEFAULT_ENTRY_PRICE, help="(legging) open a leg at or below this ask"
    )
    parser.add_argument(
        "--min-margin",
        type=float,
        default=v1.DEFAULT_MIN_MARGIN,
        help="Profit each pair must lock after fees, in $ per pair (also the legging hedge margin)",
    )
    parser.add_argument("--max-window-usd", type=float, default=v1.DEFAULT_MAX_WINDOW_USD)
    parser.add_argument("--min-seconds-remaining", type=float, default=v1.DEFAULT_MIN_SECONDS_REMAINING)
    parser.add_argument("--latency-ms", type=float, default=v1.DEFAULT_LATENCY_MS)
    parser.add_argument("--fee-rate", type=float, default=FEE_RATE_CRYPTO)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument("--perf-log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--pid-file", default=str(DEFAULT_PID_PATH))
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return

    pidfile.claim_or_exit(Path(args.pid_file))
    trader = PairCostV2PaperTrader(
        Path(args.state_file),
        Path(args.trades_log),
        args.bankroll,
        tracker=performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir),
        latency_ms=args.latency_ms,
        clip_usd=args.clip_usd,
        # an ask can never be <= -1, so v1's "open a leg" rule never fires; its hedge rule still does
        entry_price=-1.0 if args.pair_only else args.entry_price,
        min_margin=args.min_margin,
        max_window_usd=args.max_window_usd,
        min_seconds_remaining=args.min_seconds_remaining,
        fee_rate=args.fee_rate,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run(trader, args.poll_interval))
    for sig in (signal_module.SIGINT, signal_module.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()

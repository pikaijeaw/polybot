#!/usr/bin/env python3
"""
Gabagool strategy engine: YES/NO order-book arbitrage on a single binary
Polymarket market (here, the BTC 15-minute Up/Down series — see
btc_15m_market_finder.py). Named after the strategy in the reference repo
this was ported from (github.com/minculusofia-wq/Apex-Predator), which
describes it as "YES/NO arbitrage when pair cost < $0.975": since a binary
market's two outcomes always sum to exactly $1 at settlement (whichever side
resolves pays $1/share, the other pays $0), buying an equal number of shares
of both sides for a combined cost under $1 locks in a deterministic profit —
outcome-independent, no probability model or Kelly sizing involved. That
makes this a fundamentally different shape of engine than
oracle_lag_strategy.py / trend_strategy.py (which forecast a direction); it
deliberately does not implement their on_price_tick/set_market/compute/evaluate
interface.

THE $0.975 THRESHOLD FROM THE REFERENCE REPO IS NOT USED HERE — it ignores
fees, and fees are not a footnote for an arbitrage strategy, they're the
whole calculation. Polymarket's CLOB charges a taker-only fee of
    fee = shares * fee_rate * price * (1 - price)
(confirmed against https://docs.polymarket.com/trading/fees.md; fee_rate is
0.07 for crypto-category markets, confirmed live via GET /fee-rate for this
series' token IDs, which returned base_fee=1000). This fee is symmetric
around 50c and vanishes near 0/1, so it is *maximized* exactly where a
crossed book is most likely to sit (near 50c) — a naive "sum < 0.975" filter
can and does approve trades that are a guaranteed net loss once fees are
included. Concretely: up=down=0.485 (sum=0.97, comfortably under 0.975) nets
-0.5c/share after fees; see run_selftest() for the worked example. This
engine instead computes the exact fee-aware marginal profit per share at
each order-book level and only takes shares while that marginal profit
clears --min-profit-margin (default half a cent/share, a buffer for the
gap between detecting and filling both legs).

Leg risk: "guaranteed" profit assumes BOTH legs fill at the modeled prices.
paper_trading/gabagool_paper_trader.py simulates that as instantaneous/atomic,
which is realistic for paper trading but would NOT be true of a live version
of this strategy — two separate taker orders sent to two separate token
books can't be submitted atomically, so a real implementation would need to
handle a single-leg fill (directional exposure, the opposite of what this
strategy is for) as a first-class failure mode. That's out of scope here:
this strategy is paper-trading only by design (see
paper_trading/gabagool_paper_trader.py's docstring).

Usage:
    python gabagool_strategy.py --selftest   # synthetic, no-network sanity check
"""

import time
from dataclasses import dataclass

FEE_RATE_CRYPTO = 0.07  # taker fee rate for crypto-category markets, see module docstring


def fee_for_fill(price: float, shares: float, fee_rate: float) -> float:
    """fee = shares * fee_rate * price * (1 - price) — see module docstring
    for the source. Symmetric around price=0.5 (a fill at 0.3 costs the same
    fee as one at 0.7) and zero at price 0 or 1."""
    return shares * fee_rate * price * (1.0 - price)


def _sorted_asks(book: dict) -> list[tuple[float, float]]:
    """(price, size) pairs sorted ascending by price (best/cheapest first).
    Deliberately re-sorts rather than trusting the CLOB response's own
    ordering — /book's asks come back sorted *descending*
    (worst price first, best/lowest last), the opposite of what a naive
    `asks[:N]` read would assume."""
    return sorted(
        ((float(a["price"]), float(a["size"])) for a in book.get("asks", []) if float(a.get("size", 0)) > 0),
        key=lambda x: x[0],
    )


@dataclass
class ArbFill:
    shares: float
    avg_price_up: float
    avg_price_down: float
    cost_up: float
    cost_down: float
    fee_up: float
    fee_down: float
    total_cost: float  # cost_up + cost_down + fee_up + fee_down
    net_profit: float  # shares * 1.0 - total_cost, i.e. the deterministic payout minus what it cost to get there
    net_margin_per_share: float


def size_arbitrage(
    up_asks: list[tuple[float, float]],
    down_asks: list[tuple[float, float]],
    bucket_usd: float,
    fee_rate: float,
    min_profit_margin: float,
) -> ArbFill | None:
    """Walks both ask ladders (each already sorted ascending by price) with
    two pointers, consuming matched share counts from each side
    simultaneously — the fill must carry the SAME share count N on both legs,
    since only equal counts guarantee the fixed $1/share payout the whole
    strategy depends on. At each (up_price, down_price) level pair, stops as
    soon as the fee-aware marginal profit per share

        1 - up_price - down_price - fee_rate * (up_price*(1-up_price) + down_price*(1-down_price))

    drops below min_profit_margin — later levels are always worse (asks only
    get more expensive walking outward), so this is a straightforward greedy
    walk, not a search. Also stops once bucket_usd (total dollars across both
    legs) is exhausted. Returns None if no level pair clears
    min_profit_margin at all (the common case — see module docstring: this
    strategy spends most of its time finding nothing, that's expected)."""
    if not up_asks or not down_asks:
        return None

    i = j = 0
    avail_up = up_asks[0][1]
    avail_down = down_asks[0][1]
    budget_remaining = bucket_usd

    total_shares = 0.0
    cost_up = cost_down = 0.0
    fee_up = fee_down = 0.0

    while i < len(up_asks) and j < len(down_asks) and budget_remaining > 1e-9:
        p_up = up_asks[i][0]
        p_down = down_asks[j][0]
        marginal_profit = 1.0 - p_up - p_down - fee_rate * (p_up * (1.0 - p_up) + p_down * (1.0 - p_down))
        if marginal_profit < min_profit_margin:
            break

        step = min(avail_up, avail_down)
        step_cost = step * (p_up + p_down)
        if step_cost > budget_remaining:
            step = budget_remaining / (p_up + p_down)
            step_cost = budget_remaining
        if step <= 1e-9:
            break

        total_shares += step
        cost_up += step * p_up
        cost_down += step * p_down
        fee_up += fee_for_fill(p_up, step, fee_rate)
        fee_down += fee_for_fill(p_down, step, fee_rate)
        budget_remaining -= step_cost
        avail_up -= step
        avail_down -= step

        if avail_up <= 1e-9:
            i += 1
            if i < len(up_asks):
                avail_up = up_asks[i][1]
        if avail_down <= 1e-9:
            j += 1
            if j < len(down_asks):
                avail_down = down_asks[j][1]

    if total_shares <= 1e-9:
        return None

    total_cost = cost_up + cost_down + fee_up + fee_down
    net_profit = total_shares * 1.0 - total_cost
    return ArbFill(
        shares=total_shares,
        avg_price_up=cost_up / total_shares,
        avg_price_down=cost_down / total_shares,
        cost_up=cost_up,
        cost_down=cost_down,
        fee_up=fee_up,
        fee_down=fee_down,
        total_cost=total_cost,
        net_profit=net_profit,
        net_margin_per_share=net_profit / total_shares,
    )


@dataclass
class ArbOpportunity:
    ts: float
    market_slug: str
    end_epoch: int
    seconds_remaining: float
    shares: float
    avg_price_up: float
    avg_price_down: float
    cost_up: float
    cost_down: float
    fee_up: float
    fee_down: float
    total_cost: float
    net_profit: float
    net_margin_per_share: float


class GabagoolEngine:
    def __init__(
        self,
        bucket_usd: float = 50.0,
        fee_rate: float = FEE_RATE_CRYPTO,
        min_profit_margin: float = 0.005,
        min_seconds_remaining: float = 30.0,
    ):
        self.bucket_usd = bucket_usd
        self.fee_rate = fee_rate
        self.min_profit_margin = min_profit_margin
        self.min_seconds_remaining = min_seconds_remaining

    def evaluate(self, slug: str, end_epoch: int, up_book: dict, down_book: dict) -> ArbOpportunity | None:
        """up_book/down_book are raw CLOB /book responses (dicts with an
        "asks" key) for the market's two outcome tokens. Refuses to open a
        new position inside min_seconds_remaining of close — not because the
        arb math changes, but so a hypothetical future live version has a
        realistic window to get both legs filled before the window ends."""
        seconds_remaining = end_epoch - time.time()
        if seconds_remaining <= self.min_seconds_remaining:
            return None

        fill = size_arbitrage(
            _sorted_asks(up_book), _sorted_asks(down_book), self.bucket_usd, self.fee_rate, self.min_profit_margin
        )
        if fill is None:
            return None

        return ArbOpportunity(
            ts=time.time(),
            market_slug=slug,
            end_epoch=end_epoch,
            seconds_remaining=seconds_remaining,
            shares=fill.shares,
            avg_price_up=fill.avg_price_up,
            avg_price_down=fill.avg_price_down,
            cost_up=fill.cost_up,
            cost_down=fill.cost_down,
            fee_up=fill.fee_up,
            fee_down=fill.fee_down,
            total_cost=fill.total_cost,
            net_profit=fill.net_profit,
            net_margin_per_share=fill.net_margin_per_share,
        )


def format_opportunity(opp: ArbOpportunity) -> str:
    return (
        f"ARB {opp.market_slug} shares={opp.shares:.2f} "
        f"up={opp.avg_price_up:.4f} down={opp.avg_price_down:.4f} "
        f"cost=${opp.cost_up + opp.cost_down:.2f} fees=${opp.fee_up + opp.fee_down:.3f} "
        f"net_profit=${opp.net_profit:.3f} ({opp.net_margin_per_share:+.4f}/share) "
        f"t-{opp.seconds_remaining:.0f}s"
    )


def _book(asks: list[tuple[float, float]]) -> dict:
    return {"asks": [{"price": str(p), "size": str(s)} for p, s in asks]}


def run_selftest():
    """Synthetic, no-network sanity check. Demonstrates the exact scenario
    the module docstring warns about: a pair that clears the reference
    repo's naive "sum < 0.975" filter but is a real loss once the CLOB's
    taker fee is included, alongside a pair that's genuinely profitable."""
    print("=== gabagool_strategy self-test (synthetic data, no network) ===\n")

    engine = GabagoolEngine(bucket_usd=50.0, fee_rate=FEE_RATE_CRYPTO, min_profit_margin=0.005, min_seconds_remaining=0)
    end_epoch = int(time.time()) + 300

    print("Case 1: up=0.485 down=0.485 (sum=0.970, well under naive 0.975 threshold)")
    up_book = _book([(0.485, 500.0)])
    down_book = _book([(0.485, 500.0)])
    fill = size_arbitrage(_sorted_asks(up_book), _sorted_asks(down_book), 50.0, FEE_RATE_CRYPTO, min_profit_margin=-1.0)
    print(
        f"  pre-fee edge = {1 - 0.485 - 0.485:+.4f}/share   "
        f"post-fee net_margin = {fill.net_margin_per_share if fill else float('nan'):+.4f}/share"
    )
    assert fill is not None and fill.net_margin_per_share < 0, "expected this pair to be a net LOSS after fees"
    opp = engine.evaluate("selftest-loss", end_epoch, up_book, down_book)
    assert opp is None, "engine should have refused this trade (fee-aware check), but it didn't"
    print("  engine correctly REFUSED this trade (would lose money net of fees)\n")

    print("Case 2: up=0.46 down=0.50 (sum=0.960)")
    up_book = _book([(0.46, 500.0)])
    down_book = _book([(0.50, 500.0)])
    opp = engine.evaluate("selftest-profit", end_epoch, up_book, down_book)
    assert opp is not None, "expected this pair to clear the profit margin"
    print(f"  {format_opportunity(opp)}")
    print("  engine correctly TOOK this trade\n")

    print("Case 3: normal (uncrossed) book, up=0.53 down=0.49 (sum=1.02) — the typical state")
    up_book = _book([(0.53, 500.0)])
    down_book = _book([(0.49, 500.0)])
    opp = engine.evaluate("selftest-normal", end_epoch, up_book, down_book)
    assert opp is None
    print("  engine correctly found no arbitrage (sum > 1.0, no free money here)\n")

    print("Case 4: deep book walk — first level profitable, second level not")
    up_book = _book([(0.46, 20.0), (0.49, 500.0)])
    down_book = _book([(0.48, 500.0)])
    fill = size_arbitrage(
        _sorted_asks(up_book),
        _sorted_asks(down_book),
        bucket_usd=1000.0,
        fee_rate=FEE_RATE_CRYPTO,
        min_profit_margin=0.005,
    )
    assert fill is not None
    print(f"  filled {fill.shares:.2f} shares (book only offered 20 profitable shares before margin dropped)")
    assert abs(fill.shares - 20.0) < 0.01, (
        f"expected the walk to stop at the 20-share profitable level, got {fill.shares}"
    )
    print("  book-walk correctly stopped at the profitable depth, not the full requested budget\n")

    print("All self-test assertions passed.")


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--selftest", action="store_true", help="Run a synthetic, no-network check of the fee/sizing math and exit"
    )
    args = parser.parse_args()
    if args.selftest:
        run_selftest()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Arbitrage (Pair Cost): paper-trades Polymarket's BTC 5-minute Up/Down markets
by ACCUMULATING both sides at different moments, so that the average cost of
one Up share plus one Down share ("pair cost", fees included) ends up under
the $1 a matched pair always pays at settlement.

How it differs from gabagool_strategy.py: Gabagool buys both legs in the same
instant and only when the book is crossed (up_ask + down_ask + fees < 1),
which is risk-free but almost never happens. This one legs in:

  1. Balanced (equal Up/Down shares, including none): buy one --clip-usd clip
     of whichever side's ask is at or below --entry-price.
  2. Unbalanced: buy the lagging side — only up to the gap, never past it —
     whenever doing so keeps avg_up + avg_down <= 1 - --min-margin.

LEG RISK IS THE WHOLE TRADE-OFF. Step 1 is a naked directional position until
step 2 completes, and step 2 needs the other side to get cheap later. If it
never does, that clip rides to settlement as a plain bet on the side that was
cheap (i.e. the side the market thought was losing). Exposure is bounded to
one clip at a time, because a new leg only opens from a balanced position.

Fees: fee = shares * fee_rate * price * (1 - price), same taker formula and
0.07 crypto rate as gabagool_strategy.py (imported from there, not copied).
Every cost in this file is fee-inclusive.

Fills are simulated at the live CLOB best ask, capped at that level's size.
Settlement uses Polymarket's own Gamma resolution only — there is no price
fallback, because unequal share counts make the payout outcome-dependent and
this bot has no Binance feed to guess from. A position stays open until
Gamma resolves it (measured 6-10+ min after close).

Logs go to log_collector/arbitrage_pair_cost/ in the same files as every other bot (see
log_collector/README.md): signals.csv holds both asks on every poll, events.jsonl has one BUY
line per leg (leg = "open" or "hedge") plus the CLOSE.

State file is written in paper_trader.py's shape (open_positions as a list,
stats.trades/win_rate/realized_pnl, ...) so web_dashboard.py renders and
exports it with the same code as the other panels.

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/pair_cost_paper_trader.py --selftest
    python paper_trading/pair_cost_paper_trader.py
    python paper_trading/pair_cost_paper_trader.py --bankroll 100 --clip-usd 5 --entry-price 0.45
"""

import argparse
import asyncio
import json
import signal as signal_module
import sys
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import btc_5m_market_finder as finder
import performance_tracker
import pidfile
from gabagool_strategy import FEE_RATE_CRYPTO, _sorted_asks, fee_for_fill

DEFAULT_STATE_PATH = SCRIPT_DIR / "pair_cost_state.json"
DEFAULT_LOG_DIR = (
    performance_tracker.LOG_ROOT / "arbitrage_pair_cost"
)  # one folder per bot — see log_collector/README.md
DEFAULT_TRADES_LOG_PATH = DEFAULT_LOG_DIR / "events.jsonl"
DEFAULT_PID_PATH = SCRIPT_DIR / "pair_cost_paper_trader.pid"

DEFAULT_CLIP_USD = 5.0
DEFAULT_ENTRY_PRICE = 0.45
DEFAULT_MIN_MARGIN = 0.01
DEFAULT_MAX_WINDOW_USD = 20.0
DEFAULT_MIN_SECONDS_REMAINING = 60.0

SETTLE_MIN_WAIT_SECONDS = 15.0
MIN_FILL_USD = 0.50  # ignore dust fills
SIDES = ("UP", "DOWN")
# signals.csv columns that only mean something for the probability-model bots
_UNUSED_SIGNAL_FIELDS = (
    "last_price",
    "anchor_price",
    "anchor_is_approximate",
    "sigma_per_sqrt_sec",
    "p_up",
    "up_edge",
    "down_edge",
    "best_edge",
    "kelly_fraction_applied",
    "min_edge_threshold",
)


def new_position(slug: str, end_epoch: int) -> dict:
    return {
        "market_slug": slug,
        "end_epoch": end_epoch,
        "shares_up": 0.0,
        "cost_up": 0.0,  # fee-inclusive
        "shares_down": 0.0,
        "cost_down": 0.0,
        "opened_at": time.time(),
    }


def pair_cost(pos: dict) -> float | None:
    """avg_up + avg_down, fee-inclusive. None until both legs hold shares."""
    if pos["shares_up"] <= 0 or pos["shares_down"] <= 0:
        return None
    return pos["cost_up"] / pos["shares_up"] + pos["cost_down"] / pos["shares_down"]


def decide(
    pos: dict,
    asks: dict,
    cash: float,
    seconds_remaining: float,
    *,
    clip_usd: float = DEFAULT_CLIP_USD,
    entry_price: float = DEFAULT_ENTRY_PRICE,
    min_margin: float = DEFAULT_MIN_MARGIN,
    max_window_usd: float = DEFAULT_MAX_WINDOW_USD,
    min_seconds_remaining: float = DEFAULT_MIN_SECONDS_REMAINING,
    fee_rate: float = FEE_RATE_CRYPTO,
) -> tuple[str, float, float] | None:
    """asks = {"UP": (price, size) | None, "DOWN": ...}, each side's best ask.
    Returns (side, shares, price) to buy, or None. See module docstring."""

    def unit_cost(price: float) -> float:
        return price + fee_for_fill(price, 1.0, fee_rate)

    gap = pos["shares_up"] - pos["shares_down"]
    if abs(gap) > 1e-9:
        # Hedge the lagging side. Allowed right up to the close and outside
        # the per-window cap: it only ever reduces directional exposure.
        lag, lead = ("DOWN", "UP") if gap > 0 else ("UP", "DOWN")
        if asks.get(lag) is None:
            return None
        price, size = asks[lag]
        c = unit_cost(price)
        shares = min(abs(gap), size, cash / c)
        if shares * c < MIN_FILL_USD:
            return None
        k_lag, k_lead = lag.lower(), lead.lower()
        new_avg_lag = (pos[f"cost_{k_lag}"] + shares * c) / (pos[f"shares_{k_lag}"] + shares)
        avg_lead = pos[f"cost_{k_lead}"] / pos[f"shares_{k_lead}"]
        return (lag, shares, price) if new_avg_lag + avg_lead <= 1.0 - min_margin else None

    # Balanced: open a new (temporarily naked) leg on a cheap side.
    budget = min(cash, clip_usd, max_window_usd - pos["cost_up"] - pos["cost_down"])
    if seconds_remaining < min_seconds_remaining or budget < MIN_FILL_USD:
        return None
    cheap = [(asks[s][0], s) for s in SIDES if asks.get(s) is not None and asks[s][0] <= entry_price]
    if not cheap:
        return None
    price, side = min(cheap)
    c = unit_cost(price)
    shares = min(budget / c, asks[side][1])
    return (side, shares, price) if shares * c >= MIN_FILL_USD else None


def _new_stats() -> dict:
    return {"trades": 0, "wins": 0, "losses": 0, "win_rate": None, "total_staked": 0.0, "realized_pnl": 0.0}


class PairCostPaperTrader:
    def __init__(self, state_path: Path, trades_log_path: Path, starting_bankroll: float, tracker=None, **knobs):
        self.state_path = state_path
        self.trades_log_path = trades_log_path
        self.tracker = tracker  # performance_tracker.PerformanceTracker, or None (selftest/ad-hoc use)
        self.knobs = knobs
        self.fee_rate = knobs.get("fee_rate", FEE_RATE_CRYPTO)
        self.starting_bankroll = starting_bankroll
        self.cash = starting_bankroll
        self.positions: dict = {}  # slug -> position dict (see new_position)
        self.recent_closed: list = []
        self.stats = _new_stats()
        self.engine_snapshot: dict | None = None

        try:
            data = json.loads(state_path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        self.starting_bankroll = data["starting_bankroll"]
        self.cash = data["cash"]
        self.positions = {p["market_slug"]: p for p in data.get("open_positions", [])}
        self.recent_closed = data.get("recent_closed", [])
        self.stats = data.get("stats", _new_stats())
        print(f"resumed pair-cost state: cash=${self.cash:.2f}, {len(self.positions)} open", file=sys.stderr)

    def trade_market(self, market, session: requests.Session) -> None:
        asks = {}
        for side, token in (("UP", market.token_up), ("DOWN", market.token_down)):
            ladder = _sorted_asks(finder.fetch_order_book(session, token))
            asks[side] = (
                ladder[0] if ladder else None
            )  # ponytail: top of book only, walk the ladder if clips outgrow it

        pos = self.positions.get(market.slug) or new_position(market.slug, market.epoch)
        seconds_remaining = market.seconds_to_close
        action = None
        if market.accepting_orders:
            action = decide(pos, asks, self.cash, seconds_remaining, **self.knobs)
        if action is not None:
            side, shares, price = action
            cost = shares * price + fee_for_fill(price, shares, self.fee_rate)
            k = side.lower()
            leg = "open" if abs(pos["shares_up"] - pos["shares_down"]) < 1e-9 else "hedge"
            pos[f"shares_{k}"] += shares
            pos[f"cost_{k}"] += cost
            self.cash -= cost
            self.positions[market.slug] = pos
            pc = pair_cost(pos)
            performance_tracker.append_jsonl(
                self.trades_log_path,
                {
                    "event": "BUY",
                    "market_slug": market.slug,
                    "side": side,
                    "leg": leg,
                    "shares": shares,
                    "price": price,
                    "cost_usd": cost,
                    "seconds_remaining": seconds_remaining,
                    "up_ask": asks["UP"] and asks["UP"][0],
                    "down_ask": asks["DOWN"] and asks["DOWN"][0],
                    "pair_cost": pc,
                },
            )
            print(
                f"BUY {market.slug} {side} {shares:.2f} @ {price:.3f} (${cost:.2f} incl. fee) "
                f"-> up={pos['shares_up']:.2f} down={pos['shares_down']:.2f} "
                f"pair_cost={'-' if pc is None else f'{pc:.4f}'}"
            )

        if self.tracker is not None:
            # Book history in the oracle bots' signals.csv format (model columns left blank),
            # so "would a different --entry-price have worked?" is answerable afterwards.
            self.tracker.log_signal(
                market_slug=market.slug,
                seconds_remaining=seconds_remaining,
                up_ask=asks["UP"] and asks["UP"][0],
                down_ask=asks["DOWN"] and asks["DOWN"][0],
                best_side=action[0] if action else "",
                stake_usd=action[1] * action[2] if action else 0.0,
                traded=action is not None,
                **dict.fromkeys(_UNUSED_SIGNAL_FIELDS, ""),
            )

        pc = pair_cost(pos)
        self.engine_snapshot = {
            "market_slug": market.slug,
            "rows": [
                ["Closes in", f"{seconds_remaining:.0f}s"],
                ["Ask Up / Down", " / ".join("-" if asks[s] is None else f"{asks[s][0]:.3f}" for s in SIDES)],
                [
                    "Held Up / Down",
                    f"{pos['shares_up']:.1f} / {pos['shares_down']:.1f}"
                    + (f" · pair cost {pc:.3f}" if pc is not None else ""),
                ],
            ],
        }

    def check_settlements(self, session: requests.Session) -> None:
        now = time.time()
        for slug, pos in list(self.positions.items()):
            if now < pos["end_epoch"] + SETTLE_MIN_WAIT_SECONDS:
                continue
            # ponytail: Gamma-only, waits indefinitely — add a fallback if positions are ever seen stuck open
            winner = finder.resolve_market_outcome(session, slug)
            if winner is None:
                continue

            cost = pos["cost_up"] + pos["cost_down"]
            payout = pos["shares_up"] if winner == "UP" else pos["shares_down"]
            pnl = payout - cost
            self.cash += payout
            closed = {
                **self._display(pos),
                "status": "WON" if pnl >= 0 else "LOST",
                "pnl_usd": pnl,
                "resolution": winner,
                "resolution_source": "gamma",
                "settled_at": now,
            }
            performance_tracker.append_jsonl(self.trades_log_path, {"event": "CLOSE", **closed})
            if self.tracker is not None:
                # Same trades.csv columns as every other bot; the per-leg detail is in events.jsonl.
                self.tracker.log_trade(
                    market_slug=slug,
                    side="PAIR" if pair_cost(pos) is not None else ("UP" if pos["shares_up"] else "DOWN"),
                    signal_ts=pos["opened_at"],
                    signal_probability="",
                    signal_edge="",
                    kelly_fraction="",
                    requested_price=closed["entry_price"],
                    filled_price=closed["entry_price"],
                    stake_usd=cost,
                    shares=max(pos["shares_up"], pos["shares_down"]),
                    bankroll_before="",
                    bankroll_after=self.cash,
                    entry_ts=pos["opened_at"],
                    resolve_ts=now,
                    resolve_outcome=winner,
                    resolve_source="gamma",
                    status=closed["status"],
                    pnl_usd=pnl,
                )

            s = self.stats
            s["trades"] += 1
            s["wins" if pnl >= 0 else "losses"] += 1
            s["win_rate"] = s["wins"] / s["trades"]
            s["total_staked"] += cost
            s["realized_pnl"] += pnl
            self.recent_closed = (self.recent_closed + [closed])[-25:]
            del self.positions[slug]
            print(f"CLOSED {slug} winner={winner} cost=${cost:.2f} payout=${payout:.2f} pnl=${pnl:+.2f}")

    @staticmethod
    def _display(pos: dict) -> dict:
        """Adds the fields the dashboard's shared panel/CSV code reads."""
        pc = pair_cost(pos)
        cost = pos["cost_up"] + pos["cost_down"]
        return {
            **pos,
            "side": f"U{pos['shares_up']:.1f}/D{pos['shares_down']:.1f}",
            # pair cost once both legs exist; until then the single leg's avg cost
            "entry_price": pc if pc is not None else cost / max(pos["shares_up"] + pos["shares_down"], 1e-9),
            "stake_usd": cost,
        }

    def save_state(self) -> None:
        open_cost = sum(p["cost_up"] + p["cost_down"] for p in self.positions.values())
        equity = self.cash + open_cost
        snapshot = {
            "starting_bankroll": self.starting_bankroll,
            "cash": self.cash,
            "equity": equity,
            "engine": self.engine_snapshot,
            "open_positions": [self._display(p) for p in self.positions.values()],
            "recent_closed": self.recent_closed,
            "stats": {**self.stats, "return_pct": equity / self.starting_bankroll - 1.0},
            "updated_at": time.time(),
        }
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2))
        tmp.replace(self.state_path)

    def tick(self, session: requests.Session) -> None:
        live = next((m for m in finder.fetch_btc_5m_markets(session) if m.status == "LIVE"), None)
        if live is not None:
            self.trade_market(live, session)
        self.check_settlements(session)
        self.save_state()


async def run(trader: PairCostPaperTrader, poll_interval: float):
    session = requests.Session()
    session.headers.update({"User-Agent": "pair-cost-paper-trader/1.0"})
    while True:
        try:
            await asyncio.to_thread(trader.tick, session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"tick error: {e}", file=sys.stderr)
        await asyncio.sleep(poll_interval)


def run_selftest():
    """Synthetic, no-network check of the leg-in / hedge / refuse rules."""
    pos = new_position("selftest", int(time.time()) + 300)
    deep = 1e6

    # Balanced, nothing cheap -> no trade.
    assert decide(pos, {"UP": (0.51, deep), "DOWN": (0.50, deep)}, 50.0, 200.0) is None
    # Balanced, Up cheap -> open one clip of Up.
    side, shares, price = decide(pos, {"UP": (0.44, deep), "DOWN": (0.57, deep)}, 50.0, 200.0)
    assert side == "UP" and abs(shares * (price + fee_for_fill(price, 1, FEE_RATE_CRYPTO)) - DEFAULT_CLIP_USD) < 1e-6
    # ...but not too close to the close.
    assert decide(pos, {"UP": (0.44, deep), "DOWN": (0.57, deep)}, 50.0, 30.0) is None

    pos["shares_up"], pos["cost_up"] = shares, DEFAULT_CLIP_USD  # avg_up ~0.457 incl. fee
    # Unbalanced, Down still expensive -> refuse: pair cost would be >= 1 - margin.
    assert decide(pos, {"UP": (0.40, deep), "DOWN": (0.57, deep)}, 50.0, 200.0) is None
    # 0.52 looks fine pre-fee (0.44 + 0.52 = 0.96) but is 0.995 with fees -> still refuse.
    assert decide(pos, {"UP": (0.40, deep), "DOWN": (0.52, deep)}, 50.0, 200.0) is None
    # Down gets cheap -> hedge exactly the gap, even inside the last minute.
    side, hedge, price = decide(pos, {"UP": (0.52, deep), "DOWN": (0.49, deep)}, 50.0, 5.0)
    assert side == "DOWN" and abs(hedge - shares) < 1e-9

    pos["shares_down"] = hedge
    pos["cost_down"] = hedge * price + fee_for_fill(price, hedge, FEE_RATE_CRYPTO)
    pc = pair_cost(pos)
    assert pc < 1.0 - DEFAULT_MIN_MARGIN
    # Locked: either outcome pays `shares`, which exceeds total cost.
    assert shares - (pos["cost_up"] + pos["cost_down"]) > 0
    # Thin book -> hedge is capped at the level's size, never past the gap.
    pos["shares_down"] = 0.0
    pos["cost_down"] = 0.0
    assert decide(pos, {"UP": (0.52, deep), "DOWN": (0.49, 3.0)}, 50.0, 200.0)[1] == 3.0
    print(f"All self-test assertions passed (locked pair cost {pc:.4f}).")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--selftest", action="store_true", help="Synthetic, no-network check of the rules, then exit")
    parser.add_argument("--bankroll", type=float, default=50.0, help="Starting paper bankroll in USD (default: 50)")
    parser.add_argument(
        "--clip-usd",
        type=float,
        default=DEFAULT_CLIP_USD,
        help="$ per new leg — also the max unhedged exposure at any moment",
    )
    parser.add_argument(
        "--entry-price",
        type=float,
        default=DEFAULT_ENTRY_PRICE,
        help="Open a new leg only when a side's best ask is at or below this",
    )
    parser.add_argument(
        "--min-margin",
        type=float,
        default=DEFAULT_MIN_MARGIN,
        help="Hedge only if fee-inclusive pair cost stays <= 1 - this",
    )
    parser.add_argument(
        "--max-window-usd", type=float, default=DEFAULT_MAX_WINDOW_USD, help="Stop opening new legs past this $/window"
    )
    parser.add_argument(
        "--min-seconds-remaining",
        type=float,
        default=DEFAULT_MIN_SECONDS_REMAINING,
        help="Don't open a new leg this close to the window's end (hedges still allowed)",
    )
    parser.add_argument("--fee-rate", type=float, default=FEE_RATE_CRYPTO)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument("--perf-log-dir", default=str(DEFAULT_LOG_DIR), help="signals/trades/executions CSVs")
    parser.add_argument("--pid-file", default=str(DEFAULT_PID_PATH))
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return

    pidfile.claim_or_exit(Path(args.pid_file))
    trader = PairCostPaperTrader(
        Path(args.state_file),
        Path(args.trades_log),
        args.bankroll,
        tracker=performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir),
        clip_usd=args.clip_usd,
        entry_price=args.entry_price,
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

#!/usr/bin/env python3
"""
Copy trading (paper): follows one Polymarket account and mirrors its trades
into a virtual bankroll, scaled down by --ratio. Works on any market the
account trades, not just the BTC 5-minute series.

How it follows: Polymarket's public data API lists every account's fills
(data-api.polymarket.com/activity?user=<wallet>&type=TRADE), no key needed.
This polls it every --poll-interval seconds and copies only fills that happen
AFTER this process started — history is never replayed, and fills missed
while the bot was down are skipped (their prices are long gone).

What a copy actually costs is the point of paper-trading this, so fills are
NOT booked at the leader's price. A copied BUY fills at the token's live best
ask at the moment we see the trade, plus the taker fee, and is skipped if
that ask is more than --max-slippage above what the leader paid. The data
API itself lags the chain by seconds to a minute, which on fast markets is
often the whole edge — the "skipped" counter on the dashboard measures that.

Sizing: our shares = leader's shares * --ratio. Leaders like gabagool22 work
in hundreds of sub-$5 fills, so scaled fills are accumulated per token until
they are worth MIN_FILL_USD (Polymarket's $1 marketable-order minimum) and
then filled as one order. --max-market-usd caps total spend per market.
SELLs are mirrored at the live best bid, up to what we hold.

Settlement: Gamma's outcomePrices for the market's slug (closed=true), paid
per share on the outcome index we hold. Same source as
btc_5m_market_finder.resolve_market_outcome, but that helper only understands
Up/Down markets, so the generic lookup lives here.

Fees use the crypto-category taker rate (0.07, see gabagool_strategy.py) for
every market. Other categories charge less or nothing — pass --fee-rate if
the account you follow trades those.

Logs go to log_collector/copy_trading/ in the same files as every other bot (see
log_collector/README.md). events.jsonl has one BUY/SELL/SKIP line per copy decision, with
lag_s and our price vs the leader's.

State file uses the shape web_dashboard.py's shared panel/CSV/reset code
reads, the same as pair_cost_paper_trader.py.

Usage (run from anywhere — paths resolve relative to this script):
    python paper_trading/copy_paper_trader.py --selftest
    python paper_trading/copy_paper_trader.py                       # follows gabagool22
    python paper_trading/copy_paper_trader.py --follow https://polymarket.com/@someone --ratio 0.1
    python paper_trading/copy_paper_trader.py --follow 0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d
"""

import argparse
import json
import re
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import btc_5m_market_finder as finder
import performance_tracker
import pidfile
from gabagool_strategy import FEE_RATE_CRYPTO, fee_for_fill

DATA_API = "https://data-api.polymarket.com"

DEFAULT_STATE_PATH = SCRIPT_DIR / "copy_state.json"
DEFAULT_LOG_DIR = performance_tracker.LOG_ROOT / "copy_trading"  # one folder per bot — see log_collector/README.md
DEFAULT_TRADES_LOG_PATH = DEFAULT_LOG_DIR / "events.jsonl"
DEFAULT_PID_PATH = SCRIPT_DIR / "copy_paper_trader.pid"

DEFAULT_FOLLOW = "gabagool22"
DEFAULT_RATIO = 0.05
DEFAULT_MAX_MARKET_USD = 10.0
DEFAULT_MAX_SLIPPAGE = 0.02

MIN_FILL_USD = 1.0
SETTLE_CHECK_SECONDS = 30.0


def resolve_wallet(session: requests.Session, who: str) -> tuple[str, str]:
    """Accepts a 0x wallet, a username, or a profile URL. Returns (wallet, label)."""
    who = who.strip().split("?")[0].rstrip("/").rsplit("/", 1)[-1].lstrip("@")
    if re.fullmatch(r"0x[0-9a-fA-F]{40}", who):
        return who.lower(), who[:10]
    resp = session.get(f"{finder.GAMMA_API}/public-search", params={"q": who, "search_profiles": "true"}, timeout=10)
    resp.raise_for_status()
    for p in resp.json().get("profiles") or []:
        # exact match only: the search also returns look-alike names ("gabagool22-2th", ...)
        if (p.get("name") or "").lower() == who.lower() and p.get("proxyWallet"):
            return p["proxyWallet"].lower(), p["name"]
    sys.exit(f"no Polymarket profile named exactly {who!r} — pass its 0x wallet address instead")


def fetch_trades(session: requests.Session, wallet: str) -> list:
    """The account's most recent fills, newest first."""
    resp = session.get(f"{DATA_API}/activity", params={"user": wallet, "type": "TRADE", "limit": 100}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_resolution(session: requests.Session, slug: str) -> list | None:
    """Per-outcome payout ($/share) once the market has resolved, else None."""
    resp = session.get(f"{finder.GAMMA_API}/markets", params={"slug": slug, "closed": "true"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    try:
        prices = [float(p) for p in json.loads(data[0].get("outcomePrices") or "[]")]
    except (json.JSONDecodeError, ValueError):
        return None
    # ponytail: a 50/50 resolution never reaches 0.99 and would stay open; handle if one ever shows up
    return prices if prices and max(prices) >= 0.99 else None


def _trade_key(t: dict) -> tuple:
    return (t.get("transactionHash"), t.get("asset"), t.get("side"), t.get("size"), t.get("price"))


def _new_stats() -> dict:
    return {"trades": 0, "wins": 0, "losses": 0, "win_rate": None, "total_staked": 0.0, "realized_pnl": 0.0}


class CopyPaperTrader:
    def __init__(
        self,
        state_path: Path,
        trades_log_path: Path,
        starting_bankroll: float,
        ratio: float = DEFAULT_RATIO,
        max_market_usd: float = DEFAULT_MAX_MARKET_USD,
        max_slippage: float = DEFAULT_MAX_SLIPPAGE,
        fee_rate: float = FEE_RATE_CRYPTO,
        tracker=None,
    ):
        self.tracker = tracker  # performance_tracker.PerformanceTracker, or None (selftest/ad-hoc use)
        self.state_path = state_path
        self.trades_log_path = trades_log_path
        self.ratio = ratio
        self.max_market_usd = max_market_usd
        self.max_slippage = max_slippage
        self.fee_rate = fee_rate
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "copy-paper-trader/1.0"})

        self.starting_bankroll = starting_bankroll
        self.cash = starting_bankroll
        self.positions: dict = {}  # token id -> position dict
        self.recent_closed: list = []
        self.stats = _new_stats()

        self.started_at = time.time()
        self.seen: set = set()
        self.pending: dict = {}  # token id -> scaled BUY shares not yet worth MIN_FILL_USD
        self.copied = self.skipped = 0
        self.leader_last_trade_ts: float | None = None
        self.follow_label = ""
        self._next_settle_check = 0.0

        try:
            data = json.loads(state_path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        self.starting_bankroll = data["starting_bankroll"]
        self.cash = data["cash"]
        self.positions = {p["asset"]: p for p in data.get("open_positions", [])}
        self.recent_closed = data.get("recent_closed", [])
        self.stats = data.get("stats", _new_stats())
        print(f"resumed copy state: cash=${self.cash:.2f}, {len(self.positions)} open", file=sys.stderr)

    def best_bid_ask(self, asset: str) -> tuple[float | None, float | None]:
        # max/min rather than [0]: /book's asks come back worst-first (see CLAUDE.md)
        book = finder.fetch_order_book(self.session, asset)
        bid = max((float(b["price"]) for b in book.get("bids") or []), default=None)
        ask = min((float(a["price"]) for a in book.get("asks") or []), default=None)
        return bid, ask

    def on_leader_trades(self, page: list) -> None:
        """page = fetch_trades() output. Copies the fills not seen before."""
        if page:
            self.leader_last_trade_ts = max(t["timestamp"] for t in page)
        fresh = [t for t in page if t["timestamp"] >= self.started_at and _trade_key(t) not in self.seen]
        self.seen = {_trade_key(t) for t in page}

        grouped: dict = {}  # (asset, side) -> [leader shares, leader usd, sample trade]
        for t in fresh:
            g = grouped.setdefault((t["asset"], t["side"]), [0.0, 0.0, t])
            g[0] += float(t["size"])
            g[1] += float(t["size"]) * float(t["price"])
        for (asset, side), (shares, usd, sample) in grouped.items():
            (self._buy if side == "BUY" else self._sell)(asset, shares * self.ratio, usd / shares, sample)

    def _event(self, event: str, t: dict, leader_price: float, **fields) -> None:
        """One events.jsonl line per copy decision. lag_s (how old the leader's
        fill was when we acted) and our price vs theirs are what say whether
        copying this account is viable at all."""
        performance_tracker.append_jsonl(
            self.trades_log_path,
            {
                "event": event,
                "market_slug": f"{t['slug']}:{t.get('outcome')}",
                "side": t["side"],
                "leader_price": leader_price,
                "lag_s": time.time() - t["timestamp"],
                **fields,
            },
        )

    def _skip(self, why: str, t: dict) -> None:
        self.skipped += 1
        print(f"skip {t['side']} {t['slug']} {t.get('outcome')}: {why}", file=sys.stderr)

    def _buy(self, asset: str, shares: float, leader_price: float, t: dict) -> None:
        want = self.pending.pop(asset, 0.0) + shares
        if want * leader_price < MIN_FILL_USD:
            self.pending[asset] = want  # too small to be an order yet; keep accumulating
            return
        _, ask = self.best_bid_ask(asset)
        if ask is None or ask > leader_price + self.max_slippage:
            self._event("SKIP", t, leader_price, reason="slippage", our_price=ask)
            return self._skip(f"ask {ask} vs leader {leader_price:.3f} is past --max-slippage", t)

        unit = ask + fee_for_fill(ask, 1.0, self.fee_rate)
        in_market = sum(p["cost"] for p in self.positions.values() if p["slug"] == t["slug"])
        # ponytail: assumes the best ask level is deep enough; walk the book if --ratio fills get large
        shares = min(want, min(self.cash, self.max_market_usd - in_market) / unit)
        if shares * unit < MIN_FILL_USD:
            self._event("SKIP", t, leader_price, reason="cash_or_market_cap", our_price=ask)
            return self._skip("out of cash or at --max-market-usd", t)

        pos = self.positions.setdefault(
            asset,
            {
                # dashboard history is keyed on market_slug, and a leader often holds both outcomes of one market
                "market_slug": f"{t['slug']}:{t.get('outcome')}",
                "slug": t["slug"],
                "asset": asset,
                "side": t.get("outcome") or str(t.get("outcomeIndex")),
                "outcome_index": t["outcomeIndex"],
                "shares": 0.0,
                "bought": 0.0,
                "cost": 0.0,  # fee-inclusive
                "proceeds": 0.0,  # from SELLs, net of fee
                "opened_at": time.time(),
            },
        )
        pos["shares"] += shares
        pos["bought"] += shares
        pos["cost"] += shares * unit
        self.cash -= shares * unit
        self.copied += 1
        self._event("BUY", t, leader_price, our_price=ask, shares=shares, cost_usd=shares * unit)
        print(
            f"COPY BUY {pos['market_slug']} {shares:.2f} @ {ask:.3f} (leader {leader_price:.3f}) ${shares * unit:.2f}"
        )

    def _sell(self, asset: str, shares: float, leader_price: float, t: dict) -> None:
        self.pending.pop(asset, None)
        pos = self.positions.get(asset)
        if pos is None:
            return  # leader is selling something we never copied
        bid, _ = self.best_bid_ask(asset)
        if bid is None or bid < leader_price - self.max_slippage:
            self._event("SKIP", t, leader_price, reason="slippage", our_price=bid)
            return self._skip(f"bid {bid} vs leader {leader_price:.3f} is past --max-slippage", t)
        shares = min(shares, pos["shares"])
        net = shares * bid - fee_for_fill(bid, shares, self.fee_rate)
        pos["shares"] -= shares
        pos["proceeds"] += net
        self.cash += net
        self.copied += 1
        self._event("SELL", t, leader_price, our_price=bid, shares=shares, proceeds_usd=net)
        print(f"COPY SELL {pos['market_slug']} {shares:.2f} @ {bid:.3f} (leader {leader_price:.3f}) ${net:.2f}")
        if pos["shares"] < 1e-6:
            self._close(pos, 0.0, "sold")

    def _close(self, pos: dict, payout: float, source: str) -> None:
        pnl = pos["proceeds"] + payout - pos["cost"]
        self.cash += payout
        closed = {
            **self._display(pos),
            "status": "WON" if pnl >= 0 else "LOST",
            "pnl_usd": pnl,
            "resolution_source": source,
            "settled_at": time.time(),
        }
        performance_tracker.append_jsonl(self.trades_log_path, {"event": "CLOSE", **closed})
        if self.tracker is not None:
            # Same trades.csv columns as every other bot; copy-specific detail is in events.jsonl.
            self.tracker.log_trade(
                market_slug=pos["market_slug"],
                side=pos["side"],
                entry_price=closed["entry_price"],
                stake_usd=pos["cost"],
                shares=pos["bought"],
                bankroll_after=self.cash,
                entry_ts=pos["opened_at"],
                resolve_ts=closed["settled_at"],
                resolve_outcome="" if source == "sold" else ("WIN" if payout > 0 else "LOSE"),
                resolve_source=source,
                status=closed["status"],
                pnl_usd=pnl,
            )
        s = self.stats
        s["trades"] += 1
        s["wins" if pnl >= 0 else "losses"] += 1
        s["win_rate"] = s["wins"] / s["trades"]
        s["total_staked"] += pos["cost"]
        s["realized_pnl"] += pnl
        self.recent_closed = (self.recent_closed + [closed])[-25:]
        del self.positions[pos["asset"]]
        print(f"CLOSED {pos['market_slug']} ({source}) cost=${pos['cost']:.2f} pnl=${pnl:+.2f}")

    def check_settlements(self) -> None:
        if time.time() < self._next_settle_check:
            return
        self._next_settle_check = time.time() + SETTLE_CHECK_SECONDS
        for slug in {p["slug"] for p in self.positions.values()}:
            payouts = fetch_resolution(self.session, slug)
            if payouts is None:
                continue
            for pos in [p for p in self.positions.values() if p["slug"] == slug]:
                self._close(pos, pos["shares"] * payouts[pos["outcome_index"]], "gamma")

    @staticmethod
    def _display(pos: dict) -> dict:
        """Adds the fields the dashboard's shared panel/CSV code reads."""
        return {**pos, "entry_price": pos["cost"] / pos["bought"], "stake_usd": pos["cost"]}

    def save_state(self) -> None:
        equity = self.cash + sum(p["cost"] - p["proceeds"] for p in self.positions.values())
        last = self.leader_last_trade_ts
        snapshot = {
            "starting_bankroll": self.starting_bankroll,
            "cash": self.cash,
            "equity": equity,
            "engine": {
                "rows": [
                    ["Following", self.follow_label],
                    [
                        "Their last trade",
                        datetime.fromtimestamp(last, UTC).strftime("%Y-%m-%d %H:%M UTC") if last else "none found",
                    ],
                    ["Copied / skipped (this run)", f"{self.copied} / {self.skipped}"],
                ]
            },
            "open_positions": [self._display(p) for p in self.positions.values()],
            "recent_closed": self.recent_closed,
            "stats": {**self.stats, "return_pct": equity / self.starting_bankroll - 1.0},
            "updated_at": time.time(),
        }
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2))
        tmp.replace(self.state_path)


def run_selftest():
    """Synthetic, no-network check of the copy/accumulate/skip/settle accounting."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        t = CopyPaperTrader(Path(d) / "s.json", Path(d) / "t.jsonl", 50.0, ratio=0.1, fee_rate=0.0)
        book = {"A": (0.49, 0.50)}
        t.best_bid_ask = lambda asset: book[asset]
        now = time.time() + 1

        def trade(side, size, price, tx, ts=now):
            return {"transactionHash": tx, "asset": "A", "side": side, "size": size, "price": price, "timestamp": ts,
                    "slug": "m", "outcome": "Up", "outcomeIndex": 0}  # fmt: skip

        # History from before startup is never replayed.
        t.on_leader_trades([trade("BUY", 1000, 0.50, "old", ts=t.started_at - 60)])
        assert not t.positions and t.cash == 50.0
        # 10 leader shares * 0.1 = 1 share = $0.50: below the $1 minimum, so it waits...
        page = [trade("BUY", 10, 0.50, "tx1")]
        t.on_leader_trades(page)
        assert not t.positions and t.pending["A"] == 1.0
        # ...the same page again copies nothing twice...
        t.on_leader_trades(page)
        assert t.pending["A"] == 1.0
        # ...and the next fill tips it over: 1 + 3 = 4 shares at our ask.
        t.on_leader_trades([trade("BUY", 30, 0.50, "tx2"), *page])
        assert abs(t.positions["A"]["shares"] - 4.0) < 1e-9 and abs(t.cash - 48.0) < 1e-9
        # Ask ran away from the leader's price -> skipped, not chased.
        book["A"] = (0.59, 0.60)
        t.on_leader_trades([trade("BUY", 100, 0.50, "tx3")])
        assert t.skipped == 1 and abs(t.positions["A"]["shares"] - 4.0) < 1e-9
        # --max-market-usd caps one market: a huge leader buy only fills up to $10 total.
        book["A"] = (0.49, 0.50)
        t.on_leader_trades([trade("BUY", 10000, 0.50, "tx4")])
        assert abs(t.positions["A"]["cost"] - 10.0) < 1e-9
        # Leader sells half -> we sell the scaled amount at our bid.
        t.on_leader_trades([trade("SELL", 100, 0.49, "tx5")])
        assert abs(t.positions["A"]["shares"] - 10.0) < 1e-9 and abs(t.cash - 44.9) < 1e-9
        # Market resolves our way: 10 shares pay $10; pnl = 4.90 + 10 - 10.
        t._close(t.positions["A"], 10.0 * 1.0, "gamma")
        assert not t.positions and abs(t.cash - 54.9) < 1e-9 and abs(t.stats["realized_pnl"] - 4.9) < 1e-9
        t.save_state()
    print("All self-test assertions passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--selftest", action="store_true", help="Synthetic, no-network check, then exit")
    parser.add_argument("--follow", default=DEFAULT_FOLLOW, help="Account to copy: username, profile URL, or 0x wallet")
    parser.add_argument("--bankroll", type=float, default=50.0, help="Starting paper bankroll in USD (default: 50)")
    parser.add_argument("--ratio", type=float, default=DEFAULT_RATIO, help="Our shares per leader share")
    parser.add_argument("--max-market-usd", type=float, default=DEFAULT_MAX_MARKET_USD, help="Spend cap per market")
    parser.add_argument(
        "--max-slippage",
        type=float,
        default=DEFAULT_MAX_SLIPPAGE,
        help="Skip a copy if the live price is this much worse than the leader's",
    )
    parser.add_argument("--fee-rate", type=float, default=FEE_RATE_CRYPTO)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(DEFAULT_TRADES_LOG_PATH))
    parser.add_argument(
        "--perf-log-dir", default=str(DEFAULT_LOG_DIR), help="Folder for signals.csv/trades.csv/runs.jsonl"
    )
    parser.add_argument("--pid-file", default=str(DEFAULT_PID_PATH))
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        return

    pidfile.claim_or_exit(Path(args.pid_file))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # so the dashboard's Stop runs pidfile cleanup

    trader = CopyPaperTrader(
        Path(args.state_file),
        Path(args.trades_log),
        args.bankroll,
        ratio=args.ratio,
        max_market_usd=args.max_market_usd,
        max_slippage=args.max_slippage,
        fee_rate=args.fee_rate,
        tracker=performance_tracker.PerformanceTracker(log_dir=args.perf_log_dir),
    )
    wallet, trader.follow_label = resolve_wallet(trader.session, args.follow)
    print(f"following {trader.follow_label} ({wallet}) at ratio {args.ratio}", file=sys.stderr)

    try:
        while True:
            try:
                trader.on_leader_trades(fetch_trades(trader.session, wallet))
                trader.check_settlements()
                trader.save_state()
            except (requests.RequestException, ValueError, KeyError) as e:
                print(f"tick error: {e}", file=sys.stderr)
            time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

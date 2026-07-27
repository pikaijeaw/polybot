#!/usr/bin/env python3
"""
Discover Polymarket's 15-minute BTC Up/Down markets.

Same series pattern as this project's 5-minute finder (btc_5m_market_finder.py),
just on a different recurrence: these are the "btc-up-or-down-15m" series,
slugs look like btc-updown-15m-<epoch> where <epoch> is the unix timestamp of
the market's close/resolution time, always a multiple of 900. Confirmed live
against Gamma directly (GET /markets?slug=btc-updown-15m-<computed epoch>
returns a real market with series.slug == "btc-up-or-down-15m") rather than
assumed from the 5m naming convention.

This is a deliberately separate file rather than a parameterized version of
btc_5m_market_finder.py, same reasoning as trend_strategy.py living alongside
oracle_lag_strategy.py rather than as a flag on it. It does, though, reuse
resolve_market_outcome() and fetch_order_book() from btc_5m_market_finder.py
by import rather than copy-pasting: both are already generic over
`slug`/`token_id`, and this repo's convention (see CLAUDE.md) is that
settlement-source logic in particular must not fork across strategies.

No API key required — this only reads public market/orderbook data.

Usage:
    python btc_15m_market_finder.py                 # show current + upcoming
    python btc_15m_market_finder.py --watch          # refresh every 5s
    python btc_15m_market_finder.py --book           # include CLOB order book depth
    python btc_15m_market_finder.py --json           # machine-readable output
"""

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import requests

from btc_5m_market_finder import fetch_order_book, resolve_market_outcome

GAMMA_API = "https://gamma-api.polymarket.com"
SLUG_RE = re.compile(r"^btc-updown-15m-(\d+)$")
WINDOW_SECONDS = 900

__all__ = ["Market", "fetch_btc_15m_markets", "fetch_order_book", "resolve_market_outcome", "WINDOW_SECONDS"]


@dataclass
class Market:
    market_id: str
    slug: str
    question: str
    condition_id: str
    end_date: str
    start_date: str
    token_up: str
    token_down: str
    best_bid: float | None
    best_ask: float | None
    accepting_orders: bool
    liquidity: float
    volume: float
    epoch: int = field(init=False)

    def __post_init__(self):
        self.epoch = int(SLUG_RE.match(self.slug).group(1))

    @property
    def status(self) -> str:
        now = time.time()
        if now < self.epoch - WINDOW_SECONDS:
            return "UPCOMING"
        if now < self.epoch:
            return "LIVE"
        return "CLOSED"

    @property
    def seconds_to_close(self) -> float:
        return self.epoch - time.time()


def _parse_market(m: dict) -> Market | None:
    if not SLUG_RE.match(m.get("slug", "")):
        return None
    try:
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        outcomes = json.loads(m.get("outcomes") or "[]")
    except json.JSONDecodeError:
        return None
    if len(token_ids) != 2 or len(outcomes) != 2:
        return None

    by_outcome = dict(zip(outcomes, token_ids, strict=True))
    if "Up" not in by_outcome or "Down" not in by_outcome:
        return None

    return Market(
        market_id=m["id"],
        slug=m["slug"],
        question=m["question"],
        condition_id=m["conditionId"],
        end_date=m.get("endDate", ""),
        start_date=m.get("startDate", ""),
        token_up=by_outcome["Up"],
        token_down=by_outcome["Down"],
        best_bid=_to_float(m.get("bestBid")),
        best_ask=_to_float(m.get("bestAsk")),
        accepting_orders=bool(m.get("acceptingOrders")),
        liquidity=_to_float(m.get("liquidityNum") or m.get("liquidity")) or 0.0,
        volume=_to_float(m.get("volumeNum") or m.get("volume")) or 0.0,
    )


def fetch_btc_15m_markets(session: requests.Session, lookback: int = 1, lookahead: int = 2) -> list[Market]:
    """Looks up the current 15-minute window plus `lookback` windows behind it
    and `lookahead` windows ahead of it, by computing each one's slug
    directly from wall-clock time — same reasoning as
    btc_5m_market_finder.py's fetch_btc_5m_markets (Gamma's creation-time
    ordering surfaces tomorrow's pre-created windows, not today's live one)."""
    now = time.time()
    live_close_epoch = ((int(now) // WINDOW_SECONDS) + 1) * WINDOW_SECONDS
    epochs = [live_close_epoch + i * WINDOW_SECONDS for i in range(-lookback, lookahead + 1)]

    markets = []
    for epoch in epochs:
        slug = f"btc-updown-15m-{epoch}"
        resp = session.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            continue  # window not created yet (or already archived) — skip
        parsed = _parse_market(data[0])
        if parsed is not None:
            markets.append(parsed)

    markets.sort(key=lambda x: x.epoch)
    return markets


def _to_float(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def format_market(m: Market, show_book: bool, session: requests.Session) -> str:
    lines = [
        f"[{m.status:8s}] {m.question}",
        f"  slug={m.slug}  closes_in={m.seconds_to_close:+.0f}s  end={m.end_date}",
        f"  condition_id={m.condition_id}",
        f"  Up   token={m.token_up}",
        f"  Down token={m.token_down}",
        f"  best_bid={m.best_bid}  best_ask={m.best_ask}"
        f"  liquidity={m.liquidity:.2f}  volume={m.volume:.2f}"
        f"  accepting_orders={m.accepting_orders}",
    ]
    if show_book:
        for label, token in (("Up", m.token_up), ("Down", m.token_down)):
            try:
                book = fetch_order_book(session, token)
                asks = sorted(book.get("asks", []), key=lambda a: float(a["price"]))[:3]
                bids = sorted(book.get("bids", []), key=lambda b: -float(b["price"]))[:3]
                lines.append(f"  {label} book  bids(top3)={bids}  asks(top3)={asks}")
            except requests.RequestException as e:
                lines.append(f"  {label} book  error={e}")
    return "\n".join(lines)


def run_once(args, session: requests.Session):
    markets = fetch_btc_15m_markets(session)

    if args.json:
        payload = [
            {
                "slug": m.slug,
                "question": m.question,
                "status": m.status,
                "condition_id": m.condition_id,
                "start_date": m.start_date,
                "end_date": m.end_date,
                "seconds_to_close": m.seconds_to_close,
                "token_up": m.token_up,
                "token_down": m.token_down,
                "best_bid": m.best_bid,
                "best_ask": m.best_ask,
                "liquidity": m.liquidity,
                "volume": m.volume,
                "accepting_orders": m.accepting_orders,
            }
            for m in markets
        ]
        print(json.dumps(payload, indent=2))
        return

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"=== BTC 15m Up/Down markets @ {now} ===")
    if not markets:
        print("No open btc-updown-15m markets found right now.")
        return
    for m in markets:
        print(format_market(m, args.book, session))
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--watch", action="store_true", help="Keep polling and refresh the listing")
    parser.add_argument(
        "--interval", type=float, default=5.0, help="Seconds between refreshes with --watch (default: 5)"
    )
    parser.add_argument(
        "--book", action="store_true", help="Also fetch top-of-book depth from the CLOB for each market"
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of a text report")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "btc-15m-market-finder/1.0"})

    try:
        if args.watch:
            while True:
                run_once(args, session)
                time.sleep(args.interval)
        else:
            run_once(args, session)
    except KeyboardInterrupt:
        sys.exit(0)
    except requests.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

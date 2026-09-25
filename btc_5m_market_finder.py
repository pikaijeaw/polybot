#!/usr/bin/env python3
"""
Discover Polymarket's 5-minute BTC Up/Down markets.

These are the recurring "Bitcoin Up or Down - <time range>" markets on the
"btc-up-or-down-5m" series (slugs look like btc-updown-5m-<epoch>, where
<epoch> is the unix timestamp of the market's close/resolution time, always
a multiple of 300). Windows are contiguous and 5-minutes-aligned, so instead
of asking Gamma to list "recently active" markets, this computes the
handful of epochs that should exist around the current wall-clock time and
looks each one up directly by slug.

That's deliberate, not just an optimization: Gamma's own "most recently
created" ordering is unreliable here. Polymarket appears to batch pre-create
a full day's worth of future 5-minute windows in advance, so sorting by
creation time can surface tomorrow's just-created placeholder windows
instead of today's genuinely-about-to-close one (which was created ~24h
earlier and so looks "old" by that sort even though it's the one that's
actually live). Direct slug lookups sidestep that entirely. This also means
we don't rely on Gamma's `closed` flag, which lags behind actual resolution
for these fast-cycling markets — status is derived from the epoch vs. wall
clock instead.

No API key required — this only reads public market/orderbook data.

Usage:
    python btc_5m_market_finder.py                 # show current + upcoming
    python btc_5m_market_finder.py --watch          # refresh every 5s
    python btc_5m_market_finder.py --book           # include CLOB order book depth
    python btc_5m_market_finder.py --json           # machine-readable output
"""

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import requests

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
SLUG_RE = re.compile(r"^btc-updown-5m-(\d+)$")
WINDOW_SECONDS = 300


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
        if now < self.epoch - 300:
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


def fetch_btc_5m_markets(session: requests.Session, lookback: int = 1, lookahead: int = 2) -> list[Market]:
    """Looks up the current 5-minute window plus `lookback` windows behind it
    and `lookahead` windows ahead of it, by computing each one's slug
    directly from wall-clock time (see module docstring for why — Gamma's
    creation-time ordering can't be trusted to surface the right one)."""
    now = time.time()
    live_close_epoch = ((int(now) // WINDOW_SECONDS) + 1) * WINDOW_SECONDS
    epochs = [live_close_epoch + i * WINDOW_SECONDS for i in range(-lookback, lookahead + 1)]

    markets = []
    for epoch in epochs:
        slug = f"btc-updown-5m-{epoch}"
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


def fetch_order_book(session: requests.Session, token_id: str) -> dict:
    resp = session.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_best_ask(session: requests.Session, token_id: str) -> float | None:
    """Lowest ask on the live CLOB book — what a buy would actually pay right
    now. Gamma's bestAsk lags the real book, so paper fills use this instead.
    min() rather than asks[0]: /book returns asks sorted descending."""
    asks = fetch_order_book(session, token_id).get("asks") or []
    return min((float(a["price"]) for a in asks), default=None)


def resolve_market_outcome(session: requests.Session, slug: str) -> str | None:
    """Looks up whether a market has resolved and, if so, which side won, by
    reading Gamma's own outcomePrices for it. Returns "UP", "DOWN", or None
    if not resolved yet (or not found). Shared by every paper
    bot so they all settle against the exact same logic.

    Passing closed=true is required, not optional: Gamma's default
    /markets?slug= query only serves markets that are still active and
    drops a slug entirely within ~10 minutes of its close, well before
    SETTLE_FALLBACK_AFTER would even trigger the callers' price-based
    fallback. Without this filter, this function returns None for every
    market, every time, silently forcing 100% of settlements onto the
    fallback path instead of Polymarket's real Chainlink-based resolution."""
    resp = session.get(f"{GAMMA_API}/markets", params={"slug": slug, "closed": "true"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        return None
    m = data[0]
    try:
        outcomes = json.loads(m.get("outcomes") or "[]")
        prices = [float(p) for p in json.loads(m.get("outcomePrices") or "[]")]
    except (json.JSONDecodeError, ValueError):
        return None
    if len(outcomes) != 2 or len(prices) != 2:
        return None
    by_outcome = dict(zip(outcomes, prices, strict=True))
    up, down = by_outcome.get("Up"), by_outcome.get("Down")
    if up is None or down is None or up == down:
        return None
    return "UP" if up > down else "DOWN"


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
                top_bids = book.get("bids", [])[-3:][::-1]
                top_asks = book.get("asks", [])[:3]
                lines.append(f"  {label} book  bids(top3)={top_bids}  asks(top3)={top_asks}")
            except requests.RequestException as e:
                lines.append(f"  {label} book  error={e}")
    return "\n".join(lines)


def run_once(args, session: requests.Session):
    markets = fetch_btc_5m_markets(session)

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
    print(f"=== BTC 5m Up/Down markets @ {now} ===")
    if not markets:
        print("No open btc-updown-5m markets found right now.")
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
    session.headers.update({"User-Agent": "btc-5m-market-finder/1.0"})

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

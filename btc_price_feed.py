#!/usr/bin/env python3
"""
Real-time BTC price feed from the Binance WebSocket market-data stream.

Connects to Binance's public combined stream (no API key needed) and prints
each update as it arrives. Auto-reconnects with exponential backoff on any
drop, since these streams periodically get closed server-side (Binance
recycles connections roughly every 24h and on network hiccups).

Streams supported (see --stream):
    trade       Every executed trade (price, quantity, buyer/seller flag).
    bookTicker  Best bid/ask price+size, updated on every order book change.
    kline       OHLCV candle updates for a given interval (default 1m).

If the websocket connection can't be established or drops repeatedly, the
feed falls back to polling Binance's REST ticker endpoint
(GET /api/v3/ticker/price) for a plain last-price update, while it keeps
retrying the websocket in the background. It switches back to the websocket
automatically once that reconnects.

Usage:
    python btc_price_feed.py                      # trade stream, human-readable
    python btc_price_feed.py --stream bookTicker
    python btc_price_feed.py --stream kline --interval 1m
    python btc_price_feed.py --symbol ethusdt
    python btc_price_feed.py --json                # newline-delimited JSON
    python btc_price_feed.py --no-rest-fallback     # disable the REST fallback
"""

import argparse
import asyncio
import json
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

import aiohttp
import websockets

BASE_WS_URL = "wss://stream.binance.com:9443/ws"
REST_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"

MAX_BACKOFF = 30.0


@dataclass
class Tick:
    kind: str
    symbol: str
    event_time_ms: int
    data: dict

    @property
    def timestamp(self) -> str:
        return datetime.fromtimestamp(self.event_time_ms / 1000, tz=UTC).strftime("%H:%M:%S.%f")[:-3]


def parse_message(stream: str, raw: dict) -> Tick:
    if stream == "trade":
        return Tick(
            kind="trade",
            symbol=raw["s"],
            event_time_ms=raw["E"],
            data={
                "price": float(raw["p"]),
                "qty": float(raw["q"]),
                "buyer_is_maker": raw["m"],
            },
        )
    if stream == "bookTicker":
        return Tick(
            kind="bookTicker",
            symbol=raw["s"],
            event_time_ms=0,  # bookTicker payload has no event timestamp
            data={
                "bid": float(raw["b"]),
                "bid_qty": float(raw["B"]),
                "ask": float(raw["a"]),
                "ask_qty": float(raw["A"]),
            },
        )
    if stream == "kline":
        k = raw["k"]
        return Tick(
            kind="kline",
            symbol=raw["s"],
            event_time_ms=raw["E"],
            data={
                "interval": k["i"],
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "closed": k["x"],
            },
        )
    raise ValueError(f"unhandled stream type: {stream}")


def format_tick(tick: Tick) -> str:
    ts = tick.timestamp if tick.event_time_ms else datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    if tick.kind == "trade":
        d = tick.data
        side = "SELL" if d["buyer_is_maker"] else "BUY "
        return f"[{ts}] {tick.symbol} TRADE {side} price={d['price']:.2f} qty={d['qty']:.6f}"
    if tick.kind == "bookTicker":
        d = tick.data
        spread = d["ask"] - d["bid"]
        return f"[{ts}] {tick.symbol} BOOK  bid={d['bid']:.2f} ({d['bid_qty']:.4f})  ask={d['ask']:.2f} ({d['ask_qty']:.4f})  spread={spread:.2f}"
    if tick.kind == "kline":
        d = tick.data
        flag = "CLOSED" if d["closed"] else "open  "
        return f"[{ts}] {tick.symbol} KLINE({d['interval']},{flag}) O={d['open']:.2f} H={d['high']:.2f} L={d['low']:.2f} C={d['close']:.2f} V={d['volume']:.4f}"
    if tick.kind == "rest":
        d = tick.data
        return f"[{ts}] {tick.symbol} REST  price={d['price']:.2f}  (fallback poll, websocket down)"
    return str(tick)


async def rest_poll_loop(symbol: str, poll_interval: float, on_tick):
    """Poll the REST last-price endpoint until cancelled. Used as a fallback
    while the websocket is down."""
    params = {"symbol": symbol.upper()}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(REST_TICKER_URL, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                tick = Tick(kind="rest", symbol=data["symbol"], event_time_ms=0, data={"price": float(data["price"])})
                on_tick(tick)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"REST fallback poll error: {e}", file=sys.stderr)
            await asyncio.sleep(poll_interval)


async def stream_prices(
    symbol: str,
    stream: str,
    interval: str,
    on_tick,
    rest_fallback: bool = True,
    rest_fallback_after: int = 2,
    rest_poll_interval: float = 2.0,
):
    stream_name = f"{symbol}@{stream}" if stream != "kline" else f"{symbol}@kline_{interval}"
    url = f"{BASE_WS_URL}/{stream_name}"

    backoff = 1.0
    consecutive_failures = 0
    fallback_task: asyncio.Task | None = None

    def start_fallback():
        nonlocal fallback_task
        if rest_fallback and fallback_task is None:
            print(
                f"websocket down after {consecutive_failures} attempt(s); starting REST fallback polling",
                file=sys.stderr,
            )
            fallback_task = asyncio.ensure_future(rest_poll_loop(symbol, rest_poll_interval, on_tick))

    async def stop_fallback():
        nonlocal fallback_task
        if fallback_task is not None:
            fallback_task.cancel()
            try:
                await fallback_task
            except asyncio.CancelledError:
                pass
            fallback_task = None
            print("websocket reconnected; stopping REST fallback polling", file=sys.stderr)

    try:
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    print(f"connected to {url}", file=sys.stderr)
                    await stop_fallback()
                    backoff = 1.0
                    consecutive_failures = 0
                    async for raw_msg in ws:
                        raw = json.loads(raw_msg)
                        tick = parse_message(stream, raw)
                        on_tick(tick)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                print(f"connection error ({e}); reconnecting in {backoff:.1f}s", file=sys.stderr)
                if rest_fallback and consecutive_failures >= rest_fallback_after:
                    start_fallback()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
    finally:
        await stop_fallback()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbol", default="btcusdt", help="Trading pair, lowercase (default: btcusdt)")
    parser.add_argument(
        "--stream",
        choices=["trade", "bookTicker", "kline"],
        default="trade",
        help="Which Binance stream to subscribe to (default: trade)",
    )
    parser.add_argument("--interval", default="1m", help="Kline interval, only used with --stream kline (default: 1m)")
    parser.add_argument(
        "--json", action="store_true", help="Print newline-delimited JSON instead of human-readable text"
    )
    parser.add_argument(
        "--no-rest-fallback",
        dest="rest_fallback",
        action="store_false",
        help="Disable the REST polling fallback when the websocket is down",
    )
    parser.add_argument(
        "--rest-fallback-after",
        type=int,
        default=2,
        help="Consecutive websocket failures before REST fallback polling kicks in (default: 2)",
    )
    parser.add_argument(
        "--rest-poll-interval", type=float, default=2.0, help="Seconds between REST fallback polls (default: 2.0)"
    )
    args = parser.parse_args()

    def on_tick(tick: Tick):
        if args.json:
            print(
                json.dumps(
                    {
                        "kind": tick.kind,
                        "symbol": tick.symbol,
                        "event_time_ms": tick.event_time_ms,
                        **tick.data,
                    }
                )
            )
        else:
            print(format_tick(tick))
        sys.stdout.flush()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(
        stream_prices(
            args.symbol,
            args.stream,
            args.interval,
            on_tick,
            rest_fallback=args.rest_fallback,
            rest_fallback_after=args.rest_fallback_after,
            rest_poll_interval=args.rest_poll_interval,
        )
    )

    def shutdown(*_):
        task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass  # not supported on this platform (e.g. Windows)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()

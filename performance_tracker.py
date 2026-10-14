#!/usr/bin/env python3
"""
Log writer for the paper bots. Each bot gets one folder under log_collector/
(see log_collector/README.md) holding:

    runs.jsonl       One line per process start: timestamp + the exact
                     command-line settings, so every row below can be tied
                     to the knobs that produced it.

    trades.csv       One row per trade, appended once it resolves — entry
                     and resolution in a single record, so there's no
                     in-flight row to reconcile later (open positions are
                     visible live in the bot's state file; this is the
                     post-trade blotter). Same columns for every bot.

    signals.csv      One row per poll: the prices the bot saw and whether it
                     bought. Answers "what was available, and what did we
                     miss?" after the fact.

    events.jsonl     Free-form event lines written by the bots themselves
                     through append_jsonl() below.

This module only defines the schemas and the writers; the bots in
paper_trading/ wire it in.

Usage as a library:
    from performance_tracker import PerformanceTracker
    tracker = PerformanceTracker(log_dir=LOG_ROOT / "my_bot")
    tracker.log_signal(market_slug=..., seconds_remaining=..., up_ask=..., down_ask=..., traded=False)
    tracker.log_trade(market_slug=..., status="WON", ...)

Run directly for a quick summary report over existing logs:
    python performance_tracker.py --report --log-dir log_collector/arbitrage_pair_cost
"""

import argparse
import csv
import json
import statistics
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
# The one folder every bot logs into, one sub-folder per bot — see log_collector/README.md.
LOG_ROOT = SCRIPT_DIR / "log_collector"

SIGNALS_COLUMNS = [
    "ts",
    "ts_iso",
    "market_slug",
    "seconds_remaining",
    "up_ask",
    "down_ask",
    "traded",
    "side",
    "stake_usd",
]

TRADES_COLUMNS = [
    "trade_id",
    "market_slug",
    "side",
    "entry_price",
    "stake_usd",
    "shares",
    "bankroll_after",
    "entry_ts",
    "entry_ts_iso",
    "resolve_ts",
    "resolve_ts_iso",
    "holding_seconds",
    "resolve_outcome",
    "resolve_source",
    "status",
    "pnl_usd",
    "pnl_bps",
]


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def append_jsonl(path, record: dict):
    """Appends one JSON line, stamped with ts/ts_iso — the one writer behind
    every bot's events.jsonl and runs.jsonl, so they all share those fields."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = time.time()
    with open(path, "a") as f:
        f.write(json.dumps({"ts": ts, "ts_iso": _iso(ts), **record}) + "\n")


class PerformanceTracker:
    """Thread-safe (a lock guards every append), so a bot's worker thread and
    its event loop can both call in."""

    def __init__(self, log_dir):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.signals_path = self.log_dir / "signals.csv"
        self.trades_path = self.log_dir / "trades.csv"
        self._lock = threading.Lock()
        self._init_file(self.signals_path, SIGNALS_COLUMNS)
        self._init_file(self.trades_path, TRADES_COLUMNS)
        # One line per process start: without the settings, nobody can tell
        # later which knobs produced which rows.
        append_jsonl(self.log_dir / "runs.jsonl", {"script": Path(sys.argv[0]).name, "argv": sys.argv[1:]})

    @staticmethod
    def _init_file(path: Path, columns: list):
        if not path.exists():
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(columns)

    def _append(self, path: Path, columns: list, row: dict):
        with self._lock:
            with open(path, "a", newline="") as f:
                csv.writer(f).writerow([row.get(c, "") for c in columns])

    def log_signal(self, *, market_slug, seconds_remaining, up_ask, down_ask, traded, side="", stake_usd=0.0):
        ts = time.time()
        self._append(
            self.signals_path,
            SIGNALS_COLUMNS,
            {
                "ts": ts,
                "ts_iso": _iso(ts),
                "market_slug": market_slug,
                "seconds_remaining": seconds_remaining,
                "up_ask": up_ask,
                "down_ask": down_ask,
                "traded": traded,
                "side": side,
                "stake_usd": stake_usd,
            },
        )

    def log_trade(
        self,
        *,
        market_slug,
        side,
        entry_price,
        stake_usd,
        shares,
        bankroll_after,
        entry_ts,
        resolve_ts,
        resolve_outcome,
        resolve_source,
        status,
        pnl_usd,
    ) -> str:
        """entry_price and stake_usd are fee-inclusive."""
        trade_id = str(uuid.uuid4())
        self._append(
            self.trades_path,
            TRADES_COLUMNS,
            {
                "trade_id": trade_id,
                "market_slug": market_slug,
                "side": side,
                "entry_price": entry_price,
                "stake_usd": stake_usd,
                "shares": shares,
                "bankroll_after": bankroll_after,
                "entry_ts": entry_ts,
                "entry_ts_iso": _iso(entry_ts),
                "resolve_ts": resolve_ts,
                "resolve_ts_iso": _iso(resolve_ts),
                "holding_seconds": f"{resolve_ts - entry_ts:.1f}",
                "resolve_outcome": resolve_outcome,
                "resolve_source": resolve_source,
                "status": status,
                "pnl_usd": f"{pnl_usd:.4f}",
                "pnl_bps": f"{(pnl_usd / stake_usd * 10000) if stake_usd else 0.0:.2f}",
            },
        )
        return trade_id


# ---------------------------------------------------------------------------
# Report (stdlib only)
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def print_report(log_dir: Path):
    signals = _read_csv(log_dir / "signals.csv")
    trades = _read_csv(log_dir / "trades.csv")

    print(f"=== PolyBot performance report: {log_dir} ===\n")

    print(f"--- signals.csv ({len(signals)} rows) ---")
    if signals:
        traded = [s for s in signals if s["traded"] == "True"]
        print(f"  polls: {len(signals)}   bought on: {len(traded)} ({len(traded) / len(signals) * 100:.1f}%)")
    else:
        print("  no data yet")
    print()

    print(f"--- trades.csv ({len(trades)} rows) ---")
    if trades:
        wins = [t for t in trades if t["status"] == "WON"]
        pnl = [float(t["pnl_usd"]) for t in trades if t.get("pnl_usd")]
        holding = [float(t["holding_seconds"]) for t in trades if t.get("holding_seconds")]
        n = len(trades)
        print(f"  trades: {n}   wins: {len(wins)}   losses: {n - len(wins)}   win_rate: {len(wins) / n * 100:.1f}%")
        if pnl:
            print(f"  total P&L: ${sum(pnl):+.2f}   avg P&L/trade: ${statistics.mean(pnl):+.4f}")
        if holding:
            print(f"  holding(s)   mean={statistics.mean(holding):.1f}   median={statistics.median(holding):.1f}")
    else:
        print("  no data yet")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--report", action="store_true", help="Print a summary report over existing logs and exit")
    parser.add_argument("--log-dir", required=True)
    args = parser.parse_args()

    if args.report:
        print_report(Path(args.log_dir))
    else:
        print(__doc__)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Quant-grade performance tracker for PolyBot — three append-only CSV logs.

    signals.csv     Every signal the strategy evaluates, traded or not.
                     Are our edges real? What are we missing?

    trades.csv       One row per trade, appended once it resolves — entry,
                     fill, exit and resolution all in a single record, so
                     there's no in-flight row to reconcile or update later
                     (open positions are already visible live via
                     paper_state.json; this is the post-trade blotter).
                     How well do we execute? Where does P&L leak?

    executions.csv   Every request/response API call, with latency. Not
                     websocket ticks (no per-message request latency to
                     measure there) — connect/reconnect events if anything.
                     How fast are we? Where's the latency?

This module only defines the schemas and a PerformanceTracker to append to
them — it doesn't generate any data on its own. paper_trader.py wires it in
to the live paper-trading loop.

One caveat worth knowing before reading signals.csv: it only tells you
whether an *evaluated* signal was real if the market it names also shows up
in trades.csv (untraded signals never get a recorded resolution, so their
calibration isn't directly measurable from these logs alone).

Usage as a library:
    from performance_tracker import PerformanceTracker
    tracker = PerformanceTracker(log_dir="perf_logs")
    tracker.log_signal(market_slug=..., traded=False, skip_reason="below_min_edge", ...)
    tracker.log_trade(market_slug=..., status="WON", ...)
    with tracker.time_execution(api="gamma_markets", endpoint="/markets") as ex:
        resp = session.get(...)
        ex.http_status = resp.status_code

Run directly for a quick summary report over existing logs:
    python performance_tracker.py --report
    python performance_tracker.py --report --log-dir paper_trading/perf_logs
"""

import argparse
import csv
import statistics
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG_DIR = SCRIPT_DIR / "perf_logs"

SIGNALS_COLUMNS = [
    "ts",
    "ts_iso",
    "market_slug",
    "seconds_remaining",
    "last_price",
    "anchor_price",
    "anchor_is_approximate",
    "sigma_per_sqrt_sec",
    "p_up",
    "up_ask",
    "down_ask",
    "up_edge",
    "down_edge",
    "best_side",
    "best_edge",
    "kelly_fraction_applied",
    "stake_usd",
    "min_edge_threshold",
    "traded",
    "skip_reason",
]

TRADES_COLUMNS = [
    "trade_id",
    "market_slug",
    "side",
    "signal_ts",
    "signal_probability",
    "signal_edge",
    "kelly_fraction",
    "requested_price",
    "filled_price",
    "slippage_bps",
    "stake_usd",
    "shares",
    "fees_usd",
    "bankroll_before",
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

EXECUTIONS_COLUMNS = [
    "ts",
    "ts_iso",
    "api",
    "method",
    "endpoint",
    "context",
    "latency_ms",
    "status",
    "http_status",
    "error_message",
]


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class ExecutionHandle:
    """Mutable holder yielded by time_execution() so the caller can attach
    an http_status/context once the call completes, before the row is
    written on context-manager exit."""

    context: str = ""
    http_status: int | None = None


class PerformanceTracker:
    """Thread-safe (a lock guards every append) since paper_trader.py's
    asyncio loop and its Flask dashboard thread can both call in."""

    def __init__(self, log_dir=DEFAULT_LOG_DIR):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.signals_path = self.log_dir / "signals.csv"
        self.trades_path = self.log_dir / "trades.csv"
        self.executions_path = self.log_dir / "executions.csv"
        self._lock = threading.Lock()
        self._init_file(self.signals_path, SIGNALS_COLUMNS)
        self._init_file(self.trades_path, TRADES_COLUMNS)
        self._init_file(self.executions_path, EXECUTIONS_COLUMNS)

    @staticmethod
    def _init_file(path: Path, columns: list):
        if not path.exists():
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(columns)

    def _append(self, path: Path, columns: list, row: dict):
        with self._lock:
            with open(path, "a", newline="") as f:
                csv.writer(f).writerow([row.get(c, "") for c in columns])
                f.flush()

    def log_signal(
        self,
        *,
        market_slug,
        seconds_remaining,
        last_price,
        anchor_price,
        anchor_is_approximate,
        sigma_per_sqrt_sec,
        p_up,
        up_ask,
        down_ask,
        up_edge,
        down_edge,
        best_side,
        best_edge,
        kelly_fraction_applied,
        stake_usd,
        min_edge_threshold,
        traded,
        skip_reason="",
        ts=None,
    ):
        ts = ts if ts is not None else time.time()
        self._append(
            self.signals_path,
            SIGNALS_COLUMNS,
            {
                "ts": ts,
                "ts_iso": _iso(ts),
                "market_slug": market_slug,
                "seconds_remaining": seconds_remaining,
                "last_price": last_price,
                "anchor_price": anchor_price,
                "anchor_is_approximate": anchor_is_approximate,
                "sigma_per_sqrt_sec": sigma_per_sqrt_sec,
                "p_up": p_up,
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_edge": up_edge,
                "down_edge": down_edge,
                "best_side": best_side,
                "best_edge": best_edge,
                "kelly_fraction_applied": kelly_fraction_applied,
                "stake_usd": stake_usd,
                "min_edge_threshold": min_edge_threshold,
                "traded": traded,
                "skip_reason": skip_reason,
            },
        )

    def log_trade(
        self,
        *,
        market_slug,
        side,
        signal_ts,
        signal_probability,
        signal_edge,
        kelly_fraction,
        requested_price,
        filled_price,
        stake_usd,
        shares,
        bankroll_before,
        bankroll_after,
        entry_ts,
        resolve_ts,
        resolve_outcome,
        resolve_source,
        status,
        pnl_usd,
        fees_usd=0.0,
        trade_id=None,
    ) -> str:
        trade_id = trade_id or str(uuid.uuid4())
        slippage_bps = ((filled_price - requested_price) / requested_price * 10000) if requested_price else 0.0
        holding_seconds = resolve_ts - entry_ts
        pnl_bps = (pnl_usd / stake_usd * 10000) if stake_usd else 0.0
        self._append(
            self.trades_path,
            TRADES_COLUMNS,
            {
                "trade_id": trade_id,
                "market_slug": market_slug,
                "side": side,
                "signal_ts": signal_ts,
                "signal_probability": signal_probability,
                "signal_edge": signal_edge,
                "kelly_fraction": kelly_fraction,
                "requested_price": requested_price,
                "filled_price": filled_price,
                "slippage_bps": f"{slippage_bps:.2f}",
                "stake_usd": stake_usd,
                "shares": shares,
                "fees_usd": fees_usd,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll_after,
                "entry_ts": entry_ts,
                "entry_ts_iso": _iso(entry_ts),
                "resolve_ts": resolve_ts,
                "resolve_ts_iso": _iso(resolve_ts),
                "holding_seconds": f"{holding_seconds:.1f}",
                "resolve_outcome": resolve_outcome,
                "resolve_source": resolve_source,
                "status": status,
                "pnl_usd": f"{pnl_usd:.4f}",
                "pnl_bps": f"{pnl_bps:.2f}",
            },
        )
        return trade_id

    @contextmanager
    def time_execution(self, api: str, method: str = "GET", endpoint: str = "", context: str = ""):
        handle = ExecutionHandle(context=context)
        start = time.perf_counter()
        status = "ok"
        error_message = ""
        try:
            yield handle
        except Exception as e:
            status = "error"
            error_message = str(e)
            raise
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            ts = time.time()
            self._append(
                self.executions_path,
                EXECUTIONS_COLUMNS,
                {
                    "ts": ts,
                    "ts_iso": _iso(ts),
                    "api": api,
                    "method": method,
                    "endpoint": endpoint,
                    "context": handle.context,
                    "latency_ms": f"{latency_ms:.2f}",
                    "status": status,
                    "http_status": handle.http_status if handle.http_status is not None else "",
                    "error_message": error_message,
                },
            )


# ---------------------------------------------------------------------------
# Report (stdlib only)
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _percentile(sorted_values: list, p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[idx]


def print_report(log_dir: Path):
    signals = _read_csv(log_dir / "signals.csv")
    trades = _read_csv(log_dir / "trades.csv")
    executions = _read_csv(log_dir / "executions.csv")

    print(f"=== PolyBot performance report: {log_dir} ===\n")

    print(f"--- signals.csv ({len(signals)} rows) — are our edges real? what are we missing? ---")
    if signals:
        traded = [s for s in signals if s["traded"] == "True"]
        edges = [float(s["best_edge"]) for s in signals if s.get("best_edge")]
        print(f"  evaluated: {len(signals)}   traded: {len(traded)} ({len(traded) / len(signals) * 100:.1f}%)")
        if edges:
            print(
                f"  best_edge   mean={statistics.mean(edges) * 100:+.2f}%   "
                f"median={statistics.median(edges) * 100:+.2f}%   max={max(edges) * 100:+.2f}%"
            )
        skip_reasons = {}
        for s in signals:
            if s["traded"] != "True" and s["skip_reason"]:
                skip_reasons[s["skip_reason"]] = skip_reasons.get(s["skip_reason"], 0) + 1
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            print(f"  skipped ({reason}): {count}")
    else:
        print("  no data yet")
    print()

    print(f"--- trades.csv ({len(trades)} rows) — how well do we execute? where does P&L leak? ---")
    if trades:
        wins = [t for t in trades if t["status"] == "WON"]
        losses = [t for t in trades if t["status"] == "LOST"]
        pnl = [float(t["pnl_usd"]) for t in trades if t.get("pnl_usd")]
        slippage = [float(t["slippage_bps"]) for t in trades if t.get("slippage_bps")]
        holding = [float(t["holding_seconds"]) for t in trades if t.get("holding_seconds")]
        n = len(trades)
        print(f"  trades: {n}   wins: {len(wins)}   losses: {len(losses)}   win_rate: {len(wins) / n * 100:.1f}%")
        if pnl:
            print(f"  total P&L: ${sum(pnl):+.2f}   avg P&L/trade: ${statistics.mean(pnl):+.4f}")
        if slippage:
            print(
                f"  slippage(bps)   mean={statistics.mean(slippage):+.2f}   max_abs={max(abs(s) for s in slippage):.2f}"
            )
        if holding:
            print(f"  holding(s)   mean={statistics.mean(holding):.1f}   median={statistics.median(holding):.1f}")
    else:
        print("  no data yet")
    print()

    print(f"--- executions.csv ({len(executions)} rows) — how fast are we? where's the latency? ---")
    if executions:
        by_api = {}
        for e in executions:
            by_api.setdefault(e["api"], []).append(e)
        for api, rows in sorted(by_api.items()):
            lat = sorted(float(r["latency_ms"]) for r in rows if r.get("latency_ms"))
            errors = sum(1 for r in rows if r["status"] == "error")
            if lat:
                print(
                    f"  {api:20s} n={len(rows):5d}   p50={_percentile(lat, 50):7.1f}ms   "
                    f"p95={_percentile(lat, 95):7.1f}ms   p99={_percentile(lat, 99):7.1f}ms   "
                    f"max={lat[-1]:7.1f}ms   errors={errors}"
                )
    else:
        print("  no data yet")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--report", action="store_true", help="Print a summary report over existing logs and exit")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    args = parser.parse_args()

    if args.report:
        print_report(Path(args.log_dir))
    else:
        print(__doc__)


if __name__ == "__main__":
    main()

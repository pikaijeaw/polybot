#!/usr/bin/env python3
"""
Web dashboard for the paper bots — Arbitrage (Pair Cost) and Copy Trading —
plus a live BTC price up top. A separate process from the bots it manages, by
design.

Reads each bot's own state file/trades log from disk; never talks to a bot
process directly. That's deliberate isolation: restart this dashboard as often
as you like without interrupting a running bot, and vice versa. If this
process crashes or hangs, the bots keep trading.

Mostly read-only: never signs anything or places orders. The exceptions are
the bot controls below and /api/<strategy>/reset.

Usage:
    python paper_trading/web_dashboard.py
    python paper_trading/web_dashboard.py --port 8080
    python paper_trading/web_dashboard.py --host 0.0.0.0   # expose beyond localhost — only on a trusted network

Then open http://127.0.0.1:5000 in a browser. Polls this app's own
/api/data endpoint roughly every 1.5s via plain JS fetch() — no websockets.

Bot control plane: this process owns start/stop/status for each bot
(/api/bot/*), spawning it as its own subprocess and tracking liveness via a
pidfile (pidfile.py) rather than an in-memory handle — that way a dashboard
restart can still see whether a bot is running instead of losing track of it
or spawning a duplicate.

"arb" is pair_cost_paper_trader.py ("Arbitrage (Pair Cost)": legs into Up and
Down separately so their average combined cost stays under $1) — see
_arb_args. "arb2" is pair_cost_v2_paper_trader.py: the same bot, but it buys
a risk-free pair (both sides at once, under $1 after fees) whenever the book
offers one — see _arb2_args. "copy" is copy_paper_trader.py: mirrors one Polymarket account's
trades (any market) into a paper bankroll — see _copy_args. Both write the
same state shape, so they share the panel/CSV/reset code. /api/data returns
every bot's state in one payload under "bots".

Every bot logs into its own folder, log_collector/<folder>/ at the project
root (bot.log, events.jsonl, runs.jsonl, signals.csv, trades.csv) — see
LOGS_DIR/LOG_FOLDERS and log_collector/README.md.

/api/<strategy>/reset (POST) moves that bot's state file and its whole
log folder into log_collector/archive/<folder>_<UTC date>_<time>/ — never
deletes them, so a reset run is still there to analyse. Refuses (409) while
that bot is running.

/api/killswitch (POST) stops every bot and then resets each one.
"""

import argparse
import csv
import io
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template, request

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))
import copy_paper_trader as copy_bot
import pair_cost_paper_trader as pair_cost
import pair_cost_v2_paper_trader as pair_cost_v2

import pidfile

app = Flask(__name__, root_path=str(SCRIPT_DIR))


class LivePrice:
    """Polls Binance's REST last-price endpoint at most every 2 seconds."""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.price: float | None = None
        self.error: str | None = None
        self._next_poll = 0.0

    def maybe_refresh(self):
        if time.time() < self._next_poll:
            return
        try:
            resp = requests.get(
                "https://api.binance.com/api/v3/ticker/price", params={"symbol": self.symbol}, timeout=5
            )
            resp.raise_for_status()
            self.price = float(resp.json()["price"])
            self.error = None
        except Exception as e:
            self.error = str(e)
        self._next_poll = time.time() + 2.0


def _load_state(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


live_price = LivePrice()
# Every bot's logs live in log_collector/<folder>/ at the project root (bot.log,
# events.jsonl, runs.jsonl, the tracker CSVs) — see log_collector/README.md.
# Folder names are spelled out so the folder can be read without knowing the mode keys.
LOGS_DIR = PROJECT_ROOT / "log_collector"
LOG_FOLDERS = {"arb": "arbitrage_pair_cost", "arb2": "arbitrage_pair_cost_v2", "copy": "copy_trading"}

# -- bot process control -----------------------------------------------------

# Each bot is its own script with its own unique state/pid/log defaults, so no
# path overrides are needed when spawning it.
BOT_SCRIPTS = {
    "arb": {
        # A different script with its own unique file defaults, so no
        # path overrides are needed (see _arb_args).
        "script": SCRIPT_DIR / "pair_cost_paper_trader.py",
        "pidfile": pair_cost.DEFAULT_PID_PATH,
        "log": LOGS_DIR / "arbitrage_pair_cost" / "bot.log",
    },
    "arb2": {
        "script": SCRIPT_DIR / "pair_cost_v2_paper_trader.py",
        "pidfile": pair_cost_v2.DEFAULT_PID_PATH,
        "log": LOGS_DIR / "arbitrage_pair_cost_v2" / "bot.log",
    },
    "copy": {
        "script": SCRIPT_DIR / "copy_paper_trader.py",
        "pidfile": copy_bot.DEFAULT_PID_PATH,
        "log": LOGS_DIR / "copy_trading" / "bot.log",
    },
}
bot_processes: dict = {}  # mode -> subprocess.Popen, only for processes THIS dashboard instance spawned

PAPER_STRATEGIES = tuple(BOT_SCRIPTS)


def _state_paths_for(strategy: str) -> tuple:
    bot = {"arb": pair_cost, "arb2": pair_cost_v2, "copy": copy_bot}[strategy]
    return bot.DEFAULT_STATE_PATH, bot.DEFAULT_TRADES_LOG_PATH


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/data")
def api_data():
    """Every bot's state in one payload — the panels are all always visible."""
    live_price.maybe_refresh()

    bots = {}
    for strategy in PAPER_STRATEGIES:
        sp, _ = _state_paths_for(strategy)
        bots[strategy] = _load_state(sp)

    return jsonify(
        {
            "server_time": time.time(),
            "price": {"value": live_price.price, "error": live_price.error},
            "bots": bots,
        }
    )


def _bot_status(mode: str) -> dict:
    proc = bot_processes.get(mode)
    if proc is not None:
        proc.poll()  # opportunistically reap a child THIS dashboard spawned, if it has exited
    info = BOT_SCRIPTS[mode]
    pid = pidfile.read_pid(info["pidfile"])
    running = pid is not None and pidfile.pid_is_alive(pid)
    return {"mode": mode, "running": running, "pid": pid if running else None}


def _terminate(pid: int, proc: subprocess.Popen | None = None) -> bool:
    """SIGTERM pid, wait up to 5s. Returns True if it is gone."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    for _ in range(20):
        if proc is not None:
            proc.poll()  # reap it — os.kill(pid, 0) reports a zombie as "alive" otherwise
        if not pidfile.pid_is_alive(pid):
            return True
        time.sleep(0.25)
    return not pidfile.pid_is_alive(pid)


def _float_param(payload: dict, key: str, default: float, lo: float | None = None, hi: float | None = None) -> float:
    try:
        v = float(payload.get(key, default))
    except (TypeError, ValueError):
        v = default
    if lo is not None:
        v = max(v, lo)
    if hi is not None:
        v = min(v, hi)
    return v


def _arb_args(payload: dict) -> list:
    """pair_cost_paper_trader.py: the leg-in/hedge knobs. Its own state/trades/pid defaults are already unique."""
    return [
        "--bankroll", str(_float_param(payload, "bankroll", 50.0, lo=1.0)),
        "--clip-usd", str(_float_param(payload, "clip_usd", pair_cost.DEFAULT_CLIP_USD, lo=0.5)),
        "--entry-price", str(_float_param(payload, "entry_price", pair_cost.DEFAULT_ENTRY_PRICE, lo=0.01, hi=0.99)),
        "--min-margin", str(_float_param(payload, "min_margin", pair_cost.DEFAULT_MIN_MARGIN, lo=0.0, hi=0.5)),
        "--max-window-usd", str(_float_param(payload, "max_window_usd", pair_cost.DEFAULT_MAX_WINDOW_USD, lo=0.5)),
        "--min-seconds-remaining", str(_float_param(payload, "min_seconds_remaining", pair_cost.DEFAULT_MIN_SECONDS_REMAINING, lo=0.0, hi=300.0)),
        "--latency-ms", str(_float_param(payload, "latency_ms", pair_cost.DEFAULT_LATENCY_MS, lo=0.0, hi=5000.0)),
    ]  # fmt: skip


def _arb2_args(payload: dict) -> list:
    """pair_cost_v2_paper_trader.py: v1's knobs plus --pair-only (risk-free pairs only)."""
    return _arb_args(payload) + (["--pair-only"] if payload.get("pair_only") is True else [])


def _copy_args(payload: dict) -> list:
    """copy_paper_trader.py. --follow is free text from the browser (username,
    profile URL or wallet), so it goes in as one --follow=<value> argv entry:
    never through a shell, and never parseable as a separate flag."""
    follow = str(payload.get("follow") or copy_bot.DEFAULT_FOLLOW).strip()[:200]
    return [
        f"--follow={follow}",
        "--bankroll", str(_float_param(payload, "bankroll", 50.0, lo=1.0)),
        "--ratio", str(_float_param(payload, "ratio", copy_bot.DEFAULT_RATIO, lo=0.0001, hi=100.0)),
        "--max-market-usd", str(_float_param(payload, "max_market_usd", copy_bot.DEFAULT_MAX_MARKET_USD, lo=1.0)),
        "--max-slippage", str(_float_param(payload, "max_slippage", copy_bot.DEFAULT_MAX_SLIPPAGE, lo=0.0, hi=1.0)),
    ]  # fmt: skip


# Every mode's args builder in one place — api_bot_start dispatches through
# this instead of an if/else chain, so adding a strategy means adding one
# entry here rather than another branch.
ARGS_BUILDERS = {
    "arb": _arb_args,
    "arb2": _arb2_args,
    "copy": _copy_args,
}


@app.route("/api/bot/status")
def api_bot_status():
    return jsonify({mode: _bot_status(mode) for mode in BOT_SCRIPTS})


@app.route("/api/bot/start", methods=["POST"])
def api_bot_start():
    payload = request.get_json(force=True, silent=True) or {}
    mode = payload.get("mode")
    if mode not in BOT_SCRIPTS:
        return jsonify({"error": f"unknown mode {mode!r}"}), 400

    status = _bot_status(mode)
    if status["running"]:
        return jsonify({"error": f"{mode} bot already running (pid {status['pid']})"}), 409

    info = BOT_SCRIPTS[mode]
    extra_args = ARGS_BUILDERS[mode](payload)

    proc_args = [sys.executable, "-u", str(info["script"]), *extra_args]  # -u: bot.log written live, not block-buffered
    info["log"].parent.mkdir(parents=True, exist_ok=True)
    with open(info["log"], "a") as log_f:
        proc = subprocess.Popen(proc_args, stdout=log_f, stderr=subprocess.STDOUT, cwd=str(PROJECT_ROOT))
    # log_f is closed in the parent as soon as Popen has dup'd it to the child — the
    # child keeps its own fd, this process doesn't need to hold it open.
    bot_processes[mode] = proc

    time.sleep(0.5)  # give it a moment to fail fast (pidfile collision, bad args) before we report success
    if proc.poll() is not None:
        return jsonify({"error": f"{mode} bot exited immediately (code {proc.returncode}); see {info['log']}"}), 500
    return jsonify({"ok": True, "mode": mode, "pid": proc.pid})


@app.route("/api/bot/stop", methods=["POST"])
def api_bot_stop():
    payload = request.get_json(force=True, silent=True) or {}
    mode = payload.get("mode")
    if mode not in BOT_SCRIPTS:
        return jsonify({"error": f"unknown mode {mode!r}"}), 400

    status = _bot_status(mode)
    if not status["running"]:
        return jsonify({"error": f"{mode} bot is not running"}), 409

    pid = status["pid"]
    still_alive = not _terminate(pid, bot_processes.get(mode))  # proc only set if THIS dashboard spawned it
    return jsonify({"ok": not still_alive, "mode": mode, "pid": pid, "still_alive": still_alive})


@app.route("/api/killswitch", methods=["POST"])
def api_killswitch():
    """Stop every bot, then archive each bot's data."""
    result = {"bots": {}}
    for mode in PAPER_STRATEGIES:
        st = _bot_status(mode)
        if st["running"] and not _terminate(st["pid"], bot_processes.get(mode)):
            result["bots"][mode] = "still running — not reset"
            continue
        moved, _ = _archive(mode)
        result["bots"][mode] = f"reset ({len(moved)} files archived)"
    return jsonify({"ok": all(v.startswith("reset") for v in result["bots"].values()), **result})


ARCHIVE_DIR = LOGS_DIR / "archive"


def _archive(strategy: str) -> tuple[list, Path]:
    # Archived, never deleted — these logs are the data for analysing a run.
    sp, tl = _state_paths_for(strategy)
    dest = ARCHIVE_DIR / f"{LOG_FOLDERS[strategy]}_{datetime.now(UTC).strftime('%Y-%m-%d_%H%M%S')}"
    moved = []
    # The bot's whole log folder goes with the state file, bot.log and runs.jsonl included.
    for path in dict.fromkeys([sp, sp.with_suffix(".tmp"), tl, *(LOGS_DIR / LOG_FOLDERS[strategy]).glob("*")]):
        if path.is_file():
            dest.mkdir(parents=True, exist_ok=True)
            path.rename(dest / path.name)
            moved.append(str(path.relative_to(PROJECT_ROOT)))
    return moved, dest


@app.route("/api/<strategy>/reset", methods=["POST"])
def api_reset(strategy: str):
    if strategy not in PAPER_STRATEGIES:
        return jsonify({"error": f"unknown strategy {strategy!r}"}), 404
    status = _bot_status(strategy)
    if status["running"]:
        return jsonify({"error": f"{strategy} bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    moved, dest = _archive(strategy)
    return jsonify(
        {"ok": True, "deleted": moved, "archived_to": str(dest.relative_to(PROJECT_ROOT)) if moved else None}
    )


def load_trade_history(path: Path) -> list:
    """Reconstructs one row per position from the OPEN/CLOSE event pairs in
    the trades log — the full history, unlike state.json's "recent_closed"
    which is capped to the last 25 for the live dashboard."""
    positions = {}
    if not path.exists():
        return []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            slug = event.get("market_slug")
            if not slug or event.get("event") not in ("OPEN", "CLOSE"):
                continue  # arb/copy also log BUY/SELL/SKIP lines here; those aren't positions
            positions.setdefault(slug, {}).update(event)
    return sorted(positions.values(), key=lambda p: p.get("opened_at") or 0)


def _iso(ts) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def build_report_csv(strategy: str) -> str:
    sp, tl = _state_paths_for(strategy)
    history = load_trade_history(tl)
    state = _load_state(sp) or {}
    stats = state.get("stats", {})

    buf = io.StringIO()
    w = csv.writer(buf)

    title = {"arb": "Arbitrage (Pair Cost)", "arb2": "Arbitrage (Pair Cost) v2", "copy": "Copy trading"}[strategy]
    w.writerow([f"{title} — paper trading report"])
    w.writerow(["Generated", _iso(time.time())])
    w.writerow([])
    w.writerow(["Starting bankroll", f"${state.get('starting_bankroll', 0):.2f}"])
    w.writerow(["Current equity", f"${state.get('equity', 0):.2f}"])
    w.writerow(["Total return", f"{(stats.get('return_pct') or 0) * 100:.2f}%"])
    w.writerow(["Total trades", stats.get("trades", 0)])
    w.writerow(["Wins", stats.get("wins", 0)])
    w.writerow(["Losses", stats.get("losses", 0)])
    w.writerow(["Win rate", f"{(stats.get('win_rate') or 0) * 100:.1f}%" if stats.get("win_rate") is not None else "-"])
    w.writerow(["Total staked", f"${stats.get('total_staked', 0):.2f}"])
    w.writerow(["Realized P&L", f"${stats.get('realized_pnl', 0):+.2f}"])
    w.writerow([])

    w.writerow(
        [
            "Market",
            "Side",
            "Status",
            "Entry Price",
            "Stake ($)",
            "Edge at Entry",
            "Opened At",
            "Settled At",
            "Resolution Source",
            "P&L ($)",
        ]
    )
    for p in history:
        w.writerow(
            [
                p.get("market_slug", ""),
                p.get("side", ""),
                p.get("status", "OPEN"),
                f"{p.get('entry_price', 0):.3f}" if p.get("entry_price") is not None else "",
                f"{p.get('stake_usd', 0):.2f}" if p.get("stake_usd") is not None else "",
                f"{(p.get('edge_at_entry') or 0) * 100:+.2f}%" if p.get("edge_at_entry") is not None else "",
                _iso(p.get("opened_at")),
                _iso(p.get("settled_at")),
                p.get("resolution_source", ""),
                f"{p.get('pnl_usd'):+.2f}" if p.get("pnl_usd") is not None else "",
            ]
        )

    return buf.getvalue()


@app.route("/api/report.csv")
def api_report_csv():
    strategy = request.args.get("strategy")
    if strategy not in PAPER_STRATEGIES:
        return Response("Unknown strategy.", mimetype="text/plain", status=404)
    sp, _ = _state_paths_for(strategy)
    if not sp.exists():
        return Response("No paper trading data yet.", mimetype="text/plain", status=404)

    csv_text = build_report_csv(strategy)
    filename = f"{strategy}_trading_report_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1, localhost only)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true", help="Flask debug mode (auto-reload, verbose errors)")
    args = parser.parse_args()

    print(f"Dashboard running at http://{args.host}:{args.port}", file=sys.stderr)
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()

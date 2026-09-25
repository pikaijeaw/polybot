#!/usr/bin/env python3
"""
Web dashboard for the oracle-lag scalper's paper bots — v1, v2, and v3 —
plus a live BTC price up top. A separate process from any of the bots it manages by
design.

Reads each bot's own state file/trades log from disk; never imports or talks
to a bot process directly. That's deliberate isolation: restart this
dashboard as often as you like — to change ports, pull in a template edit,
whatever — without ever interrupting a running bot, and vice versa. If this
process crashes or hangs, the bots keep trading uninterrupted.

Mostly read-only: never signs anything or places orders, and only reads
state/trades files and polls public price data, same as dashboard.py
(the terminal version, used for its price-watching logic) —
with one deliberate exception, /api/<strategy>/reset (see below), which
archives that bot's own state and starts it fresh.

Note: this dashboard used to also carry Gabagool (arbitrage) and
Trend-Confirm (EMA/Supertrend) panels. Both were removed — UI only:
gabagool_strategy.py, gabagool_paper_trader.py, trend_strategy.py, and
experiments/trend_paper_trader.py are all untouched and remain fully
runnable standalone; see their own module docstrings.

Usage:
    python paper_trading/web_dashboard.py
    python paper_trading/web_dashboard.py --port 8080
    python paper_trading/web_dashboard.py --state-file /path/to/paper_state.json --trades-log /path/to/paper_trades.jsonl
    python paper_trading/web_dashboard.py --host 0.0.0.0   # expose beyond localhost — only on a trusted network

Then open http://127.0.0.1:5000 in a browser. Polls this app's own
/api/data endpoint roughly every 1.5s via plain JS fetch() — no websockets.

Bot control plane: this process also owns start/stop/status for
paper_trader.py (v1/v2/v3 — see /api/bot/*), spawning each as its own
subprocess and tracking liveness via a pidfile (pidfile.py) rather than an
in-memory handle — that way a dashboard restart can still see whether a bot
is running instead of losing track of it or spawning a duplicate.

"paper" (oracle-lag v1, 5m), "oracle_v2" (oracle-lag v2 — same engine,
three additional entry filters, see oracle_lag_strategy_v2.py), and
"oracle_v3" (oracle-lag v3 — v2's filters plus Kelly sizing that compounds
off live equity instead of a fixed bankroll, see oracle_lag_strategy_v3.py)
are three independent paper strategies, all always-visible, all
independently start/stop-able, and all rendered in parallel on one page —
not a toggle between them. All three run the exact same script
(paper_trader.py, --v2 / --v3 for the latter two) against the exact same
live market, which is the point: start them side by side to compare v1 vs
v2 vs v3 in real time — see _oracle_v2_args/_oracle_v3_args, which have to
explicitly override every one of paper_trader.py's
--state-file/--trades-log/--pid-file/--autorestart-marker/--perf-log-dir so
"oracle_v2"/"oracle_v3" don't silently share "paper"'s files (that script's
own defaults for all five are identical regardless of --v2/--v3).
/api/data returns all three paper states in one payload under "bots" (see
PAPER_STRATEGIES, _state_paths_for) rather than taking a selector;
/api/report.csv and the reset endpoints still take a per-strategy target
since a CSV export or a destructive reset is inherently a "pick one" action.

"early" is the same script again with --early (early_move_strategy.py:
early entries on a clear move only, fixed stake) — see _early_args.

/api/<strategy>/reset (POST) moves that bot's own state/trades files (and
perf-log CSVs) into paper_trading/archive/<strategy>-<UTC timestamp>/ —
never deletes them, so a reset run is still there to analyse. Each refuses (409) while its own bot is running, same reason two instances
of either bot aren't allowed to share one state file (see pidfile.py).
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

from flask import Flask, Response, jsonify, render_template, request

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))
import dashboard as dash  # reuse LivePrice / load_paper_state — no duplicated logic

import early_move_strategy
import oracle_lag_strategy_v2 as strategy_v2
import oracle_lag_strategy_v3 as strategy_v3
import pidfile

app = Flask(__name__, root_path=str(SCRIPT_DIR))

live_price = dash.LivePrice()
state_path = dash.DEFAULT_STATE_PATH
trades_log_path = SCRIPT_DIR / "paper_trades.jsonl"
PERF_LOG_DIR = SCRIPT_DIR / "perf_logs"

# -- bot process control -----------------------------------------------------

# "oracle_v2"/"oracle_v3" run paper_trader.py itself (same script as
# "paper") with --v2/--v3 — but paper_trader.py's own
# --state-file/--trades-log/--pid-file/--autorestart-marker/--perf-log-dir
# defaults are IDENTICAL regardless of which version flag is passed (those
# flags only swap the strategy engine). Left alone, a dashboard-spawned
# "oracle_v2"/"oracle_v3" process would silently share "paper"'s state
# file, trades log, perf-log CSVs, and even its pidfile (refusing to start
# at all while "paper" is running). Every one of these must be passed
# explicitly in _oracle_v2_args/_oracle_v3_args to actually get the
# isolation the panels are supposed to have — see api_bot_start.
ORACLE_V2_STATE_PATH = SCRIPT_DIR / "paper_state_v2.json"
ORACLE_V2_TRADES_LOG_PATH = SCRIPT_DIR / "paper_trades_v2.jsonl"
ORACLE_V2_PID_PATH = SCRIPT_DIR / "paper_trader_v2.pid"
ORACLE_V2_AUTORESTART_MARKER = SCRIPT_DIR / "paper_trader_v2.autorestart.json"
ORACLE_V2_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs_v2"

ORACLE_V3_STATE_PATH = SCRIPT_DIR / "paper_state_v3.json"
ORACLE_V3_TRADES_LOG_PATH = SCRIPT_DIR / "paper_trades_v3.jsonl"
ORACLE_V3_PID_PATH = SCRIPT_DIR / "paper_trader_v3.pid"
ORACLE_V3_AUTORESTART_MARKER = SCRIPT_DIR / "paper_trader_v3.autorestart.json"
ORACLE_V3_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs_v3"

EARLY_STATE_PATH = SCRIPT_DIR / "paper_state_early.json"
EARLY_TRADES_LOG_PATH = SCRIPT_DIR / "paper_trades_early.jsonl"
EARLY_PID_PATH = SCRIPT_DIR / "paper_trader_early.pid"
EARLY_AUTORESTART_MARKER = SCRIPT_DIR / "paper_trader_early.autorestart.json"
EARLY_PERF_LOG_DIR = SCRIPT_DIR / "perf_logs_early"

BOT_SCRIPTS = {
    "paper": {
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": SCRIPT_DIR / "paper_trader.pid",
        "log": SCRIPT_DIR / "bot.log",
    },
    "oracle_v2": {
        # Same script as "paper" — paper_trader.py's --v2 flag swaps in
        # oracle_lag_strategy_v2.OracleLagEngineV2 (see _oracle_v2_args).
        # Distinct pidfile/state/trades/log so it can run alongside "paper"
        # without either corrupting the other's history — that's the whole
        # point, comparing v1 and v2 live against the same market.
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": ORACLE_V2_PID_PATH,
        "log": SCRIPT_DIR / "bot_v2.log",
    },
    "oracle_v3": {
        # Same script again — paper_trader.py's --v3 flag swaps in
        # oracle_lag_strategy_v3.OracleLagEngineV3 (see _oracle_v3_args).
        # Distinct pidfile/state/trades/log, same reasoning as "oracle_v2".
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": ORACLE_V3_PID_PATH,
        "log": SCRIPT_DIR / "bot_v3.log",
    },
    "early": {
        # Same script again — paper_trader.py's --early flag swaps in
        # early_move_strategy.EarlyMoveEngine (see _early_args).
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": EARLY_PID_PATH,
        "log": SCRIPT_DIR / "bot_early.log",
    },
}
bot_processes: dict = {}  # mode -> subprocess.Popen, only for processes THIS dashboard instance spawned

# "paper", "oracle_v2", and "oracle_v3" are three independent strategies,
# each always visible and independently start/stop-able (see
# templates/dashboard.html). This maps which (state_file, trades_log) pair
# backs each one; "paper"'s pair is mutable (set from
# --state-file/--trades-log in main()), the other two are fixed to their
# own scripts' defaults since there's no CLI flag for either here.
PAPER_STRATEGIES = ("paper", "oracle_v2", "oracle_v3", "early")


def _state_paths_for(strategy: str) -> tuple:
    if strategy == "oracle_v2":
        return ORACLE_V2_STATE_PATH, ORACLE_V2_TRADES_LOG_PATH
    if strategy == "oracle_v3":
        return ORACLE_V3_STATE_PATH, ORACLE_V3_TRADES_LOG_PATH
    if strategy == "early":
        return EARLY_STATE_PATH, EARLY_TRADES_LOG_PATH
    return state_path, trades_log_path


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/data")
def api_data():
    """Returns all three paper strategies' states in one payload — see
    module docstring: these render as parallel, always-visible panels now,
    not a toggle, so there's no `?strategy=` selector to pick just one."""
    live_price.maybe_refresh()

    bots = {}
    for strategy in PAPER_STRATEGIES:
        sp, _ = _state_paths_for(strategy)
        bots[strategy] = dash.load_paper_state(sp)

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


def _int_param(payload: dict, key: str, default: int, lo: int | None = None, hi: int | None = None) -> int:
    try:
        v = int(payload.get(key, default))
    except (TypeError, ValueError):
        v = default
    if lo is not None:
        v = max(v, lo)
    if hi is not None:
        v = min(v, hi)
    return v


def _paper_args(payload: dict) -> list:
    args = [
        "--bankroll", str(_float_param(payload, "bankroll", 50.0, lo=1.0)),
        "--min-edge", str(_float_param(payload, "min_edge", 0.02, lo=0.0, hi=0.5)),
        "--max-position-pct", str(_float_param(payload, "max_position_pct", 0.05, lo=0.001, hi=1.0)),
        "--kelly-multiplier", str(_float_param(payload, "kelly_multiplier", 0.25, lo=0.01, hi=1.0)),
    ]  # fmt: skip
    max_open_positions = payload.get("max_open_positions")
    if max_open_positions not in (None, "", "null"):
        args += ["--max-open-positions", str(_int_param(payload, "max_open_positions", 1, lo=1, hi=100))]
    return args


def _oracle_v2_args(payload: dict) -> list:
    """paper_trader.py's --v2 flag plus v1's core args (same script,
    same core flags) — the four v2-only filter knobs are additive on top.
    Also passes --state-file/--trades-log/--pid-file/--autorestart-marker/
    --perf-log-dir explicitly (see ORACLE_V2_* constants and the comment
    above BOT_SCRIPTS) — paper_trader.py's own defaults for all five are
    identical regardless of --v2, so without this "oracle_v2" would
    silently collide with "paper" on every one of them."""
    return _paper_args(payload) + [
        "--v2",
        "--min-prob",
        str(_float_param(payload, "min_prob", strategy_v2.DEFAULT_MIN_PROB, lo=0.0, hi=1.0)),
        "--safety-factor",
        str(_float_param(payload, "safety_factor", strategy_v2.DEFAULT_SAFETY_FACTOR, lo=0.0, hi=1.0)),
        "--entry-window-start",
        str(_float_param(payload, "entry_window_start", strategy_v2.DEFAULT_ENTRY_WINDOW_START, lo=0.0, hi=3600.0)),
        "--entry-window-end",
        str(_float_param(payload, "entry_window_end", strategy_v2.DEFAULT_ENTRY_WINDOW_END, lo=0.0, hi=3600.0)),
        "--state-file",
        str(ORACLE_V2_STATE_PATH),
        "--trades-log",
        str(ORACLE_V2_TRADES_LOG_PATH),
        "--pid-file",
        str(ORACLE_V2_PID_PATH),
        "--autorestart-marker",
        str(ORACLE_V2_AUTORESTART_MARKER),
        "--perf-log-dir",
        str(ORACLE_V2_PERF_LOG_DIR),
    ]


def _oracle_v3_args(payload: dict) -> list:
    """Same shape as _oracle_v2_args (paper_trader.py's --v3 flag instead of
    --v2 — same three filter knobs, v3 adds no new ones of its own, since
    compounding is automatic once wired) but with its own distinct
    --state-file/--trades-log/--pid-file/--autorestart-marker/--perf-log-dir
    (ORACLE_V3_* constants) so it doesn't collide with "paper" or
    "oracle_v2"."""
    return _paper_args(payload) + [
        "--v3",
        "--min-prob",
        str(_float_param(payload, "min_prob", strategy_v3.DEFAULT_MIN_PROB, lo=0.0, hi=1.0)),
        "--safety-factor",
        str(_float_param(payload, "safety_factor", strategy_v3.DEFAULT_SAFETY_FACTOR, lo=0.0, hi=1.0)),
        "--entry-window-start",
        str(_float_param(payload, "entry_window_start", strategy_v3.DEFAULT_ENTRY_WINDOW_START, lo=0.0, hi=3600.0)),
        "--entry-window-end",
        str(_float_param(payload, "entry_window_end", strategy_v3.DEFAULT_ENTRY_WINDOW_END, lo=0.0, hi=3600.0)),
        "--state-file",
        str(ORACLE_V3_STATE_PATH),
        "--trades-log",
        str(ORACLE_V3_TRADES_LOG_PATH),
        "--pid-file",
        str(ORACLE_V3_PID_PATH),
        "--autorestart-marker",
        str(ORACLE_V3_AUTORESTART_MARKER),
        "--perf-log-dir",
        str(ORACLE_V3_PERF_LOG_DIR),
    ]


def _early_args(payload: dict) -> list:
    """paper_trader.py --early: fixed stake, no Kelly — so no bankroll-%/kelly
    knobs, just the early-move filters. Distinct files for the same reason as
    _oracle_v2_args."""
    args = [
        "--early",
        "--bankroll", str(_float_param(payload, "bankroll", 50.0, lo=1.0)),
        "--stake-usd", str(_float_param(payload, "stake_usd", early_move_strategy.DEFAULT_STAKE_USD, lo=0.1)),
        "--min-z", str(_float_param(payload, "min_z", early_move_strategy.DEFAULT_MIN_Z, lo=0.0, hi=10.0)),
        "--max-price", str(_float_param(payload, "max_price", early_move_strategy.DEFAULT_MAX_PRICE, lo=0.01, hi=0.99)),
        "--min-edge", str(_float_param(payload, "min_edge", early_move_strategy.DEFAULT_MIN_EDGE, lo=0.0, hi=0.5)),
        "--early-min-elapsed", str(_float_param(payload, "early_min_elapsed", early_move_strategy.DEFAULT_MIN_ELAPSED, lo=0.0, hi=300.0)),
        "--early-max-elapsed", str(_float_param(payload, "early_max_elapsed", early_move_strategy.DEFAULT_MAX_ELAPSED, lo=0.0, hi=300.0)),
        "--state-file", str(EARLY_STATE_PATH),
        "--trades-log", str(EARLY_TRADES_LOG_PATH),
        "--pid-file", str(EARLY_PID_PATH),
        "--autorestart-marker", str(EARLY_AUTORESTART_MARKER),
        "--perf-log-dir", str(EARLY_PERF_LOG_DIR),
    ]  # fmt: skip
    max_open_positions = payload.get("max_open_positions")
    if max_open_positions not in (None, "", "null"):
        args += ["--max-open-positions", str(_int_param(payload, "max_open_positions", 1, lo=1, hi=100))]
    return args


# Every mode's args builder in one place — api_bot_start dispatches through
# this instead of an if/else chain, so adding a strategy means adding one
# entry here rather than another branch.
ARGS_BUILDERS = {
    "paper": _paper_args,
    "oracle_v2": _oracle_v2_args,
    "oracle_v3": _oracle_v3_args,
    "early": _early_args,
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
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    proc = bot_processes.get(mode)  # only set if THIS dashboard instance spawned it
    for _ in range(20):
        if proc is not None:
            proc.poll()  # reap it — os.kill(pid, 0) below reports a zombie as "alive" otherwise
        if not pidfile.pid_is_alive(pid):
            break
        time.sleep(0.25)
    still_alive = pidfile.pid_is_alive(pid)
    return jsonify({"ok": not still_alive, "mode": mode, "pid": pid, "still_alive": still_alive})


ARCHIVE_DIR = SCRIPT_DIR / "archive"
PERF_LOG_DIRS = {
    "paper": PERF_LOG_DIR,
    "oracle_v2": ORACLE_V2_PERF_LOG_DIR,
    "oracle_v3": ORACLE_V3_PERF_LOG_DIR,
    "early": EARLY_PERF_LOG_DIR,
}


@app.route("/api/<strategy>/reset", methods=["POST"])
def api_reset(strategy: str):
    if strategy not in PAPER_STRATEGIES:
        return jsonify({"error": f"unknown strategy {strategy!r}"}), 404
    status = _bot_status(strategy)
    if status["running"]:
        return jsonify({"error": f"{strategy} bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    # Archived, never deleted — these logs are the data for analysing a run.
    sp, tl = _state_paths_for(strategy)
    dest = ARCHIVE_DIR / f"{strategy}-{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    moved = []
    for path in [sp, sp.with_suffix(".tmp"), tl, *PERF_LOG_DIRS[strategy].glob("*.csv")]:
        if path.exists():
            dest.mkdir(parents=True, exist_ok=True)
            path.rename(dest / path.name)
            moved.append(str(path.relative_to(PROJECT_ROOT)))
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
            if not slug:
                continue
            positions.setdefault(slug, {}).update(event)
    return sorted(positions.values(), key=lambda p: p.get("opened_at") or 0)


def _iso(ts) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def build_report_csv(strategy: str = "paper") -> str:
    sp, tl = _state_paths_for(strategy)
    history = load_trade_history(tl)
    state = dash.load_paper_state(sp) or {}
    stats = state.get("stats", {})

    buf = io.StringIO()
    w = csv.writer(buf)

    title = {"oracle_v2": "Oracle-lag scalper v2", "oracle_v3": "Oracle-lag scalper v3", "early": "Early-move"}.get(
        strategy, "Oracle-lag scalper"
    )
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
    strategy = request.args.get("strategy", "paper")
    if strategy not in PAPER_STRATEGIES:
        strategy = "paper"
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
    global state_path, trades_log_path

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1, localhost only)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--state-file", default=str(dash.DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(SCRIPT_DIR / "paper_trades.jsonl"))
    parser.add_argument("--debug", action="store_true", help="Flask debug mode (auto-reload, verbose errors)")
    args = parser.parse_args()

    state_path = Path(args.state_file)
    trades_log_path = Path(args.trades_log)

    print(f"Dashboard running at http://{args.host}:{args.port}", file=sys.stderr)
    print(f"Reading state from {state_path}", file=sys.stderr)
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()

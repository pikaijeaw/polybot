#!/usr/bin/env python3
"""
Web dashboard for monitoring and controlling every paper-trading strategy in
this repo — oracle-lag v1, oracle-lag v2, oracle-lag v3, and Gabagool — plus
live trading, all from one page. A separate process from any of the bots it
manages by design.

Reads each bot's own state file/trades log from disk; never imports or talks
to a bot process directly. That's deliberate isolation: restart this
dashboard as often as you like — to change ports, pull in a template edit,
whatever — without ever interrupting a running bot, and vice versa. If this
process crashes or hangs, the bots keep trading uninterrupted.

Mostly read-only: never signs anything or places orders, and only reads
state/trades files and polls public price/chain data, same as dashboard.py
(the terminal version, used for "paper"'s price/wallet-watching logic) —
with four deliberate exceptions, /api/paper/reset, /api/oracle_v2/reset,
/api/oracle_v3/reset, and /api/gabagool/reset (see below), which delete that
bot's own state.

Note: experiments/trend_paper_trader.py (EMA/Supertrend confirmation) used
to have a panel here too. It's no longer wired into this dashboard — this
was a UI-only removal, the script and its historical state/trades files are
untouched and it remains fully runnable standalone; see its own module
docstring.

Usage:
    python paper_trading/web_dashboard.py
    python paper_trading/web_dashboard.py --port 8080
    python paper_trading/web_dashboard.py --state-file /path/to/paper_state.json --trades-log /path/to/paper_trades.jsonl
    python paper_trading/web_dashboard.py --host 0.0.0.0   # expose beyond localhost — only on a trusted network

Then open http://127.0.0.1:5000 in a browser. Polls this app's own
/api/data endpoint roughly every 1.5s via plain JS fetch() — no websockets.

Bot control plane: this process also owns start/stop/status for
paper_trader.py (v1/v2/v3), gabagool_paper_trader.py (this directory), and
live_trading/live_trader.py (see /api/bot/*), spawning
each as its own subprocess and tracking liveness via a pidfile (pidfile.py)
rather than an in-memory handle — that way a dashboard restart can still see
whether a bot is running instead of losing track of it or spawning a
duplicate. Starting live mode always re-checks preflight_check.py's full
report first (/api/preflight) and refuses if it isn't READY; that gate can't
be skipped from here. Keep --host at its 127.0.0.1 default unless you have a
specific reason to expose these controls beyond localhost.

"paper" (oracle-lag v1, 5m), "oracle_v2" (oracle-lag v2 — same engine,
three additional entry filters, see oracle_lag_strategy_v2.py), "oracle_v3"
(oracle-lag v3 — v2's filters plus Kelly sizing that compounds off live
equity instead of a fixed bankroll, see oracle_lag_strategy_v3.py), and
"gabagool" (YES/NO order-book arbitrage, 15m) are four independent
strategies, all always-visible, all independently start/stop-able, and all
rendered in parallel on one page — not a toggle between them. "paper",
"oracle_v2", and "oracle_v3" run the exact same script (paper_trader.py
--v2 / --v3) against the exact same live market, which is the point: start
them side by side to compare v1 vs v2 vs v3 in real time — see
_oracle_v2_args/_oracle_v3_args, which have to explicitly override every
one of paper_trader.py's --state-file/--trades-log/--pid-file/
--autorestart-marker/--perf-log-dir so "oracle_v2"/"oracle_v3" don't
silently share "paper"'s files (that script's own defaults for all five are
identical regardless of --v2/--v3). /api/data returns all four states in
one payload (see PAPER_STRATEGIES, _state_paths_for) rather than taking a
selector; /api/report.csv and the reset endpoints still take a per-strategy
target since a CSV export or a destructive reset is inherently a "pick one"
action. "live" is its own fifth, differently-gated panel (preflight, typed
confirmation) — see templates/dashboard.html.

/api/paper/reset, /api/oracle_v2/reset, /api/oracle_v3/reset, and
/api/gabagool/reset (POST) permanently delete their own bot's state/trades
files (and perf-log CSVs, for the three that use performance_tracker —
"paper", "oracle_v2", and "oracle_v3"; not "gabagool", see
gabagool_paper_trader.py). Each refuses (409) while its own bot is running,
same reason two instances of either bot aren't allowed to share one state
file (see pidfile.py).
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
from types import SimpleNamespace

from flask import Flask, Response, jsonify, render_template, request

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(PROJECT_ROOT))
import dashboard as dash  # reuse LivePrice / WalletWatcher / load_paper_state — no duplicated logic
import gabagool_paper_trader as gaba_bot  # reuse its DEFAULT_STATE_PATH/DEFAULT_TRADES_LOG_PATH/DEFAULT_PID_PATH

import gabagool_strategy
import oracle_lag_strategy_v2 as strategy_v2
import oracle_lag_strategy_v3 as strategy_v3
import order_executor
import pidfile
import preflight_check as preflight

app = Flask(__name__, root_path=str(SCRIPT_DIR))

live_price = dash.LivePrice()
wallet: dash.WalletWatcher = None  # constructed in main() once --rpc-url is known
state_path = dash.DEFAULT_STATE_PATH
trades_log_path = SCRIPT_DIR / "paper_trades.jsonl"
PERF_LOG_DIR = SCRIPT_DIR / "perf_logs"

# -- bot process control -----------------------------------------------------

LIVE_TRADING_DIR = PROJECT_ROOT / "live_trading"

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

BOT_SCRIPTS = {
    "paper": {
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": SCRIPT_DIR / "paper_trader.pid",
        "log": SCRIPT_DIR / "bot.log",
    },
    "gabagool": {
        "script": SCRIPT_DIR / "gabagool_paper_trader.py",
        "pidfile": gaba_bot.DEFAULT_PID_PATH,
        # Distinct from "paper"'s bot.log even though both scripts live in
        # this same directory — sharing one log file would interleave two
        # unrelated bots' output into one stream.
        "log": SCRIPT_DIR / "gabagool_bot.log",
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
    "live": {
        "script": LIVE_TRADING_DIR / "live_trader.py",
        "pidfile": LIVE_TRADING_DIR / "live_trader.pid",
        "log": LIVE_TRADING_DIR / "bot.log",
    },
}
bot_processes: dict = {}  # mode -> subprocess.Popen, only for processes THIS dashboard instance spawned

# "paper", "oracle_v2", "oracle_v3", and "gabagool" are four independent
# strategies, each always visible and independently start/stop-able (see
# templates/dashboard.html) — distinct from "live", which is its own
# differently-gated panel. This maps which (state_file, trades_log) pair
# backs each one; "paper"'s pair is mutable (set from
# --state-file/--trades-log in main()), the other three are fixed to their
# own scripts' defaults since there's no CLI flag for any of them here.
PAPER_STRATEGIES = ("paper", "oracle_v2", "oracle_v3", "gabagool")


def _state_paths_for(strategy: str) -> tuple:
    if strategy == "oracle_v2":
        return ORACLE_V2_STATE_PATH, ORACLE_V2_TRADES_LOG_PATH
    if strategy == "oracle_v3":
        return ORACLE_V3_STATE_PATH, ORACLE_V3_TRADES_LOG_PATH
    if strategy == "gabagool":
        return gaba_bot.DEFAULT_STATE_PATH, gaba_bot.DEFAULT_TRADES_LOG_PATH
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
    wallet.maybe_refresh()

    bots = {}
    for strategy in PAPER_STRATEGIES:
        sp, _ = _state_paths_for(strategy)
        bots[strategy] = dash.load_paper_state(sp)

    return jsonify(
        {
            "server_time": time.time(),
            "price": {"value": live_price.price, "error": live_price.error},
            "wallet": {
                "configured": wallet.address is not None,
                "address": wallet.address,
                "pol": wallet.pol_balance,
                "usdc": wallet.usdc_balance,
                "error": wallet.error,
            },
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


def _gabagool_args(payload: dict) -> list:
    return [
        "--bankroll", str(_float_param(payload, "bankroll", 1000.0, lo=5.0)),
        "--bucket-usd", str(_float_param(payload, "bucket_usd", 50.0, lo=5.0, hi=100000.0)),
        "--fee-rate", str(_float_param(payload, "fee_rate", gabagool_strategy.FEE_RATE_CRYPTO, lo=0.0, hi=1.0)),
        "--min-profit-margin", str(_float_param(payload, "min_profit_margin", 0.005, lo=0.0, hi=0.5)),
        "--min-seconds-remaining", str(_float_param(payload, "min_seconds_remaining", 30.0, lo=0.0, hi=800.0)),
    ]  # fmt: skip


def _live_args(payload: dict) -> list:
    args = [
        "--bankroll", str(_float_param(payload, "bankroll", 50.0, lo=1.0)),
        "--min-edge", str(_float_param(payload, "min_edge", 0.02, lo=0.0, hi=0.5)),
        "--max-position-pct", str(_float_param(payload, "max_position_pct", 0.05, lo=0.001, hi=1.0)),
        "--kelly-multiplier", str(_float_param(payload, "kelly_multiplier", 0.25, lo=0.01, hi=1.0)),
        "--max-stake-usd", str(_float_param(payload, "max_stake_usd", 2.0, lo=0.5, hi=50.0)),
        "--max-trades-per-hour", str(_int_param(payload, "max_trades_per_hour", 4, lo=1, hi=100)),
        "--max-trades-per-day", str(_int_param(payload, "max_trades_per_day", 20, lo=1, hi=500)),
        "--max-open-positions", str(_int_param(payload, "max_open_positions", 1, lo=1, hi=50)),
    ]  # fmt: skip
    max_daily_loss_usd = payload.get("max_daily_loss_usd")
    if max_daily_loss_usd not in (None, "", "null"):
        args += ["--max-daily-loss-usd", str(_float_param(payload, "max_daily_loss_usd", 0.0, lo=0.5, hi=1000.0))]
    return args


# Every mode's args builder in one place — api_bot_start dispatches through
# this instead of an if/else chain, so adding a strategy means adding one
# entry here rather than another branch.
ARGS_BUILDERS = {
    "paper": _paper_args,
    "oracle_v2": _oracle_v2_args,
    "oracle_v3": _oracle_v3_args,
    "gabagool": _gabagool_args,
    "live": _live_args,
}


def _preflight_args() -> SimpleNamespace:
    return SimpleNamespace(
        host=order_executor.DEFAULT_HOST,
        chain_id=order_executor.DEFAULT_CHAIN_ID,
        rpc_url=preflight.DEFAULT_RPC_URL,
        private_key_env="POLYMARKET_PRIVATE_KEY",
        keystore=None,
        signature_type=0,
        funder=os.environ.get("POLYMARKET_FUNDER") or None,
        min_gas_pol=0.05,
    )


_preflight_cache = {"ts": 0.0, "ready": False, "results": []}
PREFLIGHT_CACHE_TTL = 30.0  # seconds — full preflight re-derives CLOB API creds, don't hammer that on every poll


def _run_preflight_uncached() -> tuple:
    """Read-only preflight check. Guards the "no key configured" case itself
    rather than letting order_executor.load_private_key's sys.exit(1) tear
    down the request thread — that call is fine from a CLI's main(), not
    from inside a Flask handler."""
    pf_args = _preflight_args()
    if not os.environ.get(pf_args.private_key_env) and not pf_args.keystore:
        return False, [
            {
                "section": "WALLET",
                "name": "credentials",
                "status": "FAIL",
                "detail": f"{pf_args.private_key_env} not set",
            }
        ]

    from py_clob_client.client import ClobClient
    from py_clob_client.config import get_contract_config
    from web3 import Web3

    private_key = order_executor.load_private_key(pf_args)
    from eth_account import Account

    address = Account.from_key(private_key).address

    w3 = Web3(Web3.HTTPProvider(pf_args.rpc_url, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        return False, [
            {"section": "CLOB", "name": "rpc", "status": "FAIL", "detail": f"could not connect to {pf_args.rpc_url}"}
        ]

    collateral_address = get_contract_config(pf_args.chain_id).collateral
    client = ClobClient(
        host=pf_args.host,
        chain_id=pf_args.chain_id,
        key=private_key,
        signature_type=pf_args.signature_type,
        funder=pf_args.funder,
    )
    results = preflight.run_all_checks(address, w3, collateral_address, client, pf_args.min_gas_pol)
    ready = not any(r.status == "FAIL" for r in results)
    # Redact the derived API key before this ever leaves the process — this
    # endpoint is meant for localhost only, but there's no reason to put a
    # credential (even a derived one, not the private key) on the wire.
    redacted = []
    for r in results:
        d = r.__dict__.copy()
        if d.get("name") == "Level 1: private-key auth" and d.get("status") == "PASS":
            d["detail"] = "api_key=<redacted>"
        redacted.append(d)
    return ready, redacted


def _run_preflight_cached(force: bool = False) -> tuple:
    now = time.time()
    if not force and (now - _preflight_cache["ts"]) < PREFLIGHT_CACHE_TTL:
        return _preflight_cache["ready"], _preflight_cache["results"]
    ready, results = _run_preflight_uncached()
    _preflight_cache.update(ts=now, ready=ready, results=results)
    return ready, results


@app.route("/api/preflight")
def api_preflight():
    force = request.args.get("force") == "1"
    ready, results = _run_preflight_cached(force=force)
    return jsonify({"ready": ready, "results": results})


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
    if mode == "live":
        ready, results = _run_preflight_cached(force=True)  # always fresh right before spending real money
        if not ready:
            return jsonify({"error": "preflight check failed — live trading refused", "preflight": results}), 412
    extra_args = ARGS_BUILDERS[mode](payload)

    proc_args = [sys.executable, str(info["script"]), *extra_args]
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


@app.route("/api/paper/reset", methods=["POST"])
def api_paper_reset():
    status = _bot_status("paper")
    if status["running"]:
        return jsonify({"error": f"paper bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    deleted = []
    for path in [state_path, trades_log_path, *PERF_LOG_DIR.glob("*.csv")]:
        try:
            path.unlink()
            deleted.append(str(path.relative_to(PROJECT_ROOT)))
        except FileNotFoundError:
            pass
    return jsonify({"ok": True, "deleted": deleted})


@app.route("/api/gabagool/reset", methods=["POST"])
def api_gabagool_reset():
    status = _bot_status("gabagool")
    if status["running"]:
        return jsonify({"error": f"gabagool bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    sp, tl = _state_paths_for("gabagool")
    deleted = []
    for path in [sp, sp.with_suffix(".tmp"), tl]:
        try:
            path.unlink()
            deleted.append(str(path.relative_to(PROJECT_ROOT)))
        except FileNotFoundError:
            pass
    return jsonify({"ok": True, "deleted": deleted})


@app.route("/api/oracle_v2/reset", methods=["POST"])
def api_oracle_v2_reset():
    status = _bot_status("oracle_v2")
    if status["running"]:
        return jsonify({"error": f"oracle_v2 bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    sp, tl = _state_paths_for("oracle_v2")
    deleted = []
    for path in [sp, sp.with_suffix(".tmp"), tl, *ORACLE_V2_PERF_LOG_DIR.glob("*.csv")]:
        try:
            path.unlink()
            deleted.append(str(path.relative_to(PROJECT_ROOT)))
        except FileNotFoundError:
            pass
    return jsonify({"ok": True, "deleted": deleted})


@app.route("/api/oracle_v3/reset", methods=["POST"])
def api_oracle_v3_reset():
    status = _bot_status("oracle_v3")
    if status["running"]:
        return jsonify({"error": f"oracle_v3 bot is running (pid {status['pid']}) — stop it before resetting"}), 409

    sp, tl = _state_paths_for("oracle_v3")
    deleted = []
    for path in [sp, sp.with_suffix(".tmp"), tl, *ORACLE_V3_PERF_LOG_DIR.glob("*.csv")]:
        try:
            path.unlink()
            deleted.append(str(path.relative_to(PROJECT_ROOT)))
        except FileNotFoundError:
            pass
    return jsonify({"ok": True, "deleted": deleted})


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

    title = {"oracle_v2": "Oracle-lag scalper v2", "oracle_v3": "Oracle-lag scalper v3"}.get(
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


def build_gabagool_report_csv() -> str:
    """Gabagool's trades are shaped nothing like the oracle-lag strategies'
    (two-leg arbitrage fills, not a single directional side/entry/edge) — see
    gabagool_strategy.py's module docstring — so this is a separate builder
    rather than another branch of build_report_csv() above."""
    sp, tl = _state_paths_for("gabagool")
    state = dash.load_paper_state(sp) or {}
    stats = state.get("stats", {})
    trades = sorted(load_trade_history(tl), key=lambda t: t.get("opened_at") or 0)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Gabagool (BTC 15m arbitrage) — paper trading report"])
    w.writerow(["Generated", _iso(time.time())])
    w.writerow([])
    w.writerow(["Starting bankroll", f"${state.get('starting_bankroll', 0):.2f}"])
    w.writerow(["Current bankroll", f"${state.get('bankroll', 0):.2f}"])
    w.writerow(["Current equity", f"${state.get('equity', 0):.2f}"])
    w.writerow(["Opportunities checked", stats.get("opportunities_checked", 0)])
    w.writerow(["Trades opened", stats.get("trades_opened", 0)])
    w.writerow(["Trades closed", stats.get("trades_closed", 0)])
    w.writerow(["Total locked profit", f"${stats.get('total_locked_profit', 0):+.3f}"])
    w.writerow(["Total fees paid", f"${stats.get('total_fees_paid', 0):.3f}"])
    w.writerow([])
    w.writerow(
        ["Market", "Shares", "Avg Up", "Avg Down", "Total Cost", "Fees", "Net Profit", "Margin/Share", "Opened At", "Closed At", "Resolution", "Resolution Source"]
    )  # fmt: skip
    for t in trades:
        w.writerow(
            [
                t.get("market_slug", ""),
                f"{t.get('shares', 0):.2f}",
                f"{t.get('avg_price_up', 0):.4f}",
                f"{t.get('avg_price_down', 0):.4f}",
                f"{t.get('total_cost', 0):.2f}",
                f"{t.get('fee_up', 0) + t.get('fee_down', 0):.3f}",
                f"{t.get('net_profit', 0):+.3f}",
                f"{t.get('net_margin_per_share', 0):+.4f}",
                _iso(t.get("opened_at")),
                _iso(t.get("closed_at")),
                t.get("resolution", ""),
                t.get("resolution_source", ""),
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

    csv_text = build_gabagool_report_csv() if strategy == "gabagool" else build_report_csv(strategy)
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
    global wallet, state_path, trades_log_path

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1, localhost only)")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--state-file", default=str(dash.DEFAULT_STATE_PATH))
    parser.add_argument("--trades-log", default=str(SCRIPT_DIR / "paper_trades.jsonl"))
    parser.add_argument("--rpc-url", default=dash.DEFAULT_RPC_URL)
    parser.add_argument("--debug", action="store_true", help="Flask debug mode (auto-reload, verbose errors)")
    args = parser.parse_args()

    state_path = Path(args.state_file)
    trades_log_path = Path(args.trades_log)
    wallet = dash.WalletWatcher(args.rpc_url)

    print(f"Dashboard running at http://{args.host}:{args.port}", file=sys.stderr)
    print(f"Reading state from {state_path}", file=sys.stderr)
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Web dashboard for monitoring the oracle-lag paper trader — a separate
process from paper_trader.py by design.

Reads paper_trader.py's state file (--state-file) and trades log
(--trades-log) from disk; never imports or talks to the bot process
directly. That's deliberate isolation: restart this dashboard as often as
you like — to change ports, pull in a template edit, whatever — without
ever interrupting the running bot, and vice versa. If this process crashes
or hangs, the bot keeps trading uninterrupted.

Mostly read-only: never signs anything or places orders, and only reads
paper_state.json/paper_trades.jsonl and polls public price/chain data, same
as dashboard.py (the terminal version) — with one deliberate exception,
/api/paper/reset (see below), which deletes that state.

Usage:
    python paper_trading/web_dashboard.py
    python paper_trading/web_dashboard.py --port 8080
    python paper_trading/web_dashboard.py --state-file /path/to/paper_state.json --trades-log /path/to/paper_trades.jsonl
    python paper_trading/web_dashboard.py --host 0.0.0.0   # expose beyond localhost — only on a trusted network

Then open http://127.0.0.1:5000 in a browser. Polls this app's own
/api/data endpoint roughly every 1.5s via plain JS fetch() — no websockets.

Bot control plane: this process also owns start/stop/status for both
paper_trader.py and live_trading/live_trader.py (see /api/bot/*), spawning
each as its own subprocess and tracking liveness via a pidfile (pidfile.py)
rather than an in-memory handle — that way a dashboard restart can still
see whether a bot is running instead of losing track of it or spawning a
duplicate. Starting live mode always re-checks preflight_check.py's full
report first (/api/preflight) and refuses if it isn't READY; that gate
can't be skipped from here. Keep --host at its 127.0.0.1 default unless you
have a specific reason to expose these controls beyond localhost.

/api/paper/reset (POST) permanently deletes paper_state.json,
paper_trades.jsonl, and perf_logs/{signals,trades,executions}.csv — the
paper bot's entire bankroll/position/history. It refuses (409) while the
paper bot is running, same as it would corrupt state for the same reason
two paper_trader.py instances aren't allowed to share one state file (see
pidfile.py). This only ever touches paper-mode files; live_trading/ state
is never in scope here.
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
BOT_SCRIPTS = {
    "paper": {
        "script": SCRIPT_DIR / "paper_trader.py",
        "pidfile": SCRIPT_DIR / "paper_trader.pid",
        "log": SCRIPT_DIR / "bot.log",
    },
    "live": {
        "script": LIVE_TRADING_DIR / "live_trader.py",
        "pidfile": LIVE_TRADING_DIR / "live_trader.pid",
        "log": LIVE_TRADING_DIR / "bot.log",
    },
}
bot_processes: dict = {}  # mode -> subprocess.Popen, only for processes THIS dashboard instance spawned


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/data")
def api_data():
    live_price.maybe_refresh()
    wallet.maybe_refresh()
    state = dash.load_paper_state(state_path)

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
            "state": state,
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
        extra_args = _live_args(payload)
    else:
        extra_args = _paper_args(payload)

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


def build_report_csv() -> str:
    history = load_trade_history(trades_log_path)
    state = dash.load_paper_state(state_path) or {}
    stats = state.get("stats", {})

    buf = io.StringIO()
    w = csv.writer(buf)

    w.writerow(["Oracle-lag scalper — paper trading report"])
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
    if not state_path.exists():
        return Response("No paper trading data yet.", mimetype="text/plain", status=404)

    csv_text = build_report_csv()
    filename = f"paper_trading_report_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.csv"
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

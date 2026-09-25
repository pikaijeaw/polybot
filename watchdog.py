#!/usr/bin/env python3
"""
Watchdog for the paper bot: polls whether paper_trader.py, if it's supposed
to be running, is actually alive, and if it has gone down unexpectedly, restarts it with its last-known
args and sends a Telegram alert. Reads TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
from .env via notify.send_telegram_message — no separate
config needed if those are already set.

Telling "crashed" apart from "the user meant to stop this" is the whole
point here: resurrecting a bot the user deliberately stopped would be a
regression on the Stop button. Each bot writes a small marker file on
startup (via pidfile.write_autorestart_marker) and clears it ONLY on its
own graceful, signal-triggered shutdown (SIGINT/SIGTERM -> Ctrl+C or the
dashboard's Stop button) — never on a crash. So:

    marker present + pidfile dead  -> crashed, restart it, send an alert
    marker absent                  -> intentionally stopped, leave it down

This is its own independent process (with its own pidfile), consistent
with paper_trader.py / web_dashboard.py being independent — a watchdog crash shouldn't take a trading bot down, and a
bot's ability to be resurrected shouldn't depend on the dashboard being up.

Restart-loop protection: if a bot keeps crashing immediately after each
restart (broken environment, whatever), --max-restarts-
per-hour caps how many times this watchdog will retry per bot before it
gives up and sends a single "needs manual attention" alert instead of
restart-spamming Telegram.

Usage:
    python watchdog.py                            # watch paper, 30s interval
    python watchdog.py --check-interval 15 --max-restarts-per-hour 3
"""

import argparse
import subprocess
import sys
import time
from collections import deque
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

import notify  # noqa: E402
import pidfile  # noqa: E402

DEFAULT_PID_PATH = PROJECT_ROOT / "watchdog.pid"

BOTS = {
    "paper": {
        "script": PROJECT_ROOT / "paper_trading" / "paper_trader.py",
        "pidfile": PROJECT_ROOT / "paper_trading" / "paper_trader.pid",
        "marker": PROJECT_ROOT / "paper_trading" / "paper_trader.autorestart.json",
        "log": PROJECT_ROOT / "paper_trading" / "bot.log",
    },
}


class RestartTracker:
    """Sliding-window restart-count limiter — caps how many times we'll auto-restart a given bot within
    a rolling window before giving up on it."""

    def __init__(self, max_restarts: int, window_seconds: float = 3600.0):
        self.max_restarts = max_restarts
        self.window_seconds = window_seconds
        self._timestamps: deque = deque()

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def allowed(self, now: float | None = None) -> bool:
        now = now if now is not None else time.time()
        self._evict(now)
        return len(self._timestamps) < self.max_restarts

    def record(self, ts: float | None = None) -> None:
        self._timestamps.append(ts if ts is not None else time.time())


def restart_bot(mode: str, info: dict, argv: list, spawned: dict) -> int:
    proc_args = [sys.executable, "-u", str(info["script"]), *argv]  # -u: bot.log written live, not block-buffered
    info["log"].parent.mkdir(parents=True, exist_ok=True)
    with open(info["log"], "a") as log_f:
        proc = subprocess.Popen(proc_args, stdout=log_f, stderr=subprocess.STDOUT, cwd=str(PROJECT_ROOT))
    spawned[mode] = proc  # keep the handle so a later crash of THIS child can be reaped, not left a zombie
    return proc.pid


def check_and_restart(mode: str, info: dict, tracker: RestartTracker, gave_up: set, spawned: dict) -> None:
    proc = spawned.get(mode)
    if proc is not None:
        proc.poll()  # reap a previous restart's child if it has since exited — avoids it lingering as a zombie

    marker = pidfile.read_autorestart_marker(info["marker"])
    if marker is None:
        gave_up.discard(mode)  # not marked as "should be running" — nothing to watch or reset
        return

    pid = pidfile.read_pid(info["pidfile"])
    if pid is not None and pidfile.pid_is_alive(pid):
        gave_up.discard(mode)  # healthy — clears any prior "gave up" state so future crashes alert again
        return

    # marker present but pidfile shows no live process -> went down unexpectedly
    if not tracker.allowed():
        if mode not in gave_up:
            notify.send_telegram_message(
                f"⚠️ PolyBot watchdog: {mode} bot keeps crashing "
                f"(restart limit reached, last known pid {pid}). NOT restarting again — needs manual attention."
            )
            gave_up.add(mode)
        return

    tracker.record()
    argv = marker.get("argv") or []
    new_pid = restart_bot(mode, info, argv, spawned)
    print(
        f"[{time.strftime('%H:%M:%S')}] {mode} bot was down (pid {pid} not alive) — restarted as pid {new_pid}",
        file=sys.stderr,
    )
    notify.send_telegram_message(
        f"\U0001f504 PolyBot watchdog: {mode} bot was down (pid {pid} not alive) — restarted automatically (new pid {new_pid})."
    )


def watch_loop(modes: list, check_interval: float, max_restarts_per_hour: int) -> None:
    trackers = {mode: RestartTracker(max_restarts_per_hour) for mode in modes}
    gave_up: set = set()
    spawned: dict = {}  # mode -> Popen, only for restarts THIS watchdog process performed
    print(f"watchdog started, watching: {', '.join(modes)} (check every {check_interval}s)", file=sys.stderr)
    while True:
        for mode in modes:
            try:
                check_and_restart(mode, BOTS[mode], trackers[mode], gave_up, spawned)
            except Exception as e:
                print(f"watchdog check error for {mode}: {e}", file=sys.stderr)
        time.sleep(check_interval)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--modes",
        default="paper",
        help="Comma-separated list of bots to watch (default: paper)",
    )
    parser.add_argument(
        "--check-interval", type=float, default=30.0, help="Seconds between liveness checks (default: 30)"
    )
    parser.add_argument(
        "--max-restarts-per-hour",
        type=int,
        default=5,
        help="Give up restarting a bot (and send one alert) after this many restarts in a rolling hour (default: 5)",
    )
    parser.add_argument("--pid-file", default=str(DEFAULT_PID_PATH))
    args = parser.parse_args()

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    unknown = [m for m in modes if m not in BOTS]
    if unknown:
        parser.error(f"unknown mode(s) {unknown}; valid: {list(BOTS)}")

    pidfile.claim_or_exit(Path(args.pid_file))

    try:
        watch_loop(modes, args.check_interval, args.max_restarts_per_hour)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()

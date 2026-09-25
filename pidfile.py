#!/usr/bin/env python3
"""
Tiny PID-file helper for single-instance process protection.

paper_trader.py claims a pidfile on startup and clean
it up on normal exit. That lets web_dashboard.py answer "is this bot already
running?" by checking whether the PID inside is still alive, without ever
holding a live subprocess handle itself — a handle breaks the moment the
dashboard restarts (new pid, different memory), but a pidfile on disk
survives that and lets a restarted dashboard re-attach instead of
accidentally spawning a second instance on top of a live one.
"""

import atexit
import json
import os
import sys
import time
from pathlib import Path


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # process exists, just owned by someone else

    # os.kill succeeding only means the pid still exists — a zombie (exited,
    # waiting on its parent to reap it) passes that check too, even though
    # it's already dead in every sense callers here care about. A bot killed
    # with SIGKILL (or crashed) while its parent hasn't reaped it yet would
    # otherwise be misreported as "running" by web_dashboard.py and, worse,
    # never get restarted by watchdog.py. /proc is Linux-only; anywhere else
    # this silently falls back to the os.kill-only result above.
    try:
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
        # the comm field (2nd column) is wrapped in parens and can itself
        # contain spaces, so split from the right of its closing paren
        # rather than naively splitting on whitespace from the start
        state = stat.rsplit(")", 1)[1].split()[0]
        if state == "Z":
            return False
    except (OSError, IndexError):
        pass
    return True


def read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def claim_or_exit(path: Path) -> None:
    """Exits this process if another instance already holds `path` and is
    still alive; otherwise writes our own pid and registers cleanup so the
    file is removed on normal exit."""
    existing = read_pid(path)
    if existing is not None and pid_is_alive(existing):
        print(f"another instance is already running (pid {existing}, pidfile {path}); exiting", file=sys.stderr)
        sys.exit(1)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(os.getpid()))

    def _cleanup():
        try:
            if read_pid(path) == os.getpid():
                path.unlink(missing_ok=True)
        except OSError:
            pass

    atexit.register(_cleanup)


# ---------------------------------------------------------------------------
# Autorestart marker — lets watchdog.py tell "crashed" apart from "the user
# meant to stop this"
# ---------------------------------------------------------------------------
#
# A bot writes this marker right after claiming its pidfile, and clears it
# ONLY on its graceful, signal-triggered shutdown path (SIGINT/SIGTERM ->
# asyncio.CancelledError) — never from a generic atexit/finally handler,
# because those also run after an uncaught exception, which would erase the
# very evidence watchdog.py needs to see. So: marker present + pidfile dead
# = crashed, restart it. Marker absent = someone deliberately stopped it,
# leave it down.


def write_autorestart_marker(path: Path, argv: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pid": os.getpid(), "argv": argv, "started_at": time.time()}))


def clear_autorestart_marker(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def read_autorestart_marker(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError, ValueError):
        return None
